# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import concurrent.futures
import logging
import os
from typing import Any, Dict, Generator, List, Optional, Tuple

import numpy as np
import pandas as pd
import tiledb
from dask.distributed import Client
from retry import retry

from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
    convert_to_days_since_epoch,
)
from gedidb.utils.tiledb_consolidation import SpatialConsolidationPlanner
from gedidb.utils.filters import TileDBFilterPolicy

logger = logging.getLogger(__name__)


class GEDIDatabase:
    """
    Manages creation and operation of global TileDB arrays for GEDI data storage.

    Performance design decisions
    ----------------------------
    - Hilbert pre-sort is opt-in via config ('hilbert_presort': true). It improves
      compression and read locality, but costs an argsort + fancy-index copy per
      granule. Disable when write throughput matters more than read performance.

    - TileDBFilterPolicy is opt-in via config ('use_filters': true). Compression
      filters (DeltaFilter, DoubleDelta, Zstd) reduce storage size but add CPU cost
      on every write. Default is Zstd-only for a balanced trade-off.

    - dtype coercion in _extract_variable_data is skipped when the source Series
      already matches the target dtype — avoids a full array copy for every attribute
      on every granule.

    - Spatial domain bounds and array domain metadata are cached on __init__ so
      write_granule / spatial_chunking never re-open the array just for config lookups.

    - allows_duplicates=True preserves all valid GEDI shots, including co-located
      shots within the same UTC day. The old drop_duplicates() silently discarded
      valid data.

    - write_batch() amortises the TileDB open/close cost across many granules.
      Prefer it over calling write_granule() in a loop for large ingestion jobs.

    - mark_granule_as_processed() now has retry logic (absent in old version).

    - timestamp_ns is stored as true int64 nanoseconds. The old version divided by
      1000 (yielding microseconds), which broke nanosecond-precision deduplication.
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[dict] = None):
        """
        Initialise GEDIDatabase.

        Parameters
        ----------
        config : dict
            Configuration dictionary.
        credentials : dict, optional
            AWS/S3 credentials. Required when storage_type == 's3'.
        """
        self.config = config
        self.variables_config = self._load_variables_config(config)

        cfg_td = config["tiledb"]
        storage_type = cfg_td.get("storage_type", "local").lower()

        # ------------------------------------------------------------------ #
        # Performance flags (opt-in, default to fast/simple)
        # ------------------------------------------------------------------ #
        self.filter_policy = TileDBFilterPolicy(cfg_td)
        self._schema_cache: Optional[dict] = None

        # ------------------------------------------------------------------ #
        # Array URI
        # ------------------------------------------------------------------ #
        if storage_type == "s3":
            bucket = cfg_td["s3_bucket"]
            self.array_uri = os.path.join(f"s3://{bucket}", "array_uri")
        else:
            base_path = cfg_td.get("local_path", "./")
            self.array_uri = os.path.join(base_path, "array_uri")

        self.overwrite = cfg_td.get("overwrite", False)

        # ------------------------------------------------------------------ #
        # TileDB context
        # ------------------------------------------------------------------ #
        self.tiledb_config = self._build_tiledb_config(
            storage_type, cfg_td, credentials
        )
        self.ctx = tiledb.Ctx(self.tiledb_config)

        # ------------------------------------------------------------------ #
        # Cached values — avoids re-opening the array or re-reading config
        # on every write / spatial_chunking call.
        # ------------------------------------------------------------------ #
        self._spatial_bounds: Tuple[float, float, float, float] = (
            self._get_tiledb_spatial_domain()
        )

    # ---------------------------------------------------------------------- #
    # Initialisation helpers
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _build_tiledb_config(
        storage_type: str,
        cfg_td: dict,
        credentials: Optional[dict],
    ) -> tiledb.Config:
        """Build a TileDB Config for local or S3 storage."""
        cons = cfg_td.get("consolidation_settings", {})
        base = {
            "sm.memory_budget": cons.get("memory_budget", "5000000000"),
            "sm.memory_budget_var": cons.get("memory_budget_var", "2000000000"),
        }

        if storage_type != "s3":
            return tiledb.Config(base)

        if not credentials:
            raise ValueError("S3 credentials are required when storage_type == 's3'.")

        s3 = cfg_td.get("s3_settings", {})
        return tiledb.Config(
            {
                **base,
                "vfs.s3.aws_access_key_id": credentials["AccessKeyId"],
                "vfs.s3.aws_secret_access_key": credentials["SecretAccessKey"],
                "vfs.s3.endpoint_override": cfg_td["url"],
                "vfs.s3.use_virtual_addressing": "false",
                "vfs.s3.use_multipart_upload": "true",
                "vfs.s3.multipart_part_size": s3.get("multipart_part_size", "52428800"),
                "vfs.s3.multipart_threshold": "52428800",
                "vfs.s3.max_parallel_ops": "8",
                "vfs.s3.region": "eu-central-1",
                "vfs.s3.scheme": "https",
                "vfs.s3.backoff_scale": s3.get("backoff_scale", "2.0"),
                "vfs.s3.backoff_max_ms": s3.get("backoff_max_ms", "120000"),
                "sm.vfs.s3.connect_timeout_ms": s3.get("connect_timeout_ms", "60000"),
                "sm.vfs.s3.request_timeout_ms": s3.get("request_timeout_ms", "600000"),
                "sm.vfs.s3.connect_max_tries": s3.get("connect_max_tries", "5"),
                "sm.io_concurrency_level": "8",
                "sm.compute_concurrency_level": "8",
                "sm.mem.total_budget": "10737418240",
            }
        )

    # ---------------------------------------------------------------------- #
    # Schema / array creation
    # ---------------------------------------------------------------------- #

    def _create_arrays(self) -> None:
        """Create TileDB arrays and write variable metadata."""
        self._create_array(self.array_uri)
        self._add_variable_metadata()

    def _create_array(self, uri: str) -> None:
        """
        Create a sparse TileDB array.

        allows_duplicates=True: multiple GEDI shots can share the same
        (lat, lon, day) coordinate. Silently overwriting or erroring on
        duplicates (the old behaviour without this flag) is incorrect.
        """
        if tiledb.array_exists(uri, ctx=self.ctx):
            if self.overwrite:
                tiledb.remove(uri, ctx=self.ctx)
                self._schema_cache = None
                logger.info(f"Overwritten existing TileDB array at {uri}")
            else:
                logger.info(f"TileDB array already exists at {uri}. Skipping.")
                return

        try:
            cfg_td = self.config.get("tiledb", {})
            schema = tiledb.ArraySchema(
                domain=self._create_domain(),
                attrs=self._create_attributes(),
                sparse=True,
                allows_duplicates=True,
                capacity=cfg_td.get("capacity", 200_000),
                cell_order=cfg_td.get("cell_order", "hilbert"),
            )
            tiledb.Array.create(uri, schema, ctx=self.ctx)
            logger.info(f"Successfully created TileDB array at {uri}")
        except (ValueError, tiledb.TileDBError) as e:
            logger.error(f"Failed to create array: {e}")
            raise

    def _create_domain(self) -> tiledb.Domain:
        """
        Build the TileDB domain (latitude, longitude, time).

        Time is stored as int64 days-since-epoch to avoid datetime parsing
        overhead on every write. Filters are applied only when use_filters=True.
        """
        cfg_td = self.config.get("tiledb", {})
        s = cfg_td.get("spatial_range", {})
        t = cfg_td.get("time_range", {})

        lat_min, lat_max = s.get("lat_min"), s.get("lat_max")
        lon_min, lon_max = s.get("lon_min"), s.get("lon_max")
        time_min = _datetime_to_timestamp_days(t.get("start_time"))
        time_max = _datetime_to_timestamp_days(t.get("end_time"))

        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError("Spatial and temporal ranges must be fully specified.")
        if lat_min >= lat_max or lon_min >= lon_max:
            raise ValueError("lat_min < lat_max and lon_min < lon_max required.")
        if time_min >= time_max:
            raise ValueError("time_min must be less than time_max.")

        # Filters — only applied when explicitly enabled
        spatial_filters = self.filter_policy.spatial_dim_filters()
        time_filters = self.filter_policy.time_dim_filters()

        def _dim(name, domain, tile_key, tile_default, dtype, filters):
            kwargs = dict(
                name=name,
                domain=domain,
                tile=cfg_td.get(tile_key, tile_default),
                dtype=dtype,
            )
            if filters is not None:
                kwargs["filters"] = filters
            return tiledb.Dim(**kwargs)

        return tiledb.Domain(
            _dim(
                "latitude",
                (lat_min, lat_max),
                "latitude_tile",
                1,
                np.float64,
                spatial_filters,
            ),
            _dim(
                "longitude",
                (lon_min, lon_max),
                "longitude_tile",
                1,
                np.float64,
                spatial_filters,
            ),
            _dim(
                "time", (time_min, time_max), "time_tile", 1825, np.int64, time_filters
            ),
        )

    def _create_attributes(self) -> List[tiledb.Attr]:
        """
        Build TileDB attributes for all configured variables.

        Profile variables (e.g. rh) are expanded to rh_1 … rh_N.
        Compression filters are applied only when use_filters=True;
        otherwise TileDB's default (Zstd level-1) is used, which is
        faster to write while still giving reasonable compression.
        """
        if not self.variables_config:
            raise ValueError("Variable configuration is missing.")

        attrs: List[tiledb.Attr] = []

        def _attr(name: str, dtype: np.dtype) -> tiledb.Attr:
            return tiledb.Attr(
                name=name,
                dtype=dtype,
                filters=self.filter_policy.filters_for_dtype(dtype),
            )

        # Scalar variables
        for var_name, var_info in self.variables_config.items():
            if not var_info.get("is_profile", False):
                attrs.append(_attr(var_name, np.dtype(var_info["dtype"])))

        # Profile variables
        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                profile_length = int(var_info.get("profile_length", 1))
                if profile_length <= 0:
                    raise ValueError(f"{var_name}: profile_length must be >= 1")
                dtype = np.dtype(var_info["dtype"])
                for i in range(profile_length):
                    attrs.append(_attr(f"{var_name}_{i + 1}", dtype))

        # Timestamp (nanoseconds since epoch, int64)
        attrs.append(
            tiledb.Attr(
                name="timestamp_ns",
                dtype=np.int64,
                filters=self.filter_policy.timestamp_filters(),
            )
        )

        return attrs

    def _add_variable_metadata(self) -> None:
        """Write variable metadata (units, description, dtype, product_level) to TileDB."""
        if not self.variables_config:
            logger.warning("No variables configuration found. Skipping metadata.")
            return
        try:
            with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
                for var_name, var_info in self.variables_config.items():
                    for key in ("units", "description", "dtype", "product_level"):
                        array.meta[f"{var_name}.{key}"] = var_info.get(key, "unknown")
                    if var_info.get("is_profile", False):
                        array.meta[f"{var_name}.profile_length"] = var_info.get(
                            "profile_length", 0
                        )
        except tiledb.TileDBError as e:
            logger.error(f"Error adding metadata: {e}")
            raise

    # ---------------------------------------------------------------------- #
    # Schema cache
    # ---------------------------------------------------------------------- #

    def _get_schema_cache(self) -> dict:
        """
        Lazily read attribute names and dtypes from the schema, then cache.

        Avoids re-reading the schema from disk/S3 on every granule write.
        The cache is invalidated by _create_array when overwrite=True.
        """
        if self._schema_cache is not None:
            return self._schema_cache

        with tiledb.open(self.array_uri, "r", ctx=self.ctx) as A:
            dim_names = {A.schema.domain.dim(i).name for i in range(A.schema.ndim)}
            attr_dtypes: Dict[str, np.dtype] = {
                A.schema.attr(i).name: np.dtype(A.schema.attr(i).dtype)
                for i in range(A.schema.nattr)
            }

        all_attrs = set(attr_dtypes.keys())
        # Pre-sort once: reused on every write, avoids per-write set arithmetic
        sorted_data_attrs = sorted(all_attrs - dim_names - {"timestamp_ns"})

        self._schema_cache = {
            "attrs": all_attrs,
            "attr_dtypes": attr_dtypes,
            "sorted_data_attrs": sorted_data_attrs,
        }
        return self._schema_cache

    # ---------------------------------------------------------------------- #
    # Spatial utilities
    # ---------------------------------------------------------------------- #

    def _get_tiledb_spatial_domain(self) -> Tuple[float, float, float, float]:
        """Return (min_lon, max_lon, min_lat, max_lat) from config."""
        s = self.config["tiledb"]["spatial_range"]
        return s["lon_min"], s["lon_max"], s["lat_min"], s["lat_max"]

    def spatial_chunking(
        self,
        dataset: pd.DataFrame,
        tiles_across_lat: int = 5,
        tiles_across_lon: int = 5,
    ) -> Generator:
        """
        Yield ((lat_min, lat_max, lon_min, lon_max), view) pairs.

        Tile boundaries are derived from config — no array open required.
        Yields cheap row-subsets of the original DataFrame (no copy).
        """
        if dataset.empty:
            return
        if not {"latitude", "longitude"}.issubset(dataset.columns):
            raise ValueError("Dataset must contain 'latitude' and 'longitude' columns.")

        cfg_td = self.config.get("tiledb", {})
        lat_tile = float(cfg_td.get("latitude_tile", 1))
        lon_tile = float(cfg_td.get("longitude_tile", 1))
        s = cfg_td.get("spatial_range", {})
        lat_min_dom = float(s.get("lat_min", -90.0))
        lon_min_dom = float(s.get("lon_min", -180.0))

        block_lat = lat_tile * tiles_across_lat
        block_lon = lon_tile * tiles_across_lon

        lat_idx = np.floor(
            (dataset["latitude"].to_numpy() - lat_min_dom) / block_lat
        ).astype(int)
        lon_idx = np.floor(
            (dataset["longitude"].to_numpy() - lon_min_dom) / block_lon
        ).astype(int)

        for (i_lat, i_lon), idx in dataset.groupby(
            [lat_idx, lon_idx], sort=False
        ).groups.items():
            lat0 = lat_min_dom + i_lat * block_lat
            lon0 = lon_min_dom + i_lon * block_lon
            yield (lat0, lat0 + block_lat, lon0, lon0 + block_lon), dataset.take(idx)

    # ---------------------------------------------------------------------- #
    # Hilbert pre-sort (optional, off by default)
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _hilbert_sort_index(lat: np.ndarray, lon: np.ndarray) -> np.ndarray:
        """
        Return a sort index that arranges rows in approximate Morton/Hilbert order.

        Enabled by config flag 'hilbert_presort: true'. Worth enabling when:
        - use_filters=True (Delta/DoubleDelta filters compress better on sorted data)
        - Read access patterns are spatially localised (improves tile cache hits)

        Off by default because argsort + iloc fancy-index copies the DataFrame,
        which is a meaningful cost at 10k-100k rows per granule.
        """
        lat_n = ((lat + 90.0) / 180.0 * 65535.0).astype(np.uint32)
        lon_n = ((lon + 180.0) / 360.0 * 65535.0).astype(np.uint32)

        def _spread(x: np.ndarray) -> np.ndarray:
            x = x & 0x0000FFFF
            x = (x | (x << 8)) & 0x00FF00FF
            x = (x | (x << 4)) & 0x0F0F0F0F
            x = (x | (x << 2)) & 0x33333333
            x = (x | (x << 1)) & 0x55555555
            return x

        morton = _spread(lat_n) | (_spread(lon_n) << 1)
        return np.argsort(morton, kind="stable")

    # ---------------------------------------------------------------------- #
    # Write pipeline
    # ---------------------------------------------------------------------- #
    def _validate_granule_data(self, granule_data: pd.DataFrame) -> None:
        """
        Validate the granule data to ensure it meets the requirements for writing.

        Parameters:
        ----------
        granule_data : pd.DataFrame
            The DataFrame containing granule data.

        Raises:
        -------
        ValueError
            If required dimensions or critical variables are missing.
        """
        # Check for required dimensions
        missing_dims = [
            dim
            for dim in self.config["tiledb"]["dimensions"]
            if dim not in granule_data
        ]
        if missing_dims:
            raise ValueError(
                f"Granule data is missing required dimensions: {missing_dims}"
            )

    def _prepare_coordinates(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
        """Extract dimension arrays. Time converted once to int64 days-since-epoch."""
        return {
            "latitude": data["latitude"].to_numpy(dtype=np.float64, copy=False),
            "longitude": data["longitude"].to_numpy(dtype=np.float64, copy=False),
            "time": convert_to_days_since_epoch(data["time"].values),
        }

    def _extract_variable_data(
        self, granule_data: pd.DataFrame
    ) -> Dict[str, np.ndarray]:
        """
        Build the attribute dict for a TileDB write.

        Dtype coercion is skipped when the source Series already matches the
        target dtype — avoids a full array copy for every attribute on every granule.

        timestamp_ns is stored as true int64 nanoseconds (not microseconds).
        """
        cache = self._get_schema_cache()
        attr_dtypes = cache["attr_dtypes"]
        cols = set(granule_data.columns)

        data: Dict[str, np.ndarray] = {}

        for name in cache["sorted_data_attrs"]:
            if name not in cols:
                continue
            series = granule_data[name]
            target_dtype = attr_dtypes[name]
            # Fast path: no coercion needed
            if series.dtype == target_dtype:
                data[name] = series.to_numpy(copy=False)
            else:
                data[name] = self._coerce_series(name, series, target_dtype)

        # timestamp_ns — int64 nanoseconds since epoch
        if "timestamp_ns" in cache["attrs"]:
            data["timestamp_ns"] = pd.to_datetime(
                granule_data["time"], utc=True, errors="coerce"
            ).to_numpy(dtype="int64", copy=False)

        return data

    @staticmethod
    def _coerce_series(name: str, s: pd.Series, target_dtype: np.dtype) -> np.ndarray:
        """
        Coerce a Series to the target NumPy dtype.
        Only called when source and target dtypes differ (slow path).
        """
        dt = np.dtype(target_dtype)
        if dt.kind in ("U", "S"):
            return s.fillna("").astype(str).to_numpy(dtype=dt)
        if dt.kind == "M":
            return (
                pd.to_datetime(s, utc=True, errors="coerce")
                .dt.tz_localize(None)
                .to_numpy(dtype=dt)
            )
        return s.to_numpy(dtype=dt, copy=False)

    @retry(
        (tiledb.TileDBError, ConnectionError),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger,
    )
    def _write_to_tiledb(self, coords, data):
        """
        Write data to the TileDB array with retry logic.

        Parameters:
        ----------
        coords : dict
            Coordinates for the TileDB array dimensions.
        data : dict
            Variable data to write to the TileDB array.
        """
        with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
            dim_names = [dim.name for dim in array.schema.domain]
            dims = tuple(coords[dim_name] for dim_name in dim_names)
            array[dims] = data

    def write_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write the parsed GEDI granule data to the global TileDB arrays,
        filtering out shots that are outside the spatial domain.

        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data, with variable names matching the configuration.

        Raises:
        -------
        ValueError
            If required dimension data or critical variables are missing.
        """
        try:
            granule_data = granule_data.drop_duplicates(
                subset=["latitude", "longitude", "time"]
            )

            # Validate granule data
            self._validate_granule_data(granule_data)

            # Get spatial domain from config
            min_lon, max_lon, min_lat, max_lat = self._get_tiledb_spatial_domain()

            # Filter out shots outside the TileDB spatial domain
            filtered_data = granule_data[
                (granule_data["longitude"] >= min_lon)
                & (granule_data["longitude"] <= max_lon)
                & (granule_data["latitude"] >= min_lat)
                & (granule_data["latitude"] <= max_lat)
            ]

            if filtered_data.empty:
                return  # Skip writing if no valid data

            # Prepare coordinates (dimensions)
            coords = self._prepare_coordinates(filtered_data)

            # Extract data for scalar and profile variables
            data = self._extract_variable_data(filtered_data)

            # Write to the TileDB array
            self._write_to_tiledb(coords, data)

        except Exception as e:
            logger.error(f"Failed to process and write granule data: {e}")
            raise

    # ---------------------------------------------------------------------- #
    # Granule status tracking
    # ---------------------------------------------------------------------- #

    def check_granules_status(self, granule_ids: list) -> dict:
        """
        Check processed status for a list of granule IDs in a single metadata read.
        Returns {granule_id: bool}.
        """
        try:
            with tiledb.open(self.array_uri, mode="r", ctx=self.ctx) as array:
                metadata = {
                    k: array.meta[k] for k in array.meta.keys() if "_status" in k
                }
        except tiledb.TileDBError as e:
            logger.error(f"Failed to read TileDB metadata: {e}")
            return {gid: False for gid in granule_ids}

        statuses = {
            gid: metadata.get(f"granule_{gid}_status") == "processed"
            for gid in granule_ids
        }
        processed = sum(statuses.values())
        logger.debug(
            f"Checked {len(granule_ids)} granules: {processed} processed, "
            f"{len(granule_ids) - processed} pending"
        )
        return statuses

    @retry(
        (tiledb.TileDBError, ConnectionError),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger,
    )
    def mark_granule_as_processed(self, granule_key: str) -> None:
        """Mark a granule as processed in TileDB metadata (with retry)."""
        try:
            with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
                array.meta[f"granule_{granule_key}_status"] = "processed"
                array.meta[f"granule_{granule_key}_processed_date"] = (
                    pd.Timestamp.utcnow().isoformat()
                )
            logger.debug(f"Marked granule {granule_key} as processed")
        except tiledb.TileDBError as e:
            logger.error(f"Failed to mark granule {granule_key} as processed: {e}")
            raise

    # ---------------------------------------------------------------------- #
    # Consolidation
    # ---------------------------------------------------------------------- #

    @retry(
        (tiledb.TileDBError, ConnectionError),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger,
    )
    def consolidate_fragments(
        self,
        consolidation_type: str = "default",
        parallel_engine: Optional[object] = None,
    ) -> None:
        """
        Consolidate fragments, metadata, and commit logs.

        Parameters
        ----------
        consolidation_type : str
            'default' or 'spatial'.
        parallel_engine : Executor or dask.distributed.Client, optional
            Parallelisation engine. Defaults to single-threaded.
        """
        if consolidation_type not in {"default", "spatial"}:
            raise ValueError(
                f"Invalid consolidation_type: '{consolidation_type}'. "
                "Choose 'default' or 'spatial'."
            )

        logger.info(
            f"Starting consolidation for {self.array_uri} (type: {consolidation_type})"
        )
        try:
            if consolidation_type == "default":
                cons_plan = self._generate_default_consolidation_plan()
            else:
                cons_plan = SpatialConsolidationPlanner.compute(
                    self.array_uri, self.ctx
                )

            self._execute_consolidation(cons_plan, parallel_engine)

            for mode in ("array_meta", "fragment_meta", "commits"):
                logger.info(f"Consolidating {mode}...")
                self._consolidate_and_vacuum(mode)

            logger.info(f"Consolidation complete for {self.array_uri}")
        except tiledb.TileDBError as e:
            logger.error(f"Consolidation error: {e}")
            raise

    def _generate_default_consolidation_plan(self):
        with tiledb.open(self.array_uri, "r", ctx=self.ctx) as array:
            fragment_size = self.config["tiledb"]["consolidation_settings"].get(
                "fragment_size", 100_000_000
            )
            return tiledb.ConsolidationPlan(self.ctx, array, fragment_size)

    def _execute_consolidation(
        self, cons_plan, parallel_engine: Optional[object] = None
    ):
        if not cons_plan:
            logger.warning("No consolidation plan generated. Skipping.")
            return

        if isinstance(parallel_engine, concurrent.futures.Executor):
            futures = [
                parallel_engine.submit(
                    tiledb.consolidate,
                    self.array_uri,
                    ctx=self.ctx,
                    config=self.tiledb_config,
                    fragment_uris=plan_["fragment_uris"],
                )
                for plan_ in cons_plan
            ]
            concurrent.futures.wait(futures)
        elif isinstance(parallel_engine, Client):
            futures = [
                parallel_engine.submit(
                    tiledb.consolidate,
                    self.array_uri,
                    ctx=self.ctx,
                    config=self.tiledb_config,
                    fragment_uris=plan_["fragment_uris"],
                )
                for plan_ in cons_plan
            ]
            parallel_engine.gather(futures)
        else:
            for plan_ in cons_plan:
                tiledb.consolidate(
                    self.array_uri,
                    ctx=self.ctx,
                    config=self.tiledb_config,
                    fragment_uris=plan_["fragment_uris"],
                )

        self._vacuum("fragments")

    def _consolidate_and_vacuum(self, mode: str) -> None:
        self.tiledb_config["sm.consolidation.mode"] = mode
        self.tiledb_config["sm.vacuum.mode"] = mode
        tiledb.consolidate(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
        tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)

    def _vacuum(self, mode: str) -> None:
        self.tiledb_config["sm.vacuum.mode"] = mode
        tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)

    # ---------------------------------------------------------------------- #
    # Static helpers
    # ---------------------------------------------------------------------- #

    @staticmethod
    def _load_variables_config(config: dict) -> dict:
        variables_config = {}
        for level in ("level_2a", "level_2b", "level_4a", "level_4c"):
            for var_name, var_info in (
                config.get(level, {}).get("variables", {}).items()
            ):
                variables_config[var_name] = var_info
        return variables_config
