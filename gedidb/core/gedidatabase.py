# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import concurrent.futures
import logging
import os
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
import tiledb
from dask.distributed import Client
from retry import retry

from gedidb.utils.tiledb_consolidation import SpatialConsolidationPlanner
from gedidb.utils.filters import TileDBFilterPolicy


# Configure the logger
logger = logging.getLogger(__name__)


class GEDIDatabase:
    """
    A class to manage the creation and operation of global TileDB arrays for GEDI data storage.
    This class is configured via an external configuration, allowing flexible schema definitions and metadata handling.
    """

    def __init__(self, config: Dict[str, Any], credentials: Optional[dict] = None):
        """
        Initialize GEDIDatabase with configuration, supporting both S3 and local storage.

        Parameters:
        -----------
        config : dict
            Configuration dictionary.
        """
        self.config = config
        self.dimension_names = config["tiledb"]["dimensions"]
        self._spatial_bounds = self._get_tiledb_spatial_domain()
        storage_type = config["tiledb"].get("storage_type", "local").lower()
        self.filter_policy = TileDBFilterPolicy(config.get("tiledb", {}))

        # Set array URIs based on storage type
        if storage_type == "s3":
            bucket = config["tiledb"]["s3_bucket"]
            self.array_uri = os.path.join(f"s3://{bucket}", "array_uri")
        elif storage_type == "local":
            base_path = config["tiledb"].get("local_path", "./")
            self.array_uri = os.path.join(base_path, "array_uri")

        self.overwrite = config["tiledb"].get("overwrite", False)
        self.variables_config = self._load_variables_config(config)

        # Set up TileDB context based on storage type
        if storage_type == "s3":
            self.tiledb_config = tiledb.Config(
                {
                    "vfs.s3.aws_access_key_id": credentials["AccessKeyId"],
                    "vfs.s3.aws_secret_access_key": credentials["SecretAccessKey"],
                    "vfs.s3.endpoint_override": config["tiledb"]["url"],
                    "vfs.s3.use_virtual_addressing": "false",  # must be false for dotted buckets
                    "vfs.s3.enable_upload_file_buffer": "false",  # avoids range PUTs
                    "vfs.s3.use_multipart_upload": "true",
                    "vfs.s3.multipart_part_size": "16777216",  # 16MB
                    "vfs.s3.multipart_threshold": "16777216",  # 16MB
                    "vfs.s3.max_parallel_ops": "8",  # keep DOG healthy
                    "vfs.s3.region": "eu-central-1",  # region ignored by Ceph but required
                    "vfs.s3.scheme": "https",
                    "sm.vfs.s3.connect_timeout_ms": "60000",
                    "sm.vfs.s3.request_timeout_ms": "600000",
                    "sm.vfs.s3.connect_max_tries": "5",
                    "sm.io_concurrency_level": "8",
                    "sm.compute_concurrency_level": "8",
                    "sm.mem.total_budget": "10737418240",
                    "sm.memory_budget": "6442450944",
                    "sm.memory_budget_var": "4294967296",
                }
            )

        elif storage_type == "local":
            # Local TileDB context with consolidation settings
            self.tiledb_config = tiledb.Config(
                {
                    # Memory budget settings
                    "sm.memory_budget": config["tiledb"]["consolidation_settings"].get(
                        "memory_budget", "5000000000"
                    ),
                    "sm.memory_budget_var": config["tiledb"][
                        "consolidation_settings"
                    ].get("memory_budget_var", "2000000000"),
                }
            )

        self.ctx = tiledb.Ctx(self.tiledb_config)
        self._schema_cache = None

    def spatial_chunking(
        self,
        dataset: pd.DataFrame,
        tiles_across_lat: int = 5,
        tiles_across_lon: int = 5,
    ):
        """
        Yield ((lat_min, lat_max, lon_min, lon_max), view) pairs without
        building a large dict. 'view' is a cheap row-subset of the original DataFrame.
        """
        if dataset.empty:
            return
        if not {"latitude", "longitude"}.issubset(dataset.columns):
            raise ValueError("Dataset must contain 'latitude' and 'longitude' columns.")

        with tiledb.open(self.array_uri, "r", ctx=self.ctx) as A:
            dom = A.schema.domain
            dim_lat, dim_lon = dom.dim(0), dom.dim(1)
            lat_tile, lon_tile = float(dim_lat.tile), float(dim_lon.tile)
            lat_min_dom, lon_min_dom = float(dim_lat.domain[0]), float(
                dim_lon.domain[0]
            )

        block_lat = lat_tile * tiles_across_lat
        block_lon = lon_tile * tiles_across_lon

        lat_idx = np.floor(
            (dataset["latitude"].to_numpy() - lat_min_dom) / block_lat
        ).astype(int)
        lon_idx = np.floor(
            (dataset["longitude"].to_numpy() - lon_min_dom) / block_lon
        ).astype(int)

        # Use groups' index arrays to avoid copying whole grouped frames
        groups = dataset.groupby([lat_idx, lon_idx], sort=False).groups
        for (i_lat, i_lon), idx in groups.items():
            lat0 = lat_min_dom + i_lat * block_lat
            lon0 = lon_min_dom + i_lon * block_lon
            bounds = (lat0, lat0 + block_lat, lon0, lon0 + block_lon)
            yield bounds, dataset.take(idx)

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
        Consolidate fragments, metadata, and commit logs for the array to optimize storage and access.

        Parameters:
        ----------
        consolidation_type : str, default='default'
            Type of consolidation to perform. Options: 'default', 'spatial'.
        parallel_engine : object, optional
            Parallelization engine such as `concurrent.futures.Executor` or
            `dask.distributed.Client`. Defaults to single-threaded execution.

        Raises:
        -------
        ValueError:
            If an invalid consolidation_type is provided.
        TileDBError:
            If consolidation or vacuum operations fail.
        """
        if consolidation_type not in {"default", "spatial"}:
            raise ValueError(
                f"Invalid consolidation_type: {consolidation_type}. Choose 'default' or 'spatial'."
            )

        logger.info(
            f"Starting consolidation process for array: {self.array_uri} (type: {consolidation_type})"
        )

        try:
            # Generate the consolidation plan based on type
            if consolidation_type == "default":
                cons_plan = self._generate_default_consolidation_plan()
            elif consolidation_type == "spatial":
                cons_plan = SpatialConsolidationPlanner.compute(
                    self.array_uri, self.ctx
                )

            logger.info("Executing consolidation...")
            self._execute_consolidation(cons_plan, parallel_engine)
            logger.info("Consolidation execution completed.")

            logger.info("Consolidating array metadata...")
            self._consolidate_and_vacuum("array_meta")
            logger.info("Consolidating fragment metadata...")
            self._consolidate_and_vacuum("fragment_meta")
            logger.info("Consolidating commit logs...")
            self._consolidate_and_vacuum("commits")

            logger.info(f"Consolidation complete for {self.array_uri}")

        except tiledb.TileDBError as e:
            logger.error(f"Error during consolidation of {self.array_uri}: {e}")
            raise

    def _generate_default_consolidation_plan(self):
        """Generate a default consolidation plan for fragments."""
        with tiledb.open(self.array_uri, "r", ctx=self.ctx) as array_:
            fragment_size = self.config["tiledb"]["consolidation_settings"].get(
                "fragment_size", 100_000_000
            )
            return tiledb.ConsolidationPlan(self.ctx, array_, fragment_size)

    def _execute_consolidation(
        self, cons_plan, parallel_engine: Optional[object] = None
    ):
        """
        Execute the consolidation tasks using the specified parallel engine.

        Parameters:
        ----------
        cons_plan : List[Dict]
            Consolidation plan generated for the array.
        parallel_engine : object, optional
            Parallelization engine such as `concurrent.futures.Executor` or
            `dask.distributed.Client`. Defaults to single-threaded execution.
        """
        if not cons_plan:
            logger.warning("No consolidation plan generated. Skipping consolidation.")
            return

        if isinstance(parallel_engine, concurrent.futures.Executor):
            # Use the provided concurrent.futures.Executor
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
            # Use Dask client if provided
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
            # Default to single-threaded execution
            for plan_ in cons_plan:
                tiledb.consolidate(
                    self.array_uri,
                    ctx=self.ctx,
                    config=self.tiledb_config,
                    fragment_uris=plan_["fragment_uris"],
                )

        # Vacuum fragments after consolidation
        self._vacuum("fragments")

    def _consolidate_and_vacuum(self, mode: str):
        """
        Consolidate and vacuum data based on the specified mode.

        Parameters:
        ----------
        mode : str
            The consolidation mode (e.g., 'array_meta', 'fragment_meta', 'commits').
        """
        self.tiledb_config["sm.consolidation.mode"] = mode
        self.tiledb_config["sm.vacuum.mode"] = mode
        tiledb.consolidate(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
        tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)

    def _vacuum(self, mode: str):
        """
        Vacuum the specified mode for the array.

        Parameters:
        ----------
        mode : str
            The vacuum mode (e.g., 'fragments', 'array_meta').
        """
        self.tiledb_config["sm.vacuum.mode"] = mode
        tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)

    def _create_arrays(self) -> None:
        """Define and create TileDB arrays based on configuration."""
        self._create_array(self.array_uri)
        self._add_variable_metadata()

    def _create_array(self, uri: str) -> None:
        """
        Creates a TileDB array based on the provided URI and configuration.

        Parameters:
        ----------
        uri : str
            The URI of the TileDB array to be created.

        Raises:
        -------
        ValueError:
            If the domain or attributes configuration is invalid.
        """
        if tiledb.array_exists(uri, ctx=self.ctx):
            if self.overwrite:
                tiledb.remove(uri, ctx=self.ctx)
                self._schema_cache = None
                logger.info(f"Overwritten existing TileDB array at {uri}")
            else:
                logger.info(f"TileDB array already exists at {uri}. Skipping creation.")
                return

        try:
            domain = self._create_domain()
            attributes = self._create_attributes()

            schema = tiledb.ArraySchema(
                domain=domain,
                attrs=attributes,
                sparse=True,
                capacity=self.config.get("tiledb", {}).get("capacity", 200000),
                cell_order=self.config.get("tiledb", {}).get("cell_order", "hilbert"),
            )
            tiledb.Array.create(uri, schema, ctx=self.ctx)
            logger.info(f"Successfully created TileDB array at {uri}")
        except ValueError as e:
            logger.error(f"Failed to create array: {e}")
            raise

    def _create_domain(self) -> tiledb.Domain:
        """
        Create the TileDB domain for storing GEDI data, including spatial (latitude,
        longitude) and temporal (time) dimensions with appropriate compression filters.

        Configuration Parameters
        ------------------------
        The following keys are expected under `config["tiledb"]`:

        **Spatial domain**
            - ``spatial_range.lat_min`` : float
                Minimum latitude (degrees north).
            - ``spatial_range.lat_max`` : float
                Maximum latitude (degrees north).
            - ``spatial_range.lon_min`` : float
                Minimum longitude (degrees east).
            - ``spatial_range.lon_max`` : float
                Maximum longitude (degrees east).

        **Temporal domain**
            - ``time_range.start_time`` : datetime-like or str
                Start of temporal coverage. Converted to integer days since epoch.
            - ``time_range.end_time`` : datetime-like or str
                End of temporal coverage. Converted to integer days since epoch.

        **Tiling and precision**
            - ``scale_factor`` : float, optional
                Precision factor for FloatScaleFilter (default = 1e-6).
                A factor of 1e-6 corresponds to ≈0.11 m spatial precision.
            - ``latitude_tile`` : int, optional
                Tile size (number of cells) along the latitude dimension (default = 1).
            - ``longitude_tile`` : int, optional
                Tile size along the longitude dimension (default = 1).
            - ``time_tile`` : int, optional
                Tile size (in days) along the time dimension (default = 365).

        Returns
        -------
        tiledb.Domain
            A TileDB domain object with three dimensions:
                ``latitude (float64)``, ``longitude (float64)``, and ``time (int64)``.
            Each dimension has filters chosen for precision and compression efficiency.

        Raises
        ------
        ValueError
            If spatial or temporal ranges are missing, or if any of the ranges are invalid
            (e.g., lat_min >= lat_max).

        Examples
        --------
        >>> domain = self._create_domain()
        >>> list(domain)
        [Dim(name='latitude',  domain=(-60.0, 60.0),  tile=1, dtype='float64'),
         Dim(name='longitude', domain=(-180.0, 180.0), tile=1, dtype='float64'),
         Dim(name='time',      domain=(18262, 18993), tile=365, dtype='int64')]
        """
        cfg_td = self.config.get("tiledb", {})
        spatial_range = cfg_td.get("spatial_range", {})
        time_range = cfg_td.get("time_range", {})

        lat_min = spatial_range.get("lat_min")
        lat_max = spatial_range.get("lat_max")
        lon_min = spatial_range.get("lon_min")
        lon_max = spatial_range.get("lon_max")

        time_min = np.datetime64(time_range.get("start_time"), "D")
        time_max = np.datetime64(time_range.get("end_time"), "D")

        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError(
                "Spatial and temporal ranges must be fully specified in the configuration."
            )
        if not (lat_min < lat_max and lon_min < lon_max and time_min < time_max):
            raise ValueError(
                "Invalid ranges: lat_min < lat_max, lon_min < lon_max, time_min < time_max required."
            )

        scale_factor = cfg_td.get("scale_factor", 1e-6)
        lat_tile = cfg_td.get("latitude_tile", 1)
        lon_tile = cfg_td.get("longitude_tile", 1)
        time_tile = np.timedelta64(cfg_td.get("time_tile", 14), "D")

        spatial_filters = self.filter_policy.spatial_dim_filters(scale_factor)
        time_filters = self.filter_policy.time_dim_filters()

        dim_lat = tiledb.Dim(
            name="latitude",
            domain=(lat_min, lat_max),
            tile=lat_tile,
            dtype=np.float64,
            filters=spatial_filters,
        )
        dim_lon = tiledb.Dim(
            name="longitude",
            domain=(lon_min, lon_max),
            tile=lon_tile,
            dtype=np.float64,
            filters=spatial_filters,
        )
        dim_time = tiledb.Dim(
            name="time",
            domain=(time_min, time_max),
            tile=time_tile,
            dtype=np.datetime64("", "D").dtype,
            filters=time_filters,
        )

        return tiledb.Domain(dim_lat, dim_lon, dim_time)

    def _create_attributes(self) -> list[tiledb.Attr]:
        """
        Create TileDB attributes for all configured GEDI variables, assigning appropriate
        compression filters based on their data type.

        Compression Strategy
        --------------------
        Compression filters are selected by :meth:`TileDBFilterPolicy.filters_for_dtype`
        based on the variable's dtype:

            | Type              | Filters applied
            |-------------------|---------------------------------------------|
            | float32 / float64 | ByteShuffle + Zstd(level from config)       |
            | uint8 (flags)     | (BitWidthReduction) + RLE + Zstd            |
            | int / uint types  | (BitWidthReduction) + Zstd                  |
            | string (U)        | Zstd(level from config)                     |

        Profile attributes are expanded into multiple attributes with numeric suffixes,
        e.g. `rh_1`, `rh_2`, … `rh_N`.

        An additional attribute `timestamp_ns` (int64) is optionally included to support
        deduplication and versioning of records, compressed using BitWidthReduction +
        Zstd(level=2). This behavior can be toggled in the configuration via:

            ``tiledb.write_timestamp_ns: true``

        Returns
        -------
        list[tiledb.Attr]
            List of TileDB attribute definitions with dtype-specific compression filters.

        Raises
        ------
        ValueError
            If `variables_config` is missing, malformed, or if a profile variable has
            an invalid `profile_length`.

        Examples
        --------
        >>> attrs = self._create_attributes()
        >>> attrs[:3]
        [Attr(name='agbd', dtype='float32', filters=ByteShuffle+Zstd(5)),
         Attr(name='degrade_flag', dtype='uint8', filters=RLE+Zstd(3)),
         Attr(name='rh_1', dtype='float32', filters=ByteShuffle+Zstd(5))]

        """
        if not self.variables_config:
            raise ValueError(
                "Variable configuration is missing. Cannot create attributes."
            )

        attrs: list[tiledb.Attr] = []

        # --- Scalar attributes (non-profile variables)
        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                continue

            dtype = np.dtype(var_info["dtype"])
            filters = self.filter_policy.filters_for_dtype(dtype)

            attrs.append(
                tiledb.Attr(
                    name=var_name,
                    dtype=dtype,
                    filters=filters,
                )
            )

        # --- Profile attributes (e.g. rh_1, rh_2, ..., rh_N)
        for var_name, var_info in self.variables_config.items():
            if not var_info.get("is_profile", False):
                continue

            profile_length = int(var_info.get("profile_length", 1))
            if profile_length <= 0:
                raise ValueError(f"{var_name}: profile_length must be >= 1")

            dtype = np.dtype(var_info["dtype"])
            filters = self.filter_policy.filters_for_dtype(dtype)

            for i in range(profile_length):
                attrs.append(
                    tiledb.Attr(
                        name=f"{var_name}_{i + 1}",
                        dtype=dtype,
                        filters=filters,
                    )
                )

        # --- Optional timestamp_ns attribute
        if self.config.get("tiledb", {}).get("write_timestamp_ns", True):
            attrs.append(
                tiledb.Attr(
                    name="timestamp_ns",
                    dtype=np.int64,
                    filters=self.filter_policy.timestamp_filters(),
                )
            )

        return attrs

    def _add_variable_metadata(self) -> None:
        """
        Add metadata to the global TileDB arrays for each variable, including units, description, dtype,
        and other relevant information. This metadata is pulled from the configuration.
        """
        if not self.variables_config:
            logger.warning(
                "No variables configuration found. Skipping metadata addition."
            )
            return

        try:
            with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
                for var_name, var_info in self.variables_config.items():
                    # Extract metadata attributes with defaults
                    metadata = {
                        "units": var_info.get("units", "unknown"),
                        "description": var_info.get(
                            "description", "No description available"
                        ),
                        "dtype": var_info.get("dtype", "unknown"),
                        "product_level": var_info.get("product_level", "unknown"),
                    }

                    # Add metadata to the array
                    for key, value in metadata.items():
                        array.meta[f"{var_name}.{key}"] = value

                    # Add profile-specific metadata
                    if var_info.get("is_profile", False):
                        array.meta[f"{var_name}.profile_length"] = var_info.get(
                            "profile_length", 0
                        )

        except tiledb.TileDBError as e:
            logger.error(f"Error adding metadata to TileDB array: {e}")
            raise

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

    def _get_schema_cache(self):
        """
        Read schema domain + attr names once (lazy), cache on the instance.
        """
        if getattr(self, "_schema_cache", None) is not None:
            return self._schema_cache

        with tiledb.open(self.array_uri, "r", ctx=self.ctx) as A:
            dom = A.schema.domain
            tmin_dom, tmax_dom = dom.dim("time").domain
            schema_attrs = {A.schema.attr(i).name for i in range(A.schema.nattr)}

            # Cache dtype per attribute from schema (authoritative)
            schema_attr_dtypes = {
                A.schema.attr(i).name: np.dtype(A.schema.attr(i).dtype)
                for i in range(A.schema.nattr)
            }

        self._schema_cache = {
            "tmin": tmin_dom,
            "tmax": tmax_dom,
            "attrs": schema_attrs,
            "attr_dtypes": schema_attr_dtypes,
        }
        return self._schema_cache

    def _coerce_series_for_attr(
        self, name: str, s: pd.Series, target_dtype: np.dtype
    ) -> np.ndarray:
        """
        Coerce a pandas Series to a NumPy array compatible with the TileDB attribute dtype.
        This is *schema-driven* (not heuristic).
        """
        dt = np.dtype(target_dtype)

        # String attributes: produce clean fixed-width unicode/bytes arrays (no object dtype)
        if dt.kind in ("U", "S"):
            # Normalize bytes/None/NaN -> string, then cast to schema dtype (Uxx or Sxx)
            out = s.astype("string").fillna("")
            return out.to_numpy(dtype=dt)

        # Datetime attributes (rare as attrs, but handle)
        if dt.kind == "M":
            t = pd.to_datetime(s, utc=True, errors="coerce")
            return t.dt.tz_localize(None).to_numpy(dtype=dt)

        return s.to_numpy(dtype=dt, copy=False)

    def write_granule(self, df: pd.DataFrame) -> None:
        """
        Write GEDI shots into the TileDB sparse array, enforcing:
          - spatial bounds clipping
          - UTC time parsing
          - flooring to UTC day for the time dimension (datetime64[D])
          - time-domain clipping
          - schema-consistent attribute selection + dtype coercion (incl. fixed-width strings)
          - optional timestamp_ns attribute (int64 ns)
        """
        try:
            if df is None or df.empty:
                return

            # ---- required columns
            required = {"latitude", "longitude", "time"}
            missing = required - set(df.columns)
            if missing:
                raise ValueError(
                    f"write_granule: missing required columns: {sorted(missing)}"
                )

            # ---- spatial clip
            min_lon, max_lon, min_lat, max_lat = self._spatial_bounds
            m_spatial = (
                (df["longitude"] >= min_lon)
                & (df["longitude"] <= max_lon)
                & (df["latitude"] >= min_lat)
                & (df["latitude"] <= max_lat)
            )
            if not bool(m_spatial.any()):
                return
            df = df.loc[m_spatial].copy()

            # ---- schema cache (domain + attr names + attr dtypes)
            cache = self._get_schema_cache()
            tmin_dom, tmax_dom = cache["tmin"], cache["tmax"]
            schema_attrs = cache["attrs"]
            schema_attr_dtypes = cache["attr_dtypes"]

            # ---- time parsing (UTC) -> floor to day -> numpy datetime64[D]
            t = pd.to_datetime(df["time"], utc=True, errors="coerce")
            m_valid = t.notna()
            if not bool(m_valid.any()):
                return

            t_day = t.dt.floor("D")
            tD = t_day.dt.tz_localize(None).to_numpy(dtype="datetime64[D]")

            # ---- time-domain clip
            m_in = (tD >= tmin_dom) & (tD <= tmax_dom)
            m = m_valid.to_numpy(dtype=bool) & m_in
            if not bool(m.any()):
                return

            idx = np.flatnonzero(m)
            df = df.iloc[idx].copy()
            t = t.iloc[idx]
            tD = tD[m]

            n = len(df)
            if n == 0:
                return

            # ---- coords
            coords = {
                "latitude": df["latitude"].to_numpy(dtype=np.float64, copy=False),
                "longitude": df["longitude"].to_numpy(dtype=np.float64, copy=False),
                "time": tD,
            }

            # ---- data dict: only schema attrs, excluding dims
            dim_names = {"latitude", "longitude", "time"}
            data: dict[str, np.ndarray] = {}

            for name in sorted(schema_attrs):
                if name in dim_names:
                    continue
                if name == "timestamp_ns":
                    continue
                if name not in df.columns:
                    continue

                target_dtype = schema_attr_dtypes[name]
                arr = self._coerce_series_for_attr(name, df[name], target_dtype)

                if len(arr) != n:
                    raise ValueError(f"Attribute '{name}' length {len(arr)} != n={n}")
                data[name] = arr

            # ---- timestamp_ns (true UTC ns)
            if "timestamp_ns" in schema_attrs:
                ts_ns = t.astype("int64").to_numpy(copy=False)  # ns since epoch UTC
                if len(ts_ns) != n:
                    raise ValueError(f"timestamp_ns length {len(ts_ns)} != n={n}")
                data["timestamp_ns"] = ts_ns

            if not data:
                return

            # ---- sanity: coords lengths
            for k, v in coords.items():
                if len(v) != n:
                    raise ValueError(f"Coord '{k}' length {len(v)} != n={n}")

            self._write_to_tiledb(coords, data)

        except Exception as e:
            logger.error(
                f"Failed to process and write granule data: {e}", exc_info=True
            )
            raise

    def check_granules_status(self, granule_ids: list) -> dict:
        """
        Check the status of multiple granules by accessing all metadata at once.

        Parameters:
        ----------
        granule_ids : list
            A list of unique granule identifiers to check.

        Returns:
        --------
        dict
            A dictionary where the keys are granule IDs and the values are booleans.
            True if the granule has been processed, False otherwise.
        """
        granule_statuses = {}

        try:
            with tiledb.open(self.array_uri, mode="r", ctx=self.ctx) as array:
                # Extract all status metadata in one pass
                metadata = {
                    key: array.meta[key]
                    for key in array.meta.keys()
                    if "_status" in key
                }

            # Check each granule's status
            for granule_id in granule_ids:
                granule_key = f"granule_{granule_id}_status"
                granule_statuses[granule_id] = metadata.get(granule_key) == "processed"

            processed_count = sum(granule_statuses.values())
            logger.debug(
                f"Checked {len(granule_ids)} granules: "
                f"{processed_count} processed, {len(granule_ids) - processed_count} pending"
            )

        except tiledb.TileDBError as e:
            logger.error(f"Failed to access TileDB metadata: {e}")
            # Return all False on error (conservative approach)
            granule_statuses = {gid: False for gid in granule_ids}

        return granule_statuses

    @retry(
        (tiledb.TileDBError, ConnectionError),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger,
    )
    def mark_granule_as_processed(self, granule_key: str) -> None:
        """
        Mark a granule as processed by storing its status and processing date in TileDB metadata.

        Parameters:
        ----------
        granule_key : str
            The unique identifier for the granule.
        """
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

    @staticmethod
    def _load_variables_config(config):
        """
        Load and parse the configuration file and consolidate variables from all product levels.

        Returns:
        --------
        dict:
            The dictionary representation of all variables configuration across products.
        """
        # Consolidate all variables from different levels
        variables_config = {}
        for level in ["level_2a", "level_2b", "level_4a", "level_4c"]:
            level_vars = config.get(level, {}).get("variables", {})
            for var_name, var_info in level_vars.items():
                variables_config[var_name] = var_info

        return variables_config

    def _get_tiledb_spatial_domain(self):
        """
        Retrieve the spatial domain (bounding box) from the configuration file.

        Returns:
        -------
        Tuple[float, float, float, float]
            (min_longitude, max_longitude, min_latitude, max_latitude)
        """
        spatial_config = self.config["tiledb"]["spatial_range"]
        min_lat = spatial_config["lat_min"]
        max_lat = spatial_config["lat_max"]
        min_lon = spatial_config["lon_min"]
        max_lon = spatial_config["lon_max"]

        return min_lon, max_lon, min_lat, max_lat
