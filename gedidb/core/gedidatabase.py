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
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import tiledb
from dask.distributed import Client
from retry import retry
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype

from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
    convert_to_days_since_epoch,
)
from gedidb.utils.tiledb_consolidation import SpatialConsolidationPlanner

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
            # S3 TileDB context with consolidation settings
            self.tiledb_config = tiledb.Config(
                {
                    # S3-specific configurations (if using S3)
                    "vfs.s3.aws_access_key_id": credentials["AccessKeyId"],
                    "vfs.s3.aws_secret_access_key": credentials["SecretAccessKey"],
                    "vfs.s3.endpoint_override": config["tiledb"]["url"],
                    "vfs.s3.region": "eu-central-1",
                    "vfs.s3.use_virtual_addressing": "true",
                    "vfs.s3.max_parallel_ops": str(os.cpu_count() or 8),
                    # S3 writting settings
                    "sm.vfs.s3.connect_timeout_ms": config["tiledb"]["s3_settings"].get(
                        "connect_timeout_ms", "10800"
                    ),
                    "sm.vfs.s3.request_timeout_ms": config["tiledb"]["s3_settings"].get(
                        "request_timeout_ms", "3000"
                    ),
                    "sm.vfs.s3.connect_max_tries": config["tiledb"]["s3_settings"].get(
                        "connect_max_tries", "5"
                    ),
                    "vfs.s3.backoff_scale": config["tiledb"]["s3_settings"].get(
                        "backoff_scale", "2.0"
                    ),  # Exponential backoff multiplier
                    "vfs.s3.backoff_max_ms": config["tiledb"]["s3_settings"].get(
                        "backoff_max_ms", "120000"
                    ),  # Maximum backoff time of 120 seconds
                    "vfs.s3.multipart_part_size": config["tiledb"]["s3_settings"].get(
                        "multipart_part_size", "52428800"
                    ),  # 50 MB
                    # Memory budget settings
                    "sm.memory_budget": config["tiledb"]["consolidation_settings"].get(
                        "memory_budget", "5000000000"
                    ),
                    "sm.memory_budget_var": config["tiledb"][
                        "consolidation_settings"
                    ].get("memory_budget_var", "2000000000"),
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

    def spatial_chunking(
        self,
        dataset: pd.DataFrame,
        tiles_across_lat: int = 10,
        tiles_across_lon: int = 10,
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
        Creates TileDB domain with FloatScaleFilter for lat/lon dimensions.

        BENEFITS:
        - Store as float64 (4 bytes), scale to int32 internally
        - DoubleDelta compression works on scaled integers
        - No manual conversion in query code
        - Precision controlled by scale factor

        PRECISION OPTIONS:
        factor=1e-6 → 0.11m precision (your current approach)
        factor=1e-7 → 0.011m precision (matches float32)
        factor=1e-5 → 1.1m precision (sufficient for GEDI)
        """
        spatial_range = self.config.get("tiledb", {}).get("spatial_range", {})
        time_range = self.config.get("tiledb", {}).get("time_range", {})

        lat_min = spatial_range.get("lat_min")
        lat_max = spatial_range.get("lat_max")
        lon_min = spatial_range.get("lon_min")
        lon_max = spatial_range.get("lon_max")

        time_min = _datetime_to_timestamp_days(time_range.get("start_time"))
        time_max = _datetime_to_timestamp_days(time_range.get("end_time"))

        # Validate ranges
        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError(
                "Spatial and temporal ranges must be fully specified in the configuration."
            )
        if not (lat_min < lat_max and lon_min < lon_max and time_min < time_max):
            raise ValueError(
                "Invalid ranges: lat_min < lat_max, lon_min < lon_max, time_min < time_max required."
            )

        # Precision factor for FloatScaleFilter
        scale_factor = self.config.get("tiledb", {}).get("scale_factor", 1e-6)

        # Tile sizes
        lat_tile = self.config.get("tiledb", {}).get("latitude_tile", 1)
        lon_tile = self.config.get("tiledb", {}).get("longitude_tile", 1)
        time_tile = self.config.get("tiledb", {}).get("time_tile", 365)

        # Spatial filters: FloatScaleFilter + DoubleDelta compression
        spatial_filters = tiledb.FilterList(
            [
                tiledb.FloatScaleFilter(
                    factor=scale_factor,
                    offset=0.0,
                    bytewidth=4,  # int32 internal storage
                ),
                tiledb.DoubleDeltaFilter(),  # Excellent for spatial data
                tiledb.BitWidthReductionFilter(),
                tiledb.ZstdFilter(level=3),
            ]
        )

        # Temporal filters: DoubleDelta works great for time series
        time_filters = tiledb.FilterList(
            [tiledb.DoubleDeltaFilter(), tiledb.ZstdFilter(level=3)]
        )

        # Create dimensions (dtype=float64 for user-facing API)
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
            dtype=np.int64,
            filters=time_filters,
        )

        return tiledb.Domain(dim_lat, dim_lon, dim_time)

    def filters_float(self, lvl=None):
        lvl = (
            int(self.config.get("tiledb", {}).get("profiles_zstd_level", 5))
            if lvl is None
            else lvl
        )
        return tiledb.FilterList(
            [tiledb.ByteShuffleFilter(), tiledb.ZstdFilter(level=lvl)]
        )

    def filters_float64(self, lvl=None):
        # slightly lower level if you care about speed
        lvl = (
            int(self.config.get("tiledb", {}).get("float64_zstd_level", 4))
            if lvl is None
            else lvl
        )
        return tiledb.FilterList(
            [tiledb.ByteShuffleFilter(), tiledb.ZstdFilter(level=lvl)]
        )

    def filters_flags(self, lvl=None):
        lvl = (
            int(self.config.get("tiledb", {}).get("flags_zstd_level", 3))
            if lvl is None
            else lvl
        )
        fl = [tiledb.RleFilter(), tiledb.ZstdFilter(level=lvl)]
        # Optional: if your TileDB has BitWidthReductionFilter, prepend it
        try:
            fl.insert(0, tiledb.BitWidthReductionFilter())
        except Exception:
            pass
        return tiledb.FilterList(fl)

    def filters_small_int(self, lvl=None):
        lvl = (
            int(self.config.get("tiledb", {}).get("int_zstd_level", 3))
            if lvl is None
            else lvl
        )
        fl = [tiledb.ZstdFilter(level=lvl)]
        try:
            fl.insert(0, tiledb.BitWidthReductionFilter())
        except Exception:
            pass
        return tiledb.FilterList(fl)

    def filters_utf8(self, lvl=None):
        lvl = (
            int(self.config.get("tiledb", {}).get("string_zstd_level", 3))
            if lvl is None
            else lvl
        )
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def _create_attributes(self) -> List[tiledb.Attr]:
        if not self.variables_config:
            raise ValueError(
                "Variable configuration is missing. Cannot create attributes."
            )

        attributes: List[tiledb.Attr] = []

        def choose_filters(var_name: str, dtype: np.dtype) -> tiledb.FilterList:
            k = dtype.kind  # 'f', 'i', 'u', 'U'
            if k == "f":
                return (
                    self.filters_float64()
                    if dtype == np.float64
                    else self.filters_float()
                )
            if k in ("i", "u"):
                return (
                    self.filters_flags()
                    if dtype == np.uint8
                    else self.filters_small_int()
                )
            if k == "U":
                return self.filters_utf8()
            return tiledb.FilterList([tiledb.ZstdFilter(level=3)])

        # Non-profile attrs
        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                continue
            dtype = np.dtype(var_info["dtype"])
            fl = choose_filters(var_name, dtype)
            attributes.append(tiledb.Attr(name=var_name, dtype=dtype, filters=fl))

        # Profile attrs (…_1 .. …_N)
        for var_name, var_info in self.variables_config.items():
            if not var_info.get("is_profile", False):
                continue
            profile_length = int(var_info.get("profile_length", 1))
            if profile_length <= 0:
                raise ValueError(f"{var_name}: profile_length must be >= 1")
            dtype = np.dtype(var_info["dtype"])
            fl = choose_filters(var_name, dtype)
            for i in range(profile_length):
                attributes.append(
                    tiledb.Attr(name=f"{var_name}_{i + 1}", dtype=dtype, filters=fl)
                )

        # Optional timestamp (ns) — robust to unordered/duplicate values
        if self.config.get("tiledb", {}).get("write_timestamp_ns", True):
            try:
                ts_filters = tiledb.FilterList(
                    [tiledb.BitWidthReductionFilter(), tiledb.ZstdFilter(level=2)]
                )
            except Exception:
                ts_filters = tiledb.FilterList([tiledb.ZstdFilter(level=2)])
            attributes.append(
                tiledb.Attr(name="timestamp_ns", dtype="int64", filters=ts_filters)
            )

        return attributes

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

    def write_granule(
        self,
        granule_data: pd.DataFrame,
        row_batch: int = 100_000,
    ) -> None:
        """
        Memory-lean write with row-only batching (no attribute batching).
        Writes *all* attributes present in the schema for each row batch.
        """
        try:
            if granule_data.empty:
                return

            # --- Deduplicate (lat, lon, time) with minimal temporaries
            latv = granule_data["latitude"].to_numpy(copy=False)
            lonv = granule_data["longitude"].to_numpy(copy=False)
            tv = granule_data["time"].to_numpy(copy=False)

            # Fast-ish composite key; robust across dtypes
            keys = pd.Index(
                np._core.defchararray.add(
                    np._core.defchararray.add(latv.astype("S"), b"|"),
                    np._core.defchararray.add(
                        lonv.astype("S"),
                        np._core.defchararray.add(b"|", tv.astype("S")),
                    ),
                )
            )
            keep = ~keys.duplicated()

            # --- Spatial mask (NumPy)
            min_lon, max_lon, min_lat, max_lat = self._spatial_bounds
            in_box = (
                (lonv >= min_lon)
                & (lonv <= max_lon)
                & (latv >= min_lat)
                & (latv <= max_lat)
            )

            sel = np.flatnonzero(keep & in_box)
            if sel.size == 0:
                return

            view = granule_data.iloc[sel]  # one slice only

            # --- Coords (precompute once)
            coords_all: Dict[str, np.ndarray] = {}
            for dim_name in self.dimension_names:
                if dim_name == "time":
                    tvals = view["time"].to_numpy(copy=False)
                    coords_all["time"] = convert_to_days_since_epoch(tvals)
                else:
                    coords_all[dim_name] = view[dim_name].to_numpy(copy=False)

            # --- timestamp_ns once (avoid repeated to_datetime)
            tcol = view["time"]
            if is_datetime64_any_dtype(tcol):
                if is_datetime64tz_dtype(tcol):
                    timestamp_ns_all = tcol.astype("int64").to_numpy(copy=False)
                else:
                    timestamp_ns_all = tcol.to_numpy(copy=False).view("int64")
            else:
                timestamp_ns_all = (
                    pd.to_datetime(tcol, utc=True).astype("int64").to_numpy()
                )

            # --- Figure out which attributes exist in both schema and frame
            #     We will write *all* of them in each batch.
            with tiledb.open(self.array_uri, mode="r", ctx=self.ctx) as A_ro:
                schema_attrs = {
                    A_ro.schema.attr(i).name for i in range(A_ro.schema.nattr)
                }

            available_cols = set(view.columns)
            attrs_all = [name for name in schema_attrs if name in available_cols]

            # Optional: dtype coercion (numeric only); safe to remove if annoying
            for v in attrs_all:
                info = self.variables_config.get(v.split("_")[0], {})
                dtype_str = info.get("dtype", str(view[v].dtype))
                try:
                    target = np.dtype(dtype_str)
                except TypeError:
                    continue  # skip e.g. 'string[python]'
                if view[v].dtype != target and target.kind in "fiu":
                    view[v] = view[v].astype(target, copy=False)

            # Some schemas include timestamp_ns as an attribute; add if present
            write_timestamp = "timestamp_ns" in schema_attrs

            # --- Row-only batching: build full data dict per batch and write once
            n = len(view)
            for r0 in range(0, n, row_batch):
                r1 = min(r0 + row_batch, n)
                coords = {k: v[r0:r1] for k, v in coords_all.items()}

                data = {}
                if write_timestamp:
                    data["timestamp_ns"] = timestamp_ns_all[r0:r1]

                # Slice each attr once (zero-copy view where possible)
                for a in attrs_all:
                    data[a] = view[a].to_numpy(copy=False)[r0:r1]

                self._write_to_tiledb(coords, data)

        except Exception as e:
            logger.error(
                f"Failed to process and write granule data: {e}", exc_info=True
            )
            raise

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
        # Use set operations for efficient membership testing
        required_dims = set(self.dimension_names)
        available_cols = set(granule_data.columns)
        missing_dims = required_dims - available_cols

        if missing_dims:
            raise ValueError(
                f"Granule data is missing required dimensions: {sorted(missing_dims)}"
            )

    def _prepare_coordinates(self, granule_data: pd.DataFrame) -> Dict[str, np.ndarray]:
        """
        Prepare coordinate data for dimensions based on the granule DataFrame.
        Converts latitude/longitude from float degrees to integer quantized degrees
        for consistency with the TileDB domain.
        """
        coords = {}

        for dim_name in self.dimension_names:
            if dim_name == "time":
                # Convert timestamps to days since epoch
                coords[dim_name] = convert_to_days_since_epoch(
                    granule_data[dim_name].values
                )
            else:
                # Latitude/longitude:
                coords[dim_name] = granule_data[dim_name].values.astype(
                    np.float64, copy=False
                )

        return coords

    def _extract_variable_data(
        self, granule_data: pd.DataFrame
    ) -> Dict[str, np.ndarray]:
        """
        Extract scalar and profile variable data from the granule DataFrame.

        Parameters:
        ----------
        granule_data : pd.DataFrame
            The DataFrame containing granule data.

        Returns:
        --------
        Dict[str, np.ndarray]
            A dictionary of variable data for writing to TileDB.
        """
        data = {}
        available_cols = set(granule_data.columns)

        # Separate scalar and profile variables for more efficient processing
        scalar_vars = []
        profile_vars = []

        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                profile_vars.append((var_name, var_info))
            else:
                scalar_vars.append(var_name)

        # Extract scalar variables in batch
        scalar_vars_present = [v for v in scalar_vars if v in available_cols]
        if scalar_vars_present:
            # Batch extract using dict comprehension
            data.update({var: granule_data[var].values for var in scalar_vars_present})

        # Extract profile variables
        for var_name, var_info in profile_vars:
            profile_length = var_info.get("profile_length", 1)

            # Generate all profile column names
            profile_cols = [f"{var_name}_{i + 1}" for i in range(profile_length)]

            # Find which profile columns actually exist
            existing_profile_cols = [
                col for col in profile_cols if col in available_cols
            ]

            if existing_profile_cols:
                # Batch extract all layers at once
                for col in existing_profile_cols:
                    data[col] = granule_data[col].values

        # Add timestamp (convert to microseconds since epoch)
        data["timestamp_ns"] = (
            pd.to_datetime(granule_data["time"]).astype("int64")
        ).values

        return data

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
