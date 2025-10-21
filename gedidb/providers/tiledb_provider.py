# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import logging
import os
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.vectorized import contains
import tiledb

logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]


class TileDBProvider:
    """
    An optimized provider class for managing low-level interactions with TileDB arrays for GEDI data.
    Supports irregular polygon filtering and efficient query building.
    """

    def __init__(
        self,
        storage_type: str = "local",
        s3_bucket: Optional[str] = None,
        local_path: Optional[str] = "./",
        url: Optional[str] = None,
        region: str = "eu-central-1",
        credentials: Optional[dict] = None,
    ):
        if not storage_type or not isinstance(storage_type, str):
            raise ValueError("The 'storage_type' argument must be a non-empty string.")

        storage_type = storage_type.lower()

        if storage_type == "s3":
            if not s3_bucket:
                raise ValueError(
                    "The 's3_bucket' must be provided when 'storage_type' is set to 's3'."
                )
            self.scalar_array_uri = f"s3://{s3_bucket}/array_uri"
            self.ctx = self._initialize_s3_context(credentials, url, region)

        elif storage_type == "local":
            if not local_path:
                raise ValueError(
                    "The 'local_path' must be provided when 'storage_type' is set to 'local'."
                )
            self.scalar_array_uri = os.path.join(local_path, "array_uri")
            self.ctx = self._initialize_local_context()

        else:
            raise ValueError(
                f"Invalid 'storage_type': {storage_type}. Must be 'local' or 's3'."
            )

        # Cache schema information to avoid repeated array opens
        self._schema_cache = None
        self._metadata_cache = None
        self._array_handle = None  # For reusing array connections

    def _initialize_s3_context(
        self, credentials: Optional[dict], url: str, region: str
    ) -> tiledb.Ctx:
        config = {
            "vfs.s3.endpoint_override": url,
            "vfs.s3.region": region,
            # Memory optimizations
            "py.init_buffer_bytes": str(16 * 1024**3),  # 16GB
            "sm.tile_cache_size": str(16 * 1024**3),  # 16GB
            # Thread optimizations
            "sm.num_reader_threads": "64",
            "sm.num_tiledb_threads": "64",
            # S3 optimizations
            "vfs.s3.max_parallel_ops": "32",
            "vfs.s3.multipart_part_size": str(50 * 1024**2),  # 50MB parts
            "vfs.s3.connect_timeout_ms": "10800000",  # 3 hours
            "vfs.s3.request_timeout_ms": "10800000",  # 3 hours
            "vfs.s3.use_virtual_addressing": "true",
            "vfs.s3.scheme": "https",
            # Query optimization
            "sm.enable_signal_handlers": "false",
            "sm.compute_concurrency_level": "64",
            "sm.io_concurrency_level": "64",
        }

        if credentials:
            config.update(
                {
                    "vfs.s3.aws_access_key_id": credentials.get("AccessKeyId", ""),
                    "vfs.s3.aws_secret_access_key": credentials.get(
                        "SecretAccessKey", ""
                    ),
                    "vfs.s3.aws_session_token": credentials.get("SessionToken", ""),
                    "vfs.s3.no_sign_request": "false",
                }
            )
        else:
            config["vfs.s3.no_sign_request"] = "true"

        return tiledb.Ctx(config)

    def _initialize_local_context(self) -> tiledb.Ctx:
        return tiledb.Ctx(
            {
                "py.init_buffer_bytes": str(4 * 1024**3),  # 4GB
                "sm.tile_cache_size": str(4 * 1024**3),  # 4GB
                "sm.num_reader_threads": "32",
                "sm.num_tiledb_threads": "32",
                "sm.compute_concurrency_level": "32",
                "sm.io_concurrency_level": "32",
            }
        )

    @lru_cache(maxsize=1)
    def get_available_variables(self) -> pd.DataFrame:
        """
        Retrieve metadata for available variables in the scalar TileDB array.
        Results are cached to avoid repeated array opens.
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        try:
            with tiledb.open(
                self.scalar_array_uri, mode="r", ctx=self.ctx
            ) as scalar_array:
                metadata = {
                    k: scalar_array.meta[k]
                    for k in scalar_array.meta
                    if not k.startswith("granule_") and "array_type" not in k
                }

                from collections import defaultdict

                organized_metadata = defaultdict(dict)
                for key, value in metadata.items():
                    if "." in key:
                        var_name, attr_type = key.split(".", 1)
                        organized_metadata[var_name][attr_type] = value

                result = pd.DataFrame.from_dict(
                    dict(organized_metadata), orient="index"
                )
                self._metadata_cache = result
                return result

        except Exception as e:
            logger.error(f"Failed to retrieve variables from TileDB: {e}")
            raise

    def _build_profile_attrs(
        self, variables: List[str], array_meta
    ) -> Tuple[List[str], Dict[str, List[str]]]:
        """
        Build attribute list and profile variable mapping efficiently.
        Uses cached metadata to avoid repeated lookups.
        """
        attr_list = []
        profile_vars = {}

        for var in variables:
            profile_key = f"{var}.profile_length"
            if profile_key in array_meta:
                profile_length = array_meta[profile_key]
                # Pre-allocate list with list comprehension (faster than append)
                profile_attrs = [f"{var}_{i}" for i in range(1, profile_length + 1)]
                attr_list.extend(profile_attrs)
                profile_vars[var] = profile_attrs
            else:
                attr_list.append(var)

        return attr_list, profile_vars

    def _build_condition_string(self, filters: Dict[str, str]) -> Optional[str]:
        """
        Build optimized TileDB query condition string from filter dictionary.
        Handles compound conditions and validates operators.
        """
        if not filters:
            return None

        # Pre-compile valid operators for faster lookup
        valid_ops = {">=", "<=", "==", "!=", ">", "<", "="}
        cond_list = []

        for key, condition in filters.items():
            condition = condition.strip()

            # Handle compound conditions (AND/OR)
            if " and " in condition.lower():
                parts = condition.lower().split(" and ")
                for part in parts:
                    part = part.strip()
                    # Find operator and build condition
                    for op in sorted(
                        valid_ops, key=len, reverse=True
                    ):  # Check longest first
                        if op in part:
                            value = part.split(op, 1)[1].strip()
                            cond_list.append(f"{key} {op} {value}")
                            break
            else:
                # Simple condition
                found_op = False
                for op in sorted(valid_ops, key=len, reverse=True):
                    if op in condition:
                        cond_list.append(f"{key} {condition}")
                        found_op = True
                        break

                if not found_op:
                    logger.warning(
                        f"No valid operator found in filter: {key} {condition}"
                    )

        return " and ".join(cond_list) if cond_list else None

    def _filter_by_polygon(
        self,
        data: Dict[str, np.ndarray],
        geometry: gpd.GeoDataFrame,
    ) -> Dict[str, np.ndarray]:
        """
        Filter query results by irregular polygon using vectorized operations.
        Much faster than point-by-point checking.

        Parameters
        ----------
        data : Dict[str, np.ndarray]
            Query results from TileDB
        geometry : gpd.GeoDataFrame
            Polygon(s) to filter by

        Returns
        -------
        Dict[str, np.ndarray]
            Filtered data containing only points within the polygon
        """
        if data is None or len(data.get("shot_number", [])) == 0:
            return data

        # Get lon/lat from data
        lons = data["longitude"]
        lats = data["latitude"]

        # Get the geometry (handle MultiPolygon or single Polygon)
        geom = geometry.unary_union if len(geometry) > 1 else geometry.geometry.iloc[0]

        # Vectorized point-in-polygon test (MUCH faster than iterating)
        mask = contains(geom, lons, lats)

        # Apply mask to all arrays
        filtered_data = {key: value[mask] for key, value in data.items()}

        logger.info(
            f"Polygon filter: {mask.sum()}/{len(mask)} points retained "
            f"({100 * mask.sum() / len(mask):.1f}%)"
        )

        return filtered_data

    def _query_array(
        self,
        variables: List[str],
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_time: Optional[np.datetime64],
        end_time: Optional[np.datetime64],
        geometry: Optional[gpd.GeoDataFrame] = None,
        return_coords: bool = True,
        use_polygon_filter: bool = False,
        **filters: Dict[str, str],
    ) -> Tuple[Optional[Dict[str, np.ndarray]], Dict[str, List[str]]]:
        """
        Execute an optimized query on a TileDB array with spatial, temporal, and polygon filters.

        Parameters
        ----------
        variables : List[str]
            Variables to query
        lat_min, lat_max, lon_min, lon_max : float
            Bounding box coordinates
        start_time, end_time : Optional[np.datetime64]
            Time range
        geometry : Optional[gpd.GeoDataFrame]
            If provided and use_polygon_filter=True, results will be filtered to this polygon
        return_coords : bool
            Whether to return coordinate arrays
        use_polygon_filter : bool
            If True, apply post-query polygon filtering (slower but handles irregular shapes)
        **filters : Dict[str, str]
            Additional attribute filters

        Returns
        -------
        Tuple[Optional[Dict[str, np.ndarray]], Dict[str, List[str]]]
            Query results and profile variable mapping
        """
        try:
            with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as array:
                # Build attribute list and profile variables (cached metadata)
                attr_list, profile_vars = self._build_profile_attrs(
                    variables, array.meta
                )

                # Build condition string
                cond_string = self._build_condition_string(filters)

                # Execute query with optimized settings
                query = array.query(
                    attrs=attr_list,
                    cond=cond_string,
                    coords=return_coords,
                    return_incomplete=False,
                )

                # Use multi_index for efficient spatial-temporal slicing
                data = query.multi_index[
                    lat_min:lat_max, lon_min:lon_max, start_time:end_time
                ]

                # Early return if no data
                if not data or len(data.get("shot_number", [])) == 0:
                    return None, profile_vars

                # Apply polygon filter if requested
                if use_polygon_filter and geometry is not None:
                    data = self._filter_by_polygon(data, geometry)

                    # Check again after polygon filter
                    if len(data.get("shot_number", [])) == 0:
                        return None, profile_vars

                return data, profile_vars

        except Exception as e:
            logger.error(f"Error querying TileDB array '{self.scalar_array_uri}': {e}")
            raise

    def _get_tiledb_spatial_domain(self) -> Tuple[float, float, float, float]:
        """
        Retrieve the spatial domain (bounding box) from the TileDB array schema.
        Results are cached to avoid repeated array opens.

        Returns
        -------
        Tuple[float, float, float, float]
            (min_longitude, max_longitude, min_latitude, max_latitude)
        """
        if self._schema_cache is not None:
            return self._schema_cache

        with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as array:
            domain = array.schema.domain
            min_lon, max_lon = domain.dim(1).domain
            min_lat, max_lat = domain.dim(0).domain

        result = (min_lon, max_lon, min_lat, max_lat)
        self._schema_cache = result
        return result

    def query_dataframe(
        self,
        variables: List[str],
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_time: Optional[np.datetime64] = None,
        end_time: Optional[np.datetime64] = None,
        geometry: Optional[gpd.GeoDataFrame] = None,
        use_polygon_filter: bool = False,
        **filters: Dict[str, str],
    ) -> Optional[pd.DataFrame]:
        """
        Query TileDB and return results as a pandas DataFrame.

        Parameters
        ----------
        variables : List[str]
            Variables to retrieve
        lat_min, lat_max, lon_min, lon_max : float
            Bounding box
        start_time, end_time : Optional[np.datetime64]
            Time range
        geometry : Optional[gpd.GeoDataFrame]
            Polygon for filtering (if use_polygon_filter=True)
        use_polygon_filter : bool
            Whether to apply polygon filtering after bbox query
        **filters : Dict[str, str]
            Additional filters

        Returns
        -------
        Optional[pd.DataFrame]
            Query results or None if no data found
        """
        data, profile_vars = self._query_array(
            variables,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_time,
            end_time,
            geometry=geometry,
            use_polygon_filter=use_polygon_filter,
            **filters,
        )

        if data is None:
            return None

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Reconstruct profile variables if needed
        for var_name, profile_cols in profile_vars.items():
            if all(col in df.columns for col in profile_cols):
                df[var_name] = df[profile_cols].values.tolist()
                df = df.drop(columns=profile_cols)

        return df

    def close(self):
        """Clean up resources."""
        self._schema_cache = None
        self._metadata_cache = None
        if self._array_handle is not None:
            self._array_handle = None
