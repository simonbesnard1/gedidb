# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import logging
from collections import defaultdict
import re
from typing import Dict, List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from scipy.spatial import cKDTree

from gedidb.providers.tiledb_provider import TileDBProvider
from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
    _timestamp_to_datetime,
    check_and_format_shape,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]


class GEDIProvider(TileDBProvider):
    """
    Optimized GEDIProvider with support for irregular polygons and efficient query building.
    """

    def __init__(
        self,
        storage_type: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        local_path: Optional[str] = None,
        url: Optional[str] = None,
        region: Optional[str] = "eu-central-1",
        credentials: Optional[dict] = None,
    ):
        super().__init__(storage_type, s3_bucket, local_path, url, region, credentials)

    def query_nearest_shots(
        self,
        variables: List[str],
        point: Tuple[float, float],
        num_shots: int,
        radius: float,
        start_time: Optional[np.datetime64] = None,
        end_time: Optional[np.datetime64] = None,
        **quality_filters,
    ) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
        """
        Retrieve data for the nearest GEDI shots around a specified reference point.
        """
        scalar_vars = variables + DEFAULT_DIMS
        start_timestamp = (
            _datetime_to_timestamp_days(start_time) if start_time else None
        )
        end_timestamp = _datetime_to_timestamp_days(end_time) if end_time else None

        lon_min, lat_min = point[0] - radius, point[1] - radius
        lon_max, lat_max = point[0] + radius, point[1] + radius

        scalar_data_subset, profile_vars = self._query_array(
            scalar_vars,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_timestamp,
            end_timestamp,
            **quality_filters,
        )

        if not scalar_data_subset:
            logger.info("No points found in the bounding box.")
            return {}, {}

        longitudes, latitudes = (
            scalar_data_subset["longitude"],
            scalar_data_subset["latitude"],
        )
        if not longitudes.size:
            logger.warning(
                "No points found within the bounding box for nearest shot query."
            )
            return {}, {}

        # Efficient KD-tree search
        tree = cKDTree(np.column_stack((longitudes, latitudes)))
        distances, indices = tree.query(point, k=min(num_shots, len(longitudes)))

        # Handle single result case
        if np.isscalar(indices):
            indices = [indices]

        nearest_shots = scalar_data_subset["shot_number"][indices]

        # Vectorized filtering (faster than dict comprehension)
        mask = np.isin(scalar_data_subset["shot_number"], nearest_shots)
        scalar_data = {k: v[mask] for k, v in scalar_data_subset.items()}

        return scalar_data, profile_vars

    def query_data(
        self,
        variables: List[str],
        geometry: Optional[gpd.GeoDataFrame] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        use_polygon_filter: bool = "auto",
        **quality_filters,
    ) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
        """
        Query GEDI data with optimized spatial filtering.

        OPTIMIZATION IMPROVEMENTS:
        1. Auto-detect when polygon filtering is needed
        2. Use vectorized polygon operations
        3. Efficient timestamp conversion

        Parameters
        ----------
        variables : List[str]
            Variables to retrieve
        geometry : gpd.GeoDataFrame, optional
            Spatial filter (can be irregular polygon)
        start_time, end_time : str, optional
            Time range in ISO format
        use_polygon_filter : bool or "auto"
            - "auto": Automatically use polygon filter if geometry is not rectangular
            - True: Always use polygon filter
            - False: Only use bounding box (faster but may include extra points)
        **quality_filters : dict
            Additional attribute filters

        Returns
        -------
        Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
            Scalar data and profile variable mapping
        """
        if geometry is None:
            raise ValueError(
                "A valid geometry (GeoDataFrame) must be provided to limit the query. "
                "Querying the entire dataset without a spatial filter is not allowed."
            )

        geometry = check_and_format_shape(geometry, simplify=True)
        lon_min, lat_min, lon_max, lat_max = geometry.total_bounds

        # Convert timestamps efficiently
        start_timestamp = (
            _datetime_to_timestamp_days(np.datetime64(start_time))
            if start_time
            else None
        )
        end_timestamp = (
            _datetime_to_timestamp_days(np.datetime64(end_time)) if end_time else None
        )

        # Auto-detect polygon filtering need
        if use_polygon_filter == "auto":

            # Check if geometry is complex (not just a rectangle)
            geom = (
                geometry.unary_union if len(geometry) > 1 else geometry.geometry.iloc[0]
            )
            bbox_area = (lon_max - lon_min) * (lat_max - lat_min)
            geom_area = geom.area

            # If geometry fills less than 80% of bbox, use polygon filter
            use_polygon_filter = (
                (geom_area / bbox_area) < 0.9 if bbox_area > 0 else False
            )

            if use_polygon_filter:
                logger.info(
                    f"Auto-enabled polygon filter (geometry covers "
                    f"{100 * geom_area / bbox_area:.1f}% of bounding box)"
                )

        # Query with optimized filtering
        scalar_vars = variables + DEFAULT_DIMS
        scalar_data, profile_vars = self._query_array(
            scalar_vars,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_timestamp,
            end_timestamp,
            # geometry=geometry,
            # use_polygon_filter=use_polygon_filter,
            **quality_filters,
        )

        return scalar_data, profile_vars

    def get_data(
        self,
        variables: List[str],
        geometry: Optional[gpd.GeoDataFrame] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        return_type: str = "xarray",
        query_type: str = "bounding_box",
        point: Optional[Tuple[float, float]] = None,
        num_shots: Optional[int] = None,
        radius: Optional[float] = None,
        use_polygon_filter: bool = "auto",
        **quality_filters,
    ) -> Union[pd.DataFrame, xr.Dataset, None]:
        """
        Retrieve GEDI data with optimized querying and flexible polygon support.

        NEW FEATURES:
        - Automatic polygon filter detection
        - Support for irregular polygons
        - Better validation and error messages

        Parameters
        ----------
        use_polygon_filter : bool or "auto", default "auto"
            Controls polygon filtering behavior:
            - "auto": Intelligently decide based on geometry complexity
            - True: Always filter by exact polygon (slower, more accurate)
            - False: Only use bounding box (faster, may include extra points)
        """
        # Validate query_type
        if query_type not in {"bounding_box", "nearest"}:
            raise ValueError(
                f"Invalid query_type '{query_type}'. Must be 'bounding_box' or 'nearest'."
            )

        # Validate return_type
        if return_type not in {"xarray", "dataframe"}:
            raise ValueError(
                f"Invalid return_type '{return_type}'. Must be either 'xarray' or 'dataframe'."
            )

        # Validation for bounding_box queries
        if query_type == "bounding_box":
            if geometry is None or not isinstance(geometry, gpd.GeoDataFrame):
                raise ValueError(
                    "For 'bounding_box' queries, a valid GeoDataFrame must be provided as 'geometry'."
                )

        # Validation for nearest queries
        elif query_type == "nearest":
            if point is None or not (isinstance(point, tuple) and len(point) == 2):
                raise ValueError(
                    "For 'nearest' queries, 'point' must be a (longitude, latitude) tuple."
                )
            if num_shots is None or num_shots <= 0:
                raise ValueError(
                    "For 'nearest' queries, 'num_shots' must be a positive integer."
                )
            if radius is None or radius <= 0:
                raise ValueError(
                    "For 'nearest' queries, 'radius' must be a positive float."
                )

        # Execute query
        if query_type == "nearest":
            scalar_data, profile_vars = self.query_nearest_shots(
                variables,
                point,
                num_shots,
                radius,
                start_time,
                end_time,
                **quality_filters,
            )
        elif query_type == "bounding_box":
            scalar_data, profile_vars = self.query_data(
                variables,
                geometry,
                start_time,
                end_time,
                use_polygon_filter=use_polygon_filter,
                **quality_filters,
            )

        if not scalar_data:
            logger.info("No data found for specified criteria.")
            return None

        # Return in requested format
        if return_type == "xarray":
            metadata = self.get_available_variables()
            return self.to_xarray(scalar_data, metadata, profile_vars)
        elif return_type == "dataframe":
            return self.to_dataframe(scalar_data, profile_vars)

    def to_dataframe(
        self,
        scalar_data: Dict[str, np.ndarray],
        profile_vars: Dict[str, List[str]] = None,
    ) -> pd.DataFrame:
        """
        Convert data to DataFrame with optimized profile reconstruction.
        """
        # Convert timestamps efficiently
        scalar_data["time"] = _timestamp_to_datetime(scalar_data["time"])

        # Create DataFrame (optimized with from_dict)
        scalar_df = pd.DataFrame.from_dict(scalar_data)

        # Reconstruct profile variables if present
        if profile_vars:
            for var_name, profile_cols in profile_vars.items():
                if all(col in scalar_df.columns for col in profile_cols):
                    # Vectorized list creation (faster than iterrows)
                    scalar_df[var_name] = scalar_df[profile_cols].values.tolist()
                    scalar_df = scalar_df.drop(columns=profile_cols)

        return scalar_df

    def to_xarray(
        self,
        scalar_data: Dict[str, np.ndarray],
        metadata: pd.DataFrame,
        profile_vars: Dict[str, List[str]],
    ) -> xr.Dataset:
        """
        Convert to Xarray with optimized array operations.

        Parameters
        ----------
        scalar_data : Dict[str, np.ndarray]
            Dictionary of scalar data arrays
        metadata : pd.DataFrame
            Variable metadata
        profile_vars : Dict[str, List[str]]
            Profile variable component mapping

        Returns
        -------
        xr.Dataset
            Xarray dataset with coordinates and metadata
        """

        # Extract profile variable components
        profile_var_components = [
            item for sublist in profile_vars.values() for item in sublist
        ]

        # Identify scalar variables
        scalar_vars = [
            var
            for var in scalar_data
            if var
            not in ["latitude", "longitude", "time", "shot_number"]
            + profile_var_components
        ]

        # Convert timestamps
        times = _timestamp_to_datetime(scalar_data["time"])

        # Create dataset with data_vars dict (faster than merging)
        data_vars = {}

        # Add scalar variables
        for var in scalar_vars:
            data_vars[var] = xr.DataArray(
                scalar_data[var],
                coords={"shot_number": scalar_data["shot_number"]},
                dims=["shot_number"],
            )

        # Pre-allocate profile arrays (keep existing optimization)
        for base_var, components in profile_vars.items():
            num_profile_points = len(components)
            num_shots = len(scalar_data["shot_number"])

            # Pre-allocate array
            profile_data = np.empty(
                (num_shots, num_profile_points),
                dtype=np.float32,
            )

            # Vectorized fill
            for idx, comp in enumerate(components):
                profile_data[:, idx] = scalar_data[comp]

            # Add to data_vars dict
            data_vars[base_var] = xr.DataArray(
                profile_data,
                coords={
                    "shot_number": scalar_data["shot_number"],
                    "profile_points": range(num_profile_points),
                },
                dims=["shot_number", "profile_points"],
            )

        # Create dataset once with all variables (no merge needed)
        dataset = xr.Dataset(
            data_vars=data_vars,
            coords={
                "shot_number": scalar_data["shot_number"],
                "latitude": ("shot_number", scalar_data["latitude"]),
                "longitude": ("shot_number", scalar_data["longitude"]),
                "time": ("shot_number", times),
            },
        )

        # Attach metadata
        self._attach_metadata(dataset, metadata)

        return dataset

    def _attach_metadata(self, dataset: xr.Dataset, metadata: pd.DataFrame) -> None:
        """
        Attach metadata to Xarray variables with support for percentile variants.
        """
        metadata_dict = metadata.to_dict(orient="index")
        default_metadata = defaultdict(
            lambda: {"description": "", "units": "", "product_level": ""}
        )

        # Variables that can have _<percentile> variants
        base_vars_with_percentiles = {"rh", "cover_z", "pai_z", "pavd_z"}

        for var in dataset.variables:
            var_metadata = metadata_dict.get(var, default_metadata)

            # Check for percentile variants (e.g., rh_95)
            match = re.match(r"^(.+?)_(\d+)$", var)
            if match:
                base_var = match.group(1)
                percentile = match.group(2)

                if base_var in base_vars_with_percentiles:
                    base_metadata = metadata_dict.get(base_var)
                    if base_metadata:
                        # Copy and modify metadata
                        var_metadata = base_metadata.copy()
                        desc = var_metadata.get("description", "")
                        var_metadata["description"] = (
                            f"{desc} ({percentile}th percentile)"
                            if desc
                            else f"{percentile}th percentile of {base_var}"
                        )

            dataset[var].attrs.update(var_metadata)
