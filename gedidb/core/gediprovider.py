# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import logging
import re
from typing import Dict, List, Literal, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from scipy.spatial import cKDTree

from gedidb.providers.tiledb_provider import TileDBProvider
from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
    _timestamp_to_datetime,
    validate_query_geometry,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]

# Compiled once — used in _attach_metadata for every variable in every to_xarray call
_PERCENTILE_RE = re.compile(r"^(.+?)_p(\d+)$")

# Dimensions that are stored as coords in the Xarray Dataset, not as data variables
_COORD_DIMS: frozenset = frozenset(["latitude", "longitude", "time", "shot_number"])


class GEDIProvider(TileDBProvider):
    """
    GEDIProvider class to interface with GEDI data stored in TileDB, with support for flexible storage types.

    Attributes
    ----------
    scalar_array_uri : str
        URI for accessing the scalar data array.
    ctx : tiledb.Ctx
        TileDB context for the configured storage type (S3 or local).

    Methods
    -------
    get_available_variables() -> pd.DataFrame
        Retrieve a list of available variables with descriptions and units.
    query_nearest_shots(...) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
        Query data for the nearest shots to a specified point.
    query_data(...) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
        Query data within specified spatial and temporal bounds.
    get_data(...) -> Union[pd.DataFrame, xr.Dataset, None]
        Retrieve queried data in either Pandas DataFrame or Xarray Dataset format.
    """

    def __init__(
        self,
        storage_type: Optional[str] = "local",
        s3_bucket: Optional[str] = None,
        local_path: Optional[str] = "./",
        url: Optional[str] = None,
        region: Optional[str] = "eu-central-1",
        credentials: Optional[dict] = None,
        config_overrides: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize GEDIProvider with URIs for scalar and profile data arrays, configured based on storage type.

        Parameters
        ----------
        storage_type : str, optional
            Storage type, either 's3' or 'local'. Defaults to 'local'.
        s3_bucket : str, optional
            The S3 bucket name for GEDI data storage. Required if `storage_type` is 's3'.
        local_path : str, optional
            The local path for storing GEDI data arrays. Used if `storage_type` is 'local'.
        url : str, optional
            Custom endpoint URL for S3-compatible object stores (e.g., MinIO).
        region : str, optional
            AWS region for S3 access. Defaults to 'eu-central-1'.

        Notes
        -----
        Supports both S3 and local storage configurations based on `storage_type`.
        """
        super().__init__(
            storage_type,
            s3_bucket,
            local_path,
            url,
            region,
            credentials,
            config_overrides=config_overrides,
        )

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
        Retrieve data for the nearest GEDI shots around a specified reference point, within a given radius.

        This function queries GEDI data for the closest shot locations to a specified longitude and latitude,
        limiting the search to a bounding box defined by a radius around the point. It filters both scalar
        and profile variables by proximity, time range, and additional quality parameters if provided.

        Parameters
        ----------
        variables : List[str]
            List of variable names to retrieve from the GEDI data.
        point : Tuple[float, float]
            Longitude and latitude coordinates representing the reference point for the nearest-shot search.
        num_shots : int
            The maximum number of nearest shots to retrieve.
        radius : float, optional
            Radius around the reference point (in degrees) within which to limit the search.
        start_time : np.datetime64, optional
            Start time for filtering data within a specific temporal range.
        end_time : np.datetime64, optional
            End time for filtering data within a specific temporal range.
        **quality_filters : dict
            Additional keyword arguments for quality filtering, applied to both scalar and profile data.

        Returns
        -------
        Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
            Two dictionaries containing the nearest GEDI shots:
            - The first dictionary holds scalar data variables, with variable names as keys and arrays of values as items.
            - The second dictionary holds profile data variables, similarly structured.

        Notes
        -----
        - This function first creates a bounding box around the point with a defined radius to reduce the search area.
        - After querying this subset, it uses a KD-tree to efficiently find the nearest shots within the subset.
        - Only points within the bounding box and meeting any specified time and quality criteria are considered.
        - The number of shots returned may be fewer than `num_shots` if fewer points meet the criteria.

        """
        scalar_vars = variables + DEFAULT_DIMS

        start_time = _datetime_to_timestamp_days(start_time) if start_time else None
        end_time = _datetime_to_timestamp_days(end_time) if end_time else None

        if (
            not np.isfinite(point).all()
            or not -180 <= point[0] <= 180
            or not -90 <= point[1] <= 90
        ):
            raise ValueError("point must contain finite WGS84 longitude and latitude.")
        if (
            not isinstance(num_shots, (int, np.integer))
            or num_shots < 1
            or not np.isfinite(radius)
            or radius <= 0
        ):
            raise ValueError(
                "num_shots must be a positive integer and radius must be positive and finite."
            )
        min_lon, max_lon, min_lat, max_lat = self._get_tiledb_spatial_domain()
        lat_min, lat_max = max(point[1] - radius, min_lat), min(
            point[1] + radius, max_lat
        )
        if lat_min > lat_max:
            return {}, {}, {}
        west, east = point[0] - radius, point[0] + radius
        if radius >= 180:
            longitude_ranges = [(-180, 180)]
        elif west < -180:
            longitude_ranges = [(west + 360, 180), (-180, east)]
        elif east > 180:
            longitude_ranges = [(west, 180), (-180, east - 360)]
        else:
            longitude_ranges = [(west, east)]
        subsets = []
        profile_vars, scalar_renames = {}, {}
        for west, east in longitude_ranges:
            west, east = max(west, min_lon), min(east, max_lon)
            if west > east:
                continue
            data, profile_vars, scalar_renames = self._query_array(
                scalar_vars,
                lat_min,
                lat_max,
                west,
                east,
                start_time,
                end_time,
                **quality_filters,
            )
            if data:
                subsets.append(data)
        scalar_data_subset = (
            {key: np.concatenate([part[key] for part in subsets]) for key in subsets[0]}
            if subsets
            else None
        )

        if not scalar_data_subset:
            logger.info("No points found in the bounding box.")
            return {}, {}, {}

        longitudes, latitudes = (
            scalar_data_subset["longitude"],
            scalar_data_subset["latitude"],
        )
        if not longitudes.size:
            logger.warning(
                "No points found within the bounding box for nearest shot query."
            )
            return {}, {}, {}

        # Efficient KD-tree search
        # Chord distance on the unit sphere preserves great-circle ordering.
        def unit_vectors(lon, lat):
            lon, lat = np.deg2rad(lon), np.deg2rad(lat)
            return np.column_stack(
                (np.cos(lat) * np.cos(lon), np.cos(lat) * np.sin(lon), np.sin(lat))
            )

        tree = cKDTree(unit_vectors(longitudes, latitudes))
        _, indices = tree.query(
            unit_vectors([point[0]], [point[1]])[0], k=min(num_shots, len(longitudes))
        )

        # Normalize to 1D array of indices
        indices = np.atleast_1d(indices)

        scalar_data = {k: v[indices] for k, v in scalar_data_subset.items()}

        return scalar_data, profile_vars, scalar_renames

    def query_data(
        self,
        variables: List[str],
        geometry: Optional[gpd.GeoDataFrame] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        use_polygon_filter: Union[bool, Literal["auto"]] = "auto",
        **quality_filters,
    ) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]:
        """
        Query GEDI data from TileDB arrays within a specified spatial bounding box and time range,
        applying optional quality filters with flexible filter expressions.

        Parameters
        ----------
        variables : List[str]
            List of variable names to retrieve from the GEDI data.
        geometry : geopandas.GeoDataFrame, optional
            A spatial geometry defining the bounding box for data filtering.
            If provided, the bounding box is extracted from the geometry's total bounds.
            If `None`, an error is raised to prevent querying the entire dataset.
        start_time : str, optional
            Start time for filtering data within a specific temporal range. Expected format is ISO 8601 (e.g., '2020-01-01').
        end_time : str, optional
            End time for filtering data within a specific temporal range. Expected format is ISO 8601.
        **quality_filters : dict
            Additional keyword arguments for quality filtering, applied to both scalar and profile data.

        Returns
        -------
        Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
            Two dictionaries containing GEDI data filtered within the specified bounds:
            - The first dictionary holds scalar data variables, with variable names as keys and arrays of values as items.
            - The second dictionary holds profile data variables, similarly structured.

        Notes
        -----
        - Quality filters are applied as boolean masks to the queried data arrays, supporting compound expressions.
        """
        if geometry is None:
            raise ValueError(
                "A valid geometry (GeoDataFrame) must be provided to limit the query. "
                "Querying the entire dataset without a spatial filter is not allowed."
            )

        geometry = validate_query_geometry(geometry)
        lon_min, lat_min, lon_max, lat_max = geometry.total_bounds

        # Convert timestamps efficiently
        start_time = _datetime_to_timestamp_days(start_time) if start_time else None
        end_time = _datetime_to_timestamp_days(end_time) if end_time else None

        if use_polygon_filter not in (True, False, "auto"):
            raise ValueError("use_polygon_filter must be True, False or 'auto'.")
        geom_union = geometry.union_all()
        if use_polygon_filter == "auto":
            # Only an exact rectangle can safely skip point-in-polygon testing.
            use_polygon_filter = not geom_union.equals(geom_union.envelope)

        # Pass the pre-computed union as geometry so _filter_by_polygon reuses it
        # directly (no second union_all call). Falls back to the GeoDataFrame when
        # no pre-computation was needed (use_polygon_filter=False or explicit True).
        scalar_vars = variables + DEFAULT_DIMS
        scalar_data, profile_vars, scalar_renames = self._query_array(
            scalar_vars,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_time,
            end_time,
            geometry=(
                geom_union
                if use_polygon_filter and geom_union is not None
                else geometry
            ),
            use_polygon_filter=use_polygon_filter,
            **quality_filters,
        )

        return scalar_data, profile_vars, scalar_renames

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
        use_polygon_filter: Union[bool, Literal["auto"]] = "auto",
        **quality_filters,
    ) -> Union[pd.DataFrame, xr.Dataset, None]:
        """
        Retrieve GEDI data based on spatial, temporal, and quality filters,
        and return it in either Pandas Dataframe or Xarray format.

        This function allows flexible querying of GEDI data, either by bounding box or
        nearest-point selection, with optional filtering based on time and quality criteria.
        Data can be returned as a Pandas DataFrame or Xarray Dataset.

        Parameters
        ----------
        variables : List[str]
            A list of variable names to retrieve from the GEDI data.
        geometry : geopandas.GeoDataFrame, optional
            Spatial filter defined as a GeoDataFrame. Used when `query_type` is 'bounding_box'.
        start_time : str, optional
            Start of the time range for filtering data. Should be in a format compatible
            with `np.datetime64`.
        end_time : str, optional
            End of the time range for filtering data. Should be in a format compatible
            with `np.datetime64`.
        return_type : str, default "xarray"
            Format in which to return the data. Options are 'dataframe' or 'xarray'.
        query_type : str, default "bounding_box"
            Type of query to perform. Options are:
            - "bounding_box": Retrieve data within the specified geometry or bounding box.
            - "nearest": Retrieve the nearest GEDI shots to a specified point.
        point : Tuple[float, float], optional
            A tuple (longitude, latitude) representing the reference point for a nearest-shot query.
            Required if `query_type` is "nearest".
        num_shots : int, default 10
            Number of nearest shots to retrieve if `query_type` is "nearest".
        radius : float, default 0.1
            Radius (in degrees) around the point to limit the spatial subset for nearest queries.
        **quality_filters : dict
            Additional filters for data quality or attribute-based filtering.

        Returns
        -------
        Union[pd.DataFrame, xr.Dataset, None]
            - Pandas DataFrame or Xarray Dataset containing the queried data, based on `return_type`.
            - None if no data is found matching the specified criteria.

        Notes
        -----
        - The `return_type` parameter controls whether data is returned as a DataFrame or Dataset.
        - The `query_type` parameter determines the querying mode (bounding box or nearest shot).
        - Ensure the TileDB context (`self.ctx`) and array URIs are correctly configured before calling this function.
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
            scalar_data, profile_vars, scalar_renames = self.query_nearest_shots(
                variables,
                point,
                num_shots,
                radius,
                start_time,
                end_time,
                **quality_filters,
            )
        elif query_type == "bounding_box":
            scalar_data, profile_vars, scalar_renames = self.query_data(
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

        # Apply single-label renames (e.g. rh_99 → rh_p98)
        scalar_data = self._apply_scalar_renames(
            scalar_data, profile_vars, scalar_renames
        )

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
        Convert scalar and profile data dictionaries into a unified pandas DataFrame.

        This function takes scalar data (single-point measurements) and profile data
        (multi-point measurements per shot) and combines them into a single DataFrame.
        Profile data is aggregated for each `shot_number`, with profile values stored as lists.

        Parameters
        ----------
        scalar_data : Dict[str, np.ndarray]
            Dictionary containing scalar data variables, where each key is a variable name
            and each value is a numpy array of measurements indexed by `shot_number`.
        profile_data : Dict[str, np.ndarray]
            Dictionary containing profile data variables, where each key is a variable name
            and each value is a numpy array of measurements. Profile data includes multiple
            measurements per `shot_number` and `profile_point`.

        Returns
        -------
        pd.DataFrame
            A pandas DataFrame where scalar data is stored as individual columns, and
            profile data is grouped by `shot_number` with each variable represented as a
            list of values per `shot_number`.

        Notes
        -----
        - Profile data columns (e.g., `latitude`, `longitude`, `time`, `profile_point`)
          are dropped after aggregation to avoid duplication.
        - The returned DataFrame merges scalar and profile data on `shot_number`, ensuring
          alignment between the two datasets.

        """
        # Create DataFrame (optimized with from_dict)
        scalar_data = {
            **scalar_data,
            "time": _timestamp_to_datetime(scalar_data["time"]),
        }
        scalar_df = pd.DataFrame.from_dict(scalar_data)

        # Reconstruct profile variables if present
        if profile_vars:
            for var_name, profile_cols in profile_vars.items():
                if all(col in scalar_df.columns for col in profile_cols):
                    # Vectorized list creation
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
        Convert scalar and profile data to an Xarray Dataset, with metadata attached.

        This function creates an Xarray Dataset by transforming scalar and profile data dictionaries
        into separate DataArrays, then merging them based on the `shot_number` dimension.
        Metadata is added to each variable in the final Dataset for descriptive context.

        Parameters
        ----------
        scalar_data : Dict[str, np.ndarray]
            Dictionary containing scalar data variables. Keys are variable names, and values
            are numpy arrays indexed by `shot_number`.
        metadata : pd.DataFrame
            DataFrame containing variable metadata (e.g., descriptions and units). The index
            should match the variable names in `scalar_data` and `profile_vars`.
        profile_vars : Dict[str, List[str]]
            Dictionary where keys are base names of profile variables (e.g., 'rh') and values
            are lists of associated variable names (e.g., ['rh_1', 'rh_2', ...]).

        Returns
        -------
        xr.Dataset
            An Xarray Dataset containing scalar and profile data with attached metadata, indexed
            by `shot_number` for scalar data and both `shot_number` and `profile_point` for profile data.

        Notes
        -----
        - Scalar data variables are included in the Dataset with the dimension `shot_number`.
        - Profile data variables are reshaped to include the `profile_point` dimension alongside `shot_number`.
        - The Dataset is annotated with metadata (descriptions, units, etc.) from the provided metadata DataFrame.
        """

        time_coord = _timestamp_to_datetime(scalar_data["time"])

        # Build exclusion set once: coord dims + all expanded profile component names
        exclude = _COORD_DIMS | frozenset(
            item for sublist in profile_vars.values() for item in sublist
        )

        # Identify scalar variables
        scalar_vars = [var for var in scalar_data if var not in exclude]

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
            profile_data = np.stack(
                [scalar_data[comp] for comp in components],
                axis=-1,
            )
            info = (
                metadata.loc[base_var].to_dict() if base_var in metadata.index else {}
            )
            raw_labels = info.get("profile_labels")
            if isinstance(raw_labels, str):
                labels = np.asarray([float(label) for label in raw_labels.split(",")])
                if len(labels) != num_profile_points or len(np.unique(labels)) != len(
                    labels
                ):
                    raise ValueError(f"Invalid profile labels for {base_var}.")
            else:
                labels = np.arange(num_profile_points)
            label_name = info.get("profile_label_name")
            if not isinstance(label_name, str) or not label_name:
                label_name = "profile_point"
            # A variable-specific dimension prevents unrelated grids from aligning.
            profile_dim = f"{base_var}_{label_name}"

            # Add to data_vars dict
            data_vars[base_var] = xr.DataArray(
                profile_data,
                coords={
                    "shot_number": scalar_data["shot_number"],
                    profile_dim: labels,
                },
                dims=["shot_number", profile_dim],
            )

        # Create dataset once with all variables (no merge needed)
        dataset = xr.Dataset(
            data_vars=data_vars,
            coords={
                "shot_number": scalar_data["shot_number"],
                "latitude": ("shot_number", scalar_data["latitude"]),
                "longitude": ("shot_number", scalar_data["longitude"]),
                "time": ("shot_number", time_coord),
            },
        )

        # Attach metadata
        self._attach_metadata(dataset, metadata)

        return dataset

    def _attach_metadata(self, dataset: xr.Dataset, metadata: pd.DataFrame) -> None:
        """
        Attach metadata to each variable in an Xarray Dataset.

        This function iterates through the variables in the given Xarray Dataset, matching each variable
        to corresponding metadata entries from a provided DataFrame. Metadata attributes such as
        `description`, `units`, and `product level` are added to each variable in the Dataset for enhanced context.

        Parameters
        ----------
        dataset : xr.Dataset
            The Xarray Dataset to which metadata will be attached. The Dataset's variable names should match
            the index of the metadata DataFrame for accurate alignment.
        metadata : pd.DataFrame
            A DataFrame containing metadata attributes (e.g., descriptions, units, product levels) for each variable.
            The DataFrame's index should correspond to variable names, with columns for each metadata attribute.
            Expected columns include:
                - 'description': A brief description of the variable.
                - 'units': The unit of measurement for the variable.
                - 'product_level': The data processing level or classification.

        Returns
        -------
        None
            This function modifies the Dataset in-place, adding metadata attributes to each variable as applicable.

        Notes
        -----
        - Only variables present in both the Dataset and metadata index will have metadata added.
        - Missing attributes in the metadata DataFrame are handled gracefully, defaulting to an empty string if absent.
        - This function provides context and unit information for each variable, improving the Dataset's interpretability.

        """
        metadata_dict = metadata.to_dict(orient="index")

        for var in dataset.variables:
            var_metadata = metadata_dict.get(var, {})
            match = _PERCENTILE_RE.match(var)
            if match:
                base_var, label = match.groups()
                base_metadata = metadata_dict.get(base_var)
                if base_metadata and isinstance(
                    base_metadata.get("profile_labels"), str
                ):
                    var_metadata = base_metadata.copy()
                    desc = var_metadata.get("description", "")
                    label_name = var_metadata.get("profile_label_name", "profile label")
                    var_metadata["description"] = f"{desc} ({label_name}: {label})"
            dataset[var].attrs.update(var_metadata)
