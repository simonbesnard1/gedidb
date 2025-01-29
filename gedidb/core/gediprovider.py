# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from scipy.spatial import cKDTree
from typing import Optional, List, Union, Dict, Tuple
from collections import defaultdict

from gedidb.utils.geo_processing import (
    check_and_format_shape,
    _datetime_to_timestamp_days,
    _timestamp_to_datetime,
)
from gedidb.providers.tiledb_provider import TileDBProvider

# Configure the logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]


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
        storage_type: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        local_path: Optional[str] = None,
        url: Optional[str] = None,
        region: Optional[str] = "eu-central-1",
        credentials: Optional[dict] = None,
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
        super().__init__(storage_type, s3_bucket, local_path, url, region, credentials)

    def query_nearest_shots(
        self,
        variables: List[str],
        point: Tuple[float, float],
        num_shots: int,
        radius: float = 0.1,
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
        radius : float, optional, default=0.1
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
        start_timestamp = (
            _datetime_to_timestamp_days(start_time) if start_time else None
        )
        end_timestamp = _datetime_to_timestamp_days(end_time) if end_time else None

        lon_min, lat_min = point[0] - radius, point[1] - radius
        lon_max, lat_max = point[0] + radius, point[1] + radius

        scalar_data_subset, profile_vars = self._query_array(
            self.scalar_array_uri,
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

        tree = cKDTree(np.column_stack((longitudes, latitudes)))
        distances, indices = tree.query(point, k=min(num_shots, len(longitudes)))
        nearest_shots = scalar_data_subset["shot_number"][indices]

        scalar_data = {
            k: np.array(v)[np.isin(scalar_data_subset["shot_number"], nearest_shots)]
            for k, v in scalar_data_subset.items()
        }

        return scalar_data, profile_vars

    def query_data(
        self,
        variables: List[str],
        geometry: Optional[gpd.GeoDataFrame] = None,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
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
            If `None`, the entire global range is used.
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

        if geometry is not None:
            geometry = check_and_format_shape(geometry, simplify=True)
            lon_min, lat_min, lon_max, lat_max = geometry.total_bounds
        else:
            lat_min, lat_max = -56.0, 56.0
            lon_min, lon_max = -180.0, 180.0

        if start_time:
            start_time = np.datetime64(start_time)
            start_timestamp = _datetime_to_timestamp_days(start_time)
        else:
            start_timestamp = None

        if end_time:
            end_time = np.datetime64(end_time)
            end_timestamp = _datetime_to_timestamp_days(end_time)
        else:
            end_timestamp = None

        # Determine variables, including dimension variables
        scalar_vars = variables + DEFAULT_DIMS

        # Query tileDB array within the specified bounds and time range
        scalar_data = self._query_array(
            self.scalar_array_uri,
            scalar_vars,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_timestamp,
            end_timestamp,
            **quality_filters,
        )

        return scalar_data

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
        **quality_filters,
    ) -> Union[pd.DataFrame, xr.Dataset, None]:
        """
        Retrieve GEDI data based on spatial, temporal, and quality filters,
        and return it in either Pandas or Xarray format.

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
            Format in which to return the data. Options are 'pandas' or 'xarray'.
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

        if query_type == "nearest" and point:
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
                variables, geometry, start_time, end_time, **quality_filters
            )
        else:
            raise ValueError(
                "Invali query_type. Must be either 'nearest' or 'bounding_box'."
            )

        if not scalar_data:
            logger.info("No data found for specified criteria.")
            return None

        metadata = self.get_available_variables()

        if return_type == "xarray":
            return self.to_xarray(scalar_data, metadata, profile_vars)
        elif return_type == "dataframe":
            return self.to_dataframe(scalar_data)
        else:
            raise ValueError(
                "Invalid return_type. Must be either 'xarray' or 'dataframe'."
            )

    def to_dataframe(self, scalar_data: Dict[str, np.ndarray]) -> pd.DataFrame:
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
        # Convert scalar data to DataFrame
        scalar_data["time"] = _timestamp_to_datetime(scalar_data["time"])
        scalar_df = pd.DataFrame.from_dict(scalar_data)

        # Merge scalar and profile data on shot_number
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

        profile_var_components = [
            item for sublist in profile_vars.values() for item in sublist
        ]
        scalar_vars = [
            var
            for var in scalar_data
            if var
            not in ["latitude", "longitude", "time", "shot_number"] + profile_var_components
        ]

        times = _timestamp_to_datetime(scalar_data["time"])

        scalar_ds = xr.Dataset(
            {
                var: xr.DataArray(
                    scalar_data[var],
                    coords={"shot_number": scalar_data["shot_number"]},
                    dims=["shot_number"],
                )
                for var in scalar_vars
            }
        )

        profile_ds = xr.Dataset()

        for base_var, components in profile_vars.items():
            # Number of profile points (columns)
            num_profile_points = len(components)

            # Preallocate an array with shape (num_shots, num_profile_points)
            profile_data = np.empty(
                (len(scalar_data["shot_number"]), num_profile_points), dtype=np.float32
            )

            # Fill the preallocated array directly
            for idx, comp in enumerate(components):
                profile_data[:, idx] = scalar_data[comp]

            # Add the preallocated array to Xarray Dataset
            profile_ds[base_var] = xr.DataArray(
                profile_data,
                coords={
                    "shot_number": scalar_data["shot_number"],
                    "profile_points": range(num_profile_points),
                },
                dims=["shot_number", "profile_points"],
            )
        dataset = xr.merge([scalar_ds, profile_ds])
        dataset = dataset.assign_coords(
            {
                "latitude": ("shot_number", scalar_data["latitude"]),
                "longitude": ("shot_number", scalar_data["longitude"]),
                "time": ("shot_number", times),
            }
        )

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
        default_metadata = defaultdict(
            lambda: {"description": "", "units": "", "product_level": ""}
        )

        for var in dataset.variables:
            var_metadata = metadata_dict.get(var, default_metadata)
            dataset[var].attrs.update(var_metadata)
