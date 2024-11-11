# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de, besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-LicenseCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import logging
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from scipy.spatial import cKDTree
from typing import Optional, List, Union, Dict, Tuple
import re

from gedidb.utils.geospatial_tools import check_and_format_shape, _datetime_to_timestamp, _timestamp_to_datetime
from gedidb.providers.tiledb_provider import TileDBProvider

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]

class GEDIProvider(TileDBProvider):
    """
    GEDIProvider class to interface with GEDI data stored in TileDB, with support for flexible storage types.

    Attributes
    ----------
    scalar_array_uri : str
        URI for accessing the scalar data array.
    profile_array_uri : str
        URI for accessing the profile data array.
    ctx : tiledb.Ctx
        TileDB context for the configured storage type (S3 or local).
    
    Methods
    -------
    get_available_variables() -> pd.DataFrame
        Retrieve a list of available variables with descriptions and units.
    get_variable_types() -> Dict[str, List[str]]
        Retrieve lists of variable names available in scalar and profile arrays.
    query_nearest_shots(...) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
        Query data for the nearest shots to a specified point.
    query_data(...) -> Tuple[Dict[str, np.ndarray], Dict[str, np.ndarray]]
        Query data within specified spatial and temporal bounds.
    get_data(...) -> Union[pd.DataFrame, xr.Dataset, None]
        Retrieve queried data in either Pandas DataFrame or Xarray Dataset format.
    """

    def __init__(self, storage_type: str = 'local', s3_bucket: Optional[str] = None, local_path: Optional[str] = './', 
                 endpoint_override: Optional[str] = None, region: str = 'eu-central-1'):
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
        endpoint_override : str, optional
            Custom endpoint URL for S3-compatible object stores (e.g., MinIO).
        region : str, optional
            AWS region for S3 access. Defaults to 'eu-central-1'.
        
        Notes
        -----
        Supports both S3 and local storage configurations based on `storage_type`.
        """
        super().__init__(storage_type, s3_bucket, local_path, endpoint_override, region)

    def query_nearest_shots(
        self, 
        variables: List[str], 
        point: Tuple[float, float], 
        num_shots: int, 
        radius: float = 0.1,
        start_time: Optional[np.datetime64] = None, 
        end_time: Optional[np.datetime64] = None,
        **quality_filters
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
        
        # Determine variable types (scalar vs. profile) and assign relevant variables to each type
        variable_types = self.get_variable_types()
        scalar_vars = [v for v in variables if v in variable_types["scalar"]] + DEFAULT_DIMS
        quality_vars = [q for q in quality_filters if q not in scalar_vars]
        scalar_vars += quality_vars  # Ensure quality variables are included in the scalar query
        profile_vars = [v for v in variables if v in variable_types["profile"]] + DEFAULT_DIMS
        
        # Convert start and end times to timestamps
        start_timestamp = _datetime_to_timestamp(np.datetime64(start_time) if start_time else None)
        end_timestamp = _datetime_to_timestamp(np.datetime64(end_time) if end_time else None)
        
        # Define bounding box limits based on the radius
        lon_min, lat_min = point[0] - radius, point[1] - radius
        lon_max, lat_max = point[0] + radius, point[1] + radius
    
        # Query the data within the bounding box
        scalar_data_subset = self._query_array(
            self.scalar_array_uri, scalar_vars, lat_min, lat_max, lon_min, lon_max, start_timestamp, end_timestamp, **quality_filters
        )
        profile_data_subset = self._query_array(
            self.profile_array_uri, profile_vars, lat_min, lat_max, lon_min, lon_max, start_timestamp, end_timestamp, **quality_filters
        )
        
        # Apply quality filters to both scalar and profile data
        if quality_filters:
            mask = self._apply_quality_filters(scalar_data_subset, quality_filters)
            scalar_data_subset = {var: data[mask] for var, data in scalar_data_subset.items()}
            
            # Determine the number of profile points
            num_profile_points = len(np.unique(profile_data_subset["profile_point"]))
            
            # Broadcast the mask for profile data along the profile points dimension
            expanded_mask = np.broadcast_to(mask[:, np.newaxis], (len(mask), num_profile_points)).ravel()
            profile_data_subset = {var: data[expanded_mask] for var, data in profile_data_subset.items()}

        # Remove quality variables from scalar_data if they were not requested
        for q_var in quality_vars:
            if q_var not in variables:
                scalar_data_subset.pop(q_var, None)
    
        if not scalar_data_subset or not profile_data_subset:
            logger.info("No points found in the bounding box.")
            return {}, {}
    
        # Find nearest points using KD-tree
        longitudes, latitudes = scalar_data_subset["longitude"], scalar_data_subset["latitude"]
        shot_numbers = scalar_data_subset["shot_number"]
        tree = cKDTree(np.column_stack((longitudes, latitudes)))
        distances, indices = tree.query(point, k=min(num_shots, len(shot_numbers)))
        nearest_shots = shot_numbers[indices]
    
        # Filter data based on nearest shot numbers
        scalar_data = {k: np.array(v)[np.isin(shot_numbers, nearest_shots)] for k, v in scalar_data_subset.items()}
        profile_data = {k: np.array(v)[np.isin(profile_data_subset["shot_number"], nearest_shots)] for k, v in profile_data_subset.items()}
    
        return scalar_data, profile_data
    
    
    def query_data(
            self, 
            variables: List[str], 
            geometry: Optional[gpd.GeoDataFrame] = None,
            start_time: Optional[str] = None, 
            end_time: Optional[str] = None,
            **quality_filters
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
            lat_min, lat_max = -90.0, 90.0
            lon_min, lon_max = -180.0, 180.0
    
        # Convert start and end times to numpy datetime64
        start_time = np.datetime64(start_time) if start_time else None
        end_time = np.datetime64(end_time) if end_time else None
        start_timestamp = _datetime_to_timestamp(start_time)
        end_timestamp = _datetime_to_timestamp(end_time)
    
        # Determine scalar and profile variables, including quality filter variables
        variable_types = self.get_variable_types()
        scalar_vars = [v for v in variables if v in variable_types["scalar"]] + DEFAULT_DIMS
        profile_vars = [v for v in variables if v in variable_types["profile"]] + DEFAULT_DIMS
        quality_vars = [q for q in quality_filters if q not in scalar_vars]
        scalar_vars += quality_vars  # Ensure quality variables are included in the scalar query
    
        # Query both scalar and profile arrays within the specified bounds and time range
        scalar_data = self._query_array(
            self.scalar_array_uri, scalar_vars, lat_min, lat_max, lon_min, lon_max, start_timestamp, end_timestamp
        )
        profile_data = self._query_array(
            self.profile_array_uri, profile_vars, lat_min, lat_max, lon_min, lon_max, start_timestamp, end_timestamp
        )
    
        # Apply quality filters to both scalar and profile data
        if quality_filters:
            mask = self._apply_quality_filters(scalar_data, quality_filters)
            scalar_data = {var: data[mask] for var, data in scalar_data.items()}
            
            # Determine the number of profile points
            num_profile_points = len(np.unique(profile_data["profile_point"]))
            
            # Broadcast the mask for profile data along the profile points dimension
            expanded_mask = np.broadcast_to(mask[:, np.newaxis], (len(mask), num_profile_points)).ravel()
            profile_data = {var: data[expanded_mask] for var, data in profile_data.items()}

        # Remove quality variables from scalar_data if they were not requested
        for q_var in quality_vars:
            if q_var not in variables:
                scalar_data.pop(q_var, None)
    
        return scalar_data, profile_data
    
    def _apply_quality_filters(self, data: Dict[str, np.ndarray], quality_filters: Dict[str, str]) -> np.ndarray:
        """
        Apply complex quality filters to a dictionary of data arrays, supporting conditions like '>= 0.9 and <= 1.0'.
    
        Parameters
        ----------
        data : Dict[str, np.ndarray]
            Dictionary of data arrays to be filtered.
        quality_filters : Dict[str, str]
            Dictionary of quality filters with conditions (e.g., '>= 0.9 and <= 1.0').
    
        Returns
        -------
        np.ndarray
            Boolean mask array that indicates which rows satisfy all quality filters.
        """
        # Start with a mask that includes all data
        mask = np.ones(len(next(iter(data.values()))), dtype=bool)
    
        # Regular expression to match operators and values (e.g., '>= 0.9', '<= 1.0')
        pattern = re.compile(r'([<>]=?|==?)\s*([0-9\.]+|[a-zA-Z_]+)')
    
        for filter_var, condition in quality_filters.items():
            if filter_var not in data:
                continue
    
            # Parse the condition (e.g., '>= 0.9 and <= 1.0')
            conditions = condition.split(' and ')
            var_mask = np.ones(len(data[filter_var]), dtype=bool)
    
            for cond in conditions:
                match = pattern.match(cond.strip())
                if not match:
                    raise ValueError(f"Invalid filter condition: {cond}")
                operator, value = match.groups()
    
                # Convert the value to a float if it is numeric, otherwise treat as string (e.g., 'coverage')
                try:
                    value = float(value)
                except ValueError:
                    pass
    
                # Apply the condition to create a mask
                if operator == '>=':
                    var_mask &= data[filter_var] >= value
                elif operator == '<=':
                    var_mask &= data[filter_var] <= value
                elif operator == '>':
                    var_mask &= data[filter_var] > value
                elif operator == '<':
                    var_mask &= data[filter_var] < value
                elif operator == '==':
                    var_mask &= data[filter_var] == value
                elif operator == '=':
                    var_mask &= data[filter_var] == value
                else:
                    raise ValueError(f"Unsupported operator: {operator}")
    
            # Combine this variable's mask with the overall mask
            mask &= var_mask
    
        return mask

    def get_data(
        self, 
        variables: List[str], 
        geometry: Optional[gpd.GeoDataFrame] = None,
        start_time: Optional[str] = None, 
        end_time: Optional[str] = None,
        return_type: str = "xarray", 
        query_type: str = "bounding_box",
        point: Optional[Tuple[float, float]] = None, 
        num_shots: int = 10, 
        radius: float = 0.1,
        **quality_filters
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
            scalar_data, profile_data = self.query_nearest_shots(
                variables, point, num_shots, radius, start_time, end_time, **quality_filters
            )
        else:
            scalar_data, profile_data = self.query_data(
                variables, geometry, start_time, end_time, **quality_filters
            )
    
        if not scalar_data and not profile_data:
            logger.info("No data found for specified criteria.")
            return None
    
        metadata = self.get_available_variables()
        return (
            self.to_xarray(scalar_data, profile_data, metadata)
            if return_type == "xarray"
            else self.to_dataframe(scalar_data, profile_data)
        )


    def to_dataframe(self, scalar_data: Dict[str, np.ndarray], profile_data: Dict[str, np.ndarray]) -> pd.DataFrame:
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
        scalar_df = pd.DataFrame.from_dict(scalar_data)
        
        # Convert profile data to DataFrame, aggregate by shot_number
        profile_df = pd.DataFrame.from_dict(profile_data)
        profile_df = profile_df.drop(columns=["latitude", "longitude", "time", "profile_point"]) \
                               .groupby("shot_number") \
                               .agg(list) \
                               .reset_index()
        
        # Merge scalar and profile data on shot_number
        return pd.merge(scalar_df, profile_df, on="shot_number", how="inner")


    def to_xarray(self, scalar_data: Dict[str, np.ndarray], profile_data: Dict[str, np.ndarray], metadata: pd.DataFrame) -> xr.Dataset:
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
        profile_data : Dict[str, np.ndarray]
            Dictionary containing profile data variables. Keys are variable names, and values
            are numpy arrays with multiple `profile_point` measurements per `shot_number`.
        metadata : pd.DataFrame
            DataFrame containing variable metadata (e.g., descriptions and units). The index 
            should match the variable names in `scalar_data` and `profile_data`.
    
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
        scalar_vars = [k for k in scalar_data if k not in ["latitude", "longitude", "time", "shot_number"]]
        profile_vars = [k for k in profile_data if k not in ["latitude", "longitude", "time", "profile_point", "shot_number"]]
        times = np.array(_timestamp_to_datetime(scalar_data["time"]))
        unique_shot_numbers = np.unique(profile_data["shot_number"])
        unique_profile_points = np.unique(profile_data["profile_point"])
    
        # Create Dataset for scalar data
        scalar_ds = xr.Dataset({
            var: xr.DataArray(scalar_data[var], coords={"shot_number": scalar_data["shot_number"]}, dims=["shot_number"]) 
            for var in scalar_vars
        })
        
        # Create Dataset for profile data with reshaping
        profile_ds = xr.Dataset({
            var: xr.DataArray(
                profile_data[var].reshape(len(unique_shot_numbers), len(unique_profile_points)),
                coords={"shot_number": unique_shot_numbers, "profile_points": unique_profile_points},
                dims=["shot_number", "profile_points"]
            ) for var in profile_vars
        })
        
        # Merge scalar and profile Datasets
        dataset = xr.merge([scalar_ds, profile_ds])
        dataset = dataset.assign_coords({
            "latitude": ("shot_number", scalar_data["latitude"]),
            "longitude": ("shot_number", scalar_data["longitude"]),
            "time": ("shot_number", times)
        })
        
        # Attach metadata to Dataset variables
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
        for var in dataset.variables:
            if var in metadata.index:
                var_metadata = metadata.loc[var]
                dataset[var].attrs.update({
                    'description': var_metadata.get('description', ''),
                    'units': var_metadata.get('units', ''),
                    'product_level': var_metadata.get('product_level', '')
                })

