# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de, besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-LicenseCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import logging
import tiledb
import numpy as np
import pandas as pd
from typing import Optional, List, Dict

# Configure the logger
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]

class TileDBProvider:
    """
    A base provider class for managing low-level interactions with TileDB arrays for GEDI data.
    This class supports both S3 and local storage configurations, allowing flexible access to scalar 
    and profile arrays stored in TileDB.

    Attributes
    ----------
    scalar_array_uri : str
        URI for accessing the scalar data array.
    profile_array_uri : str
        URI for accessing the profile data array.
    ctx : tiledb.Ctx
        TileDB context configured for either S3 or local access.

    Methods
    -------
    get_available_variables() -> pd.DataFrame
        Retrieve metadata about available variables in both scalar and profile arrays.
    get_variable_types() -> Dict[str, List[str]]
        Retrieve lists of variable names available in scalar and profile arrays.
    query_array(...)
        Execute a query on a specified TileDB array with spatial, temporal, and quality filters.
    """

    def __init__(self, storage_type: str = 'local', s3_bucket: Optional[str] = None, local_path: Optional[str] = './', 
                 url: Optional[str] = None, region: str = 'eu-central-1', credentials:Optional[dict]= None, n_workers:int =5):
        """
        Initialize the TileDBProvider with URIs for scalar and profile data arrays, configured based on storage type.

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
        - Configures TileDB contexts and array URIs based on storage type, either S3 or local.
        """
        self.n_workers = n_workers
        if storage_type.lower() == 's3':
            if not s3_bucket:
                raise ValueError("s3_bucket must be provided when storage_type is 's3'")
            self.scalar_array_uri = f"s3://{s3_bucket}/array_uri"
            self.ctx = self._initialize_s3_context(credentials, url, region)
        else:
            # Local storage
            self.scalar_array_uri = os.path.join(local_path, 'array_uri')
            self.ctx = self._initialize_local_context()

    def _initialize_s3_context(self, credentials:dict, url: str, region: str) -> tiledb.Ctx:
        """
        Set up and return a TileDB context configured for S3 storage with credentials from boto3.
        
        Parameters
        ----------
        endpoint_override : str
            The custom endpoint URL for S3-compatible storage (e.g., MinIO).
        region : str
            AWS region for S3 access.

        Returns
        -------
        tiledb.Ctx
            Configured TileDB context for S3 storage.
        """
        return tiledb.Ctx({
            "vfs.s3.aws_access_key_id": credentials['AccessKeyId'],
            "vfs.s3.aws_secret_access_key": credentials['SecretAccessKey'],
            "vfs.s3.endpoint_override": url,
            "vfs.s3.region": region,
            "py.init_buffer_bytes": "102400000"
        })

    def _initialize_local_context(self) -> tiledb.Ctx:
        """
        Set up and return a TileDB context configured for local file storage.

        Returns
        -------
        tiledb.Ctx
            Configured TileDB context for local storage.
        """
        return tiledb.Ctx({
            "py.init_buffer_bytes": "102400000"
        })


    def get_available_variables(self) -> pd.DataFrame:
        """
        Retrieve metadata for available variables in both scalar and profile TileDB arrays, 
        excluding specific fields such as 'array_type' and granule-specific metadata.
    
        This function consolidates metadata from both the scalar and profile arrays, organizing 
        them into a structured DataFrame with variable names as the index and associated attributes 
        (e.g., description, units) as columns.
    
        Returns
        -------
        pd.DataFrame
            A DataFrame containing variable names as the index and associated metadata attributes 
            (like description and units) as columns. Each row represents metadata for a specific 
            variable, providing details such as its units, description, and product level.
        
        Raises
        ------
        Exception
            If there is an error opening the TileDB arrays or retrieving metadata, the exception 
            is logged and re-raised.
        
        Notes
        -----
        - Metadata keys starting with "granule_" or containing "array_type" are ignored to exclude 
          granule-specific and array type information.
        - Metadata from both scalar and profile arrays is combined, allowing unified access to 
          all variables in the dataset.
    
        """
        try:
            with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as scalar_array:
                
                # Collect metadata for scalar and profile arrays, excluding unwanted keys
                scalar_metadata = {k: scalar_array.meta[k] for k in scalar_array.meta 
                                   if not k.startswith("granule_") and "array_type" not in k}
                
                # Combine metadata from scalar and profile arrays
                organized_metadata = {}
                
                # Organize metadata into nested dictionary structure for DataFrame conversion
                for key, value in scalar_metadata.items():
                    var_name, attr_type = key.split(".", 1)
                    if var_name not in organized_metadata:
                        organized_metadata[var_name] = {}
                    organized_metadata[var_name][attr_type] = value
                
                # Convert organized metadata into a DataFrame
                return pd.DataFrame.from_dict(organized_metadata, orient="index")
        
        except Exception as e:
            logger.error(f"Failed to retrieve available variables from TileDB: {e}")
            raise
    
    def _query_array(
        self, 
        array_uri: str, 
        variables: List[str], 
        lat_min: float, 
        lat_max: float, 
        lon_min: float, 
        lon_max: float, 
        start_time: Optional[np.datetime64], 
        end_time: Optional[np.datetime64],
        **filters
    ) -> Dict[str, np.ndarray]:
        """
        Execute a query on a TileDB array using spatial, temporal, and additional quality filters.
    
        This function retrieves data from a TileDB array based on specified spatial bounds, time range, 
        and additional quality criteria. It constructs a query to filter the data by latitude, longitude, 
        and optional temporal constraints, and returns the filtered data as a dictionary.
    
        Parameters
        ----------
        array_uri : str
            The URI of the TileDB array to query.
        variables : List[str]
            A list of variable names (attributes) to retrieve from the array.
        lat_min : float
            Minimum latitude for the bounding box filter.
        lat_max : float
            Maximum latitude for the bounding box filter.
        lon_min : float
            Minimum longitude for the bounding box filter.
        lon_max : float
            Maximum longitude for the bounding box filter.
        start_time : np.datetime64, optional
            The start time for temporal filtering; retrieves data from this timestamp onward.
        end_time : np.datetime64, optional
            The end time for temporal filtering; retrieves data up to this timestamp.
        **filters : dict
            Additional keyword arguments for filtering by data quality or other attributes. These 
            filters are applied as attribute constraints in the query.
    
        Returns
        -------
        Dict[str, np.ndarray]
            A dictionary containing the queried data, with variable names as keys and numpy arrays 
            as values for each attribute specified in `variables`.
    
        Notes
        -----
        - The `query.multi_index` method applies the specified bounding box and temporal filters.
        - Additional quality filters are passed as keyword arguments and applied to attributes.
        - Ensure the TileDB context (`self.ctx`) is configured correctly to access the array.
        """
        # Open the TileDB array
        with tiledb.open(array_uri, mode="r", ctx=self.ctx) as array:
            # Prepare attribute list
            attr_list = []
            profile_vars = {}

            # Check and expand profile variables
            for var in variables:
                if f"{var}.profile_length" in array.meta:
                    profile_length = array.meta[f"{var}.profile_length"]
                    profile_attrs = [f"{var}_{i}" for i in range(1, profile_length + 1)]
                    attr_list.extend(profile_attrs)
                    profile_vars[var] = profile_attrs
                else:
                    # Scalar variables
                    attr_list.append(var)
                    
            # Construct the quality filter condition
            cond_list = []
            for key, condition in filters.items():
                if 'and' in condition:
                    parts = condition.split('and')
                    for part in parts:
                        cond_list.append(f"{key} {part.strip()}")
                else:
                    cond_list.append(f"{key} {condition.strip()}")
            cond_string = " and ".join(cond_list) if cond_list else None
            
            # Query the data
            query = array.query(attrs=attr_list, cond=cond_string)
            data = query.multi_index[lat_min:lat_max, lon_min:lon_max, start_time:end_time]
            
            if len(data['shot_number']) == 0:
                return None, profile_vars
                        
            return data, profile_vars