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
import boto3

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
                 endpoint_override: Optional[str] = None, region: str = 'eu-central-1'):
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
        endpoint_override : str, optional
            Custom endpoint URL for S3-compatible object stores (e.g., MinIO).
        region : str, optional
            AWS region for S3 access. Defaults to 'eu-central-1'.
        
        Notes
        -----
        - Configures TileDB contexts and array URIs based on storage type, either S3 or local.
        """
        if storage_type.lower() == 's3':
            if not s3_bucket:
                raise ValueError("s3_bucket must be provided when storage_type is 's3'")
            self.scalar_array_uri = f"s3://{s3_bucket}/scalar_array_uri"
            self.profile_array_uri = f"s3://{s3_bucket}/profile_array_uri"
            self.ctx = self._initialize_s3_context(endpoint_override, region)
        else:
            # Local storage
            self.scalar_array_uri = os.path.join(local_path, 'scalar_array_uri')
            self.profile_array_uri = os.path.join(local_path, 'profile_array_uri')
            self.ctx = self._initialize_local_context()

    def _initialize_s3_context(self, endpoint_override: str, region: str) -> tiledb.Ctx:
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
        session = boto3.Session()
        creds = session.get_credentials()
        return tiledb.Ctx({
            "sm.num_reader_threads": 8,
            "vfs.s3.aws_access_key_id": creds.access_key,
            "vfs.s3.aws_secret_access_key": creds.secret_key,
            "vfs.s3.endpoint_override": endpoint_override,
            "vfs.s3.region": region
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
            "sm.num_reader_threads": 8
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
            with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as scalar_array, \
                 tiledb.open(self.profile_array_uri, mode="r", ctx=self.ctx) as profile_array:
                
                # Collect metadata for scalar and profile arrays, excluding unwanted keys
                scalar_metadata = {k: scalar_array.meta[k] for k in scalar_array.meta 
                                   if not k.startswith("granule_") and "array_type" not in k}
                profile_metadata = {k: profile_array.meta[k] for k in profile_array.meta 
                                    if not k.startswith("granule_") and "array_type" not in k}
                
                # Combine metadata from scalar and profile arrays
                combined_metadata = {**scalar_metadata, **profile_metadata}
                organized_metadata = {}
                
                # Organize metadata into nested dictionary structure for DataFrame conversion
                for key, value in combined_metadata.items():
                    var_name, attr_type = key.split(".", 1)
                    if var_name not in organized_metadata:
                        organized_metadata[var_name] = {}
                    organized_metadata[var_name][attr_type] = value
                
                # Convert organized metadata into a DataFrame
                return pd.DataFrame.from_dict(organized_metadata, orient="index")
        
        except Exception as e:
            logger.error(f"Failed to retrieve available variables from TileDB: {e}")
            raise


    def get_variable_types(self) -> Dict[str, List[str]]:
        """
        Retrieve variable names from the TileDB metadata, categorized into scalar and profile types.
    
        This function scans the metadata of both scalar and profile arrays and extracts unique 
        variable names based on keys containing a period (".") separator, which distinguishes 
        attribute names from variable names in the metadata structure.
    
        Returns
        -------
        Dict[str, List[str]]
            A dictionary with two keys:
            - "scalar": A list of variable names available in the scalar array.
            - "profile": A list of variable names available in the profile array.
        
        Notes
        -----
        - Variable names are extracted from metadata keys by taking the substring before the first 
          period (".") in each key. This assumes that metadata keys follow the format 
          "variable_name.attribute_name".
        - Only keys containing a period are included, filtering out any non-variable-related metadata.
    
        """
        with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as scalar_array, \
             tiledb.open(self.profile_array_uri, mode="r", ctx=self.ctx) as profile_array:
            
            # Extract unique variable names from metadata keys for scalar and profile arrays
            scalar_vars = list({k.split(".")[0] for k in scalar_array.meta if "." in k})
            profile_vars = list({k.split(".")[0] for k in profile_array.meta if "." in k})
    
        return {"scalar": scalar_vars, "profile": profile_vars}
    
    
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
        profile:bool = False,
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
        with tiledb.open(array_uri, mode="r", ctx=self.ctx) as array:
            query = array.query(attrs=variables)
            if profile:            
                data = query.multi_index[lat_min:lat_max, lon_min:lon_max, start_time:end_time, :]
            else:
                data = query.multi_index[lat_min:lat_max, lon_min:lon_max, start_time:end_time]                
            return data