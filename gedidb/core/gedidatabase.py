# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import tiledb
import pandas as pd
import logging
from typing import Dict, Any, List
import boto3
import numpy as np
import os

from gedidb.utils.geospatial_tools import  _datetime_to_timestamp_days, convert_to_days_since_epoch

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.WARNING)

class GEDIDatabase:
    """
    A class to manage the creation and operation of global TileDB arrays for GEDI data storage.
    This class is configured via an external configuration, allowing flexible schema definitions and metadata handling.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize GEDIDatabase with configuration, supporting both S3 and local storage.

        Parameters:
        ----------
        config : dict
            Configuration dictionary.
        """
        self.config = config
        storage_type = config['tiledb'].get('storage_type', 'local').lower()
        
        # Set array URIs based on storage type
        if storage_type == 's3':
            bucket = config['tiledb']['s3_bucket']
            self.scalar_array_uri = os.path.join(f"s3://{bucket}", 'scalar_array_uri')
            self.profile_array_uri = os.path.join(f"s3://{bucket}", 'profile_array_uri')
        else:  # Local storage
            base_path = config['tiledb'].get('local_path', './')
            self.scalar_array_uri = os.path.join(base_path, 'scalar_array_uri')
            self.profile_array_uri = os.path.join(base_path, 'profile_array_uri')
        
        self.overwrite = config['tiledb'].get('overwrite', False)
        self.variables_config = self._load_variables_config(config)
        
        # Set up TileDB context based on storage type
        if storage_type == 's3':
            # Initialize boto3 session for S3 credentials
            session = boto3.Session()
            creds = session.get_credentials()
            # S3 TileDB context with consolidation settings
            self.tiledb_config =tiledb.Config({
                                                # Consolidation settings
                                                "sm.consolidation.steps": 10,
                                                "sm.consolidation.step_max_frags": 100,  # Adjust based on fragment count
                                                "sm.consolidation.step_min_frags": 10,
                                                "sm.consolidation.buffer_size": 5_000_000_000,  # 5GB buffer size per attribute/dimension
                                                "sm.consolidation.step_size_ratio":1.5, #  allow fragments that differ by up to 50% in size to be consolidated.
                                                "sm.consolidation.amplification": 1.2, #  Allow for 20% amplification
                                                
                                                # Memory budget settings
                                                "sm.memory_budget": "150000000000",  # 150GB total memory budget
                                                "sm.memory_budget_var": "50000000000",  # 50GB for variable-sized attributes
                                            
                                                # S3-specific configurations (if using S3)
                                                "vfs.s3.aws_access_key_id": creds.access_key,
                                                "vfs.s3.aws_secret_access_key": creds.secret_key,
                                                "vfs.s3.endpoint_override": config['tiledb']['endpoint_override'],
                                                "vfs.s3.region": 'eu-central-1',
                                            })
        else:
            # Local TileDB context with consolidation settings
            self.tiledb_config = tiledb.Config({
                                                # Consolidation settings
                                                "sm.consolidation.steps": 10,
                                                "sm.consolidation.step_max_frags": 100,  # Adjust based on fragment count
                                                "sm.consolidation.step_min_frags": 10,
                                                "sm.consolidation.buffer_size": 5_000_000_000,  # 5GB buffer size per attribute/dimension
                                                "sm.consolidation.step_size_ratio":1.5, #  allow fragments that differ by up to 50% in size to be consolidated.
                                                "sm.consolidation.amplification": 1.2, #  Allow for 20% amplification

                                                # Memory budget settings
                                                "sm.memory_budget": "150000000000",  # 150GB total memory budget
                                                "sm.memory_budget_var": "50000000000",  # 50GB for variable-sized attributes
                                            })
        
        self.ctx = tiledb.Ctx(self.tiledb_config)
        
        def consolidate_fragments(self) -> None:
            """
            Consolidate fragments, metadata, and commit logs for both scalar and profile arrays 
            to optimize storage and access.
        
            Parameters:
            ----------
            consolidate : bool, default=True
                If True, perform fragment consolidation on both arrays.
            """
            for array_uri in [self.scalar_array_uri, self.profile_array_uri]:
                try:
                    logger.info(f"Consolidating fragments for array: {array_uri}")
                    
                    # Update configuration for fragment consolidation
                    self.tiledb_config["sm.consolidation.mode"] = "fragments"
                    self.tiledb_config["sm.vacuum.mode"] = "fragments"
                    
                    # Consolidate fragments
                    tiledb.consolidate(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    
                    # Vacuum after fragment consolidation
                    tiledb.vacuum(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    logger.info(f"Fragment consolidation complete for array: {array_uri}")
                    
                    # Update configuration for metadata consolidation
                    logger.info(f"Consolidating metadata (__meta) for array: {array_uri}")
                    self.tiledb_config["sm.consolidation.mode"] = "array_meta"
                    self.tiledb_config["sm.vacuum.mode"] = "array_meta"
                    
                    # Consolidate metadata
                    tiledb.consolidate(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    tiledb.vacuum(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    logger.info(f"Metadata consolidation complete for array: {array_uri}")
                    
                    # Update configuration for commit log consolidation
                    logger.info(f"Consolidating commit logs (__commits) for array: {array_uri}")
                    self.tiledb_config["sm.consolidation.mode"] = "fragment_meta"
                    self.tiledb_config["sm.vacuum.mode"] = "fragment_meta"
                    
                    # Consolidate commit logs
                    tiledb.consolidate(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    tiledb.vacuum(array_uri, ctx=self.ctx, config=self.tiledb_config)
                    logger.info(f"Commit log consolidation complete for array: {array_uri}")
                
                except tiledb.TileDBError as e:
                    logger.error(f"Error during consolidation of {array_uri}: {e}")

    def _create_arrays(self) -> None:
        """Define and create scalar and profile TileDB arrays based on configuration."""
        self._create_array(self.scalar_array_uri, scalar=True)
        self._create_array(self.profile_array_uri, scalar=False)
        self._add_variable_metadata()

    def _create_array(self, uri: str, scalar: bool) -> None:
        """Creates a TileDB array (scalar or profile) with schema based on configuration."""
        if tiledb.array_exists(uri, ctx=self.ctx):
            if self.overwrite:
                tiledb.remove(uri, ctx=self.ctx)
                logger.info(f"Overwritten existing  {'scalar' if scalar else 'profile'} TileDB array at {uri}")
            else:
                logger.info(f"{'scalar' if scalar else 'profile'} TileDB array already exists at {uri}. Skipping creation.")
                return
        
        domain = self._create_domain(scalar)
        attributes = self._create_attributes(scalar)
        schema = tiledb.ArraySchema(domain=domain, attrs=attributes, sparse=True)
        tiledb.Array.create(uri, schema, ctx=self.ctx)
        
    def _create_domain(self, scalar: bool) -> tiledb.Domain:
        """
        Creates a TileDB domain based on spatial, temporal, and profile dimensions specified in the configuration.
        
        Parameters:
        ----------
        scalar : bool
            If True, creates a domain for scalar data (3D: latitude, longitude, time).
            If False, includes an additional `profile_point` dimension for profile data (4D).
        
        Returns:
        --------
        tiledb.Domain
            A TileDB Domain object configured according to the spatial, temporal, and profile settings.
    
        Raises:
        -------
        ValueError:
            If spatial or temporal ranges are not fully specified in the configuration.
        """
        spatial_range = self.config.get("tiledb", {}).get("spatial_range", {})
        lat_min, lat_max = spatial_range.get("lat_min"), spatial_range.get("lat_max")
        lon_min, lon_max = spatial_range.get("lon_min"), spatial_range.get("lon_max")
        time_range = self.config.get("tiledb", {}).get("time_range", {})
        time_min = _datetime_to_timestamp_days(time_range.get("start_time"))
        time_max = _datetime_to_timestamp_days(time_range.get("end_time"))
        
        # Validate ranges to prevent undefined domain errors
        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError("Spatial and temporal ranges must be fully specified in the configuration.")
        
        # Define the main dimensions: latitude, longitude, and time
        dimensions = [
            tiledb.Dim("latitude", domain=(lat_min, lat_max), tile=1, dtype="float64"),
            tiledb.Dim("longitude", domain=(lon_min, lon_max), tile=1, dtype="float64"),
            tiledb.Dim("time", domain=(time_min, time_max), tile=365, dtype="int64")  # 1-year tile for time
        ]
    
        # Add profile_point dimension if not scalar
        if not scalar:
            max_profile_length = self.config['tiledb'].get('max_profile_length', 100)
            dimensions.append(tiledb.Dim("profile_point", domain=(0, max_profile_length), tile=max_profile_length, dtype="int32"))
    
        return tiledb.Domain(*dimensions)
    
    def _create_attributes(self, scalar: bool) -> List[tiledb.Attr]:
        """
        Creates TileDB attributes for scalar or profile data.
        
        Parameters:
        ----------
        scalar : bool
            If True, creates attributes for scalar data (per-shot-level).
            If False, creates attributes for profile data (profile-level).
        
        Returns:
        --------
        List[tiledb.Attr]
            A list of TileDB attributes configured with appropriate data types.
        
        Notes:
        ------
        - The `shot_number` attribute is added to the profile array only if `scalar` is False.
        """
        attributes = []
        
        for var_name, var_info in self.variables_config.items():
            dtype = var_info.get("dtype", "float64")
            is_profile = var_info.get("is_profile", False)
            
            # Define attributes based on whether they belong to scalar or profile data
            if scalar and not is_profile:
                attributes.append(tiledb.Attr(name=var_name, dtype=dtype))
            elif not scalar and is_profile:
                attributes.append(tiledb.Attr(name=var_name, dtype=dtype))
    
        # Add `shot_number` attribute if not scalar
        if not scalar:
            attributes.append(tiledb.Attr(name="shot_number", dtype="int64"))
   
        return attributes

    def _add_variable_metadata(self) -> None:
        """
        Add metadata to the global TileDB arrays for each variable, including units and long_name.
        This information is pulled from the configuration.
        """
        # Add metadata to scalar array
        with tiledb.open(self.scalar_array_uri, mode="w", ctx=self.ctx) as array:
            array.meta["array_type"] = "scalar"
            for var_name, var_info in self.variables_config.items():
                if not var_info.get('is_profile', False):
                    try:
                        units = var_info.get("units", "unknown")
                        description = var_info.get("description", "No description available")
                        dtype = var_info.get("dtype", "unknown")
                        product_level = var_info.get("product_level", "unknown")
                        
                        array.meta[f"{var_name}.units"] = units
                        array.meta[f"{var_name}.description"] = description
                        array.meta[f"{var_name}.dtype"] = dtype
                        array.meta[f"{var_name}.product_level"] = product_level 
                    except KeyError as e:
                        logger.warning(f"Missing metadata key for {var_name}: {e}")
    
        # Add metadata to profile array
        with tiledb.open(self.profile_array_uri, mode="w", ctx=self.ctx) as array:
            array.meta["array_type"] = "profile"
            for var_name, var_info in self.variables_config.items():
                if var_info.get('is_profile', False):
                    try:
                        units = var_info.get("units", "unknown")
                        description = var_info.get("description", "No description available")
                        dtype = var_info.get("dtype", "unknown")
                        product_level = var_info.get("product_level", "unknown")
                        
                        array.meta[f"{var_name}.units"] = units
                        array.meta[f"{var_name}.description"] = description
                        array.meta[f"{var_name}.dtype"] = dtype
                        array.meta[f"{var_name}.product_level"] = product_level 
                    except KeyError as e:
                        logger.warning(f"Missing metadata key for {var_name}: {e}")

    def write_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write the parsed GEDI granule data to the global TileDB arrays.
    
        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data, with variable names matching the configuration.
    
        Raises:
        -------
        ValueError if required dimension data is missing.
        """
        # Check if all required dimensions are present
        missing_dims = [dim for dim in self.config["tiledb"]['dimensions'] if dim not in granule_data]
        if missing_dims:
            raise ValueError(f"Granule data is missing required dimension data: {missing_dims}")
    
        # Prepare and write scalar data
        self.write_scalar_granule(granule_data)
    
        # Prepare and write profile data
        profile_vars = [var_name for var_name, var_info in self.variables_config.items() if var_info.get('is_profile', False)]
        if profile_vars:
            self.write_profile_granule(granule_data, profile_vars)
        
    def write_scalar_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write scalar data to the scalar TileDB array.
    
        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data.
        """
        # Prepare coordinates (dimensions)
        coords = {
            dim_name: (
                convert_to_days_since_epoch(granule_data[dim_name]).values
                if dim_name == 'time' else granule_data[dim_name].values
            )
            for dim_name in self.config["tiledb"]['dimensions']
        }
        
        # Extract scalar data attributes
        data = {
            var_name: granule_data[var_name].values
            for var_name, var_info in self.variables_config.items()
            if not var_info.get('is_profile', False)
        }
        
        data['timestamp_ns'] =(pd.to_datetime(granule_data['time']).astype('int64') // 1000).values
    
        # Write to the scalar array
        with tiledb.open(self.scalar_array_uri, mode="w", ctx=self.ctx) as array:
            dim_names = [dim.name for dim in array.schema.domain]
            dims = tuple(coords[dim_name] for dim_name in dim_names)
            array[dims] = data
    
    def write_profile_granule(self, granule_data: pd.DataFrame, profile_vars: list) -> None:
        """
        Write profile data to the profile TileDB array.
    
        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data.
        profile_vars : list
            List of profile variable names.
        """
        # Process profile data and coordinates
        coords, data = self._process_profile_variables(granule_data, profile_vars)
        
        # Write profile data to the array
        with tiledb.open(self.profile_array_uri, mode="w", ctx=self.ctx) as array:
            dim_names = [dim.name for dim in array.schema.domain]
            dims = tuple(coords[dim_name] for dim_name in dim_names)
            array[dims] = data
    
    def _process_profile_variables(self, granule_data: pd.DataFrame, profile_vars: list):
        """
        Process profile variables and add 'shot_number' as a variable instead of a dimension.
    
        Parameters:
        -----------
        granule_data : pd.DataFrame
            DataFrame containing the profile variables as lists or arrays.
        profile_vars : list
            List of profile variable names.
    
        Returns:
        --------
        coords : dict
            Coordinate arrays for 'latitude', 'longitude', 'time', 'profile_point'.
        data : dict
            Data dictionary containing the profile variables and 'shot_number'.
        """
        
        # Convert 'time' to integer timestamps in microseconds
        time_array = convert_to_days_since_epoch(granule_data['time']).values
        lat_array = granule_data['latitude'].values
        lon_array = granule_data['longitude'].values
        shot_numbers = granule_data['shot_number'].values
    
        # Determine maximum profile length across all observations
        max_profile_length = granule_data[profile_vars].map(len).values.max()
        num_observations = len(granule_data)
        
        # Initialize data arrays with NaNs to max profile length
        data_arrays = {var_name: np.full((num_observations, max_profile_length), np.nan, dtype=np.float32) for var_name in profile_vars}
    
        # Fill each profile variable with padded data
        for var_name in profile_vars:
            for i, var_data in enumerate(granule_data[var_name]):
                data_arrays[var_name][i, :len(var_data)] = var_data
        
        # Flatten data arrays for TileDB input
        for var_name in data_arrays:
            data_arrays[var_name] = data_arrays[var_name].flatten()
        
        # Create coordinate arrays
        latitudes = np.repeat(lat_array, max_profile_length)
        longitudes = np.repeat(lon_array, max_profile_length)
        times = np.repeat(time_array, max_profile_length)
        profile_points = np.tile(np.arange(max_profile_length), num_observations)
        
        # Flatten shot_numbers to align with profile data and add it as a variable
        shot_numbers_repeated = np.repeat(shot_numbers, max_profile_length)
        data_arrays['shot_number'] = shot_numbers_repeated
        
        coords = {
            'latitude': latitudes,
            'longitude': longitudes,
            'time': times,
            'profile_point': profile_points
        }
        
        return coords, data_arrays

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
            # Open scalar array and check metadata for granule statuses
            with tiledb.open(self.scalar_array_uri, mode="r", ctx=self.ctx) as scalar_array:
                scalar_metadata = {key: scalar_array.meta[key] for key in scalar_array.meta.keys() if "_status" in key}
        
            # Open profile array and check metadata for granule statuses
            with tiledb.open(self.profile_array_uri, mode="r", ctx=self.ctx) as profile_array:
                profile_metadata = {key: profile_array.meta[key] for key in profile_array.meta.keys() if "_status" in key}
        
            # Combine metadata from both arrays and check each granule
            for granule_id in granule_ids:
                granule_key = f"granule_{granule_id}_status"
                scalar_processed = scalar_metadata.get(granule_key, "") == "processed"
                profile_processed = profile_metadata.get(granule_key, "") == "processed"
                
                # Set status as True only if both arrays mark the granule as processed
                granule_statuses[granule_id] = scalar_processed and profile_processed
        
        except tiledb.TileDBError as e:
            logger.error(f"Failed to access TileDB metadata: {e}")
        
        return granule_statuses
      
    def mark_granule_as_processed(self, granule_key: str) -> None:
        """
        Mark a granule as processed by storing its status and processing date in TileDB metadata.
    
        Parameters:
        ----------
        granule_key : str
            The unique identifier for the granule.
        """
        try:
            with tiledb.open(self.scalar_array_uri, mode="w", ctx=self.ctx) as scalar_array, \
                 tiledb.open(self.profile_array_uri, mode="w", ctx=self.ctx) as profile_array:
                
                scalar_array.meta[f"granule_{granule_key}_status"] = "processed"
                scalar_array.meta[f"granule_{granule_key}_processed_date"] = pd.Timestamp.utcnow().isoformat()
                
                profile_array.meta[f"granule_{granule_key}_status"] = "processed"
                profile_array.meta[f"granule_{granule_key}_processed_date"] = pd.Timestamp.utcnow().isoformat()
                
        except tiledb.TileDBError as e:
            logger.error(f"Failed to mark granule {granule_key} as processed: {e}")
            
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
        for level in ['level_2a', 'level_2b', 'level_4a', 'level_4c']:
            level_vars = config.get(level, {}).get('variables', {})
            for var_name, var_info in level_vars.items():
                variables_config[var_name] = var_info

        return variables_config
