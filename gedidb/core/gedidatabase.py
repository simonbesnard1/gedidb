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
from typing import Dict, Any, List, Optional
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

    def __init__(self, config: Dict[str, Any], credentials:Optional[dict]=None):
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
            self.array_uri = os.path.join(f"s3://{bucket}", 'gedi_array_uri')
        elif storage_type == 'local':
            base_path = config['tiledb'].get('local_path', './')
            self.array_uri = os.path.join(base_path, 'gedi_array_uri')
            
        self.overwrite = config['tiledb'].get('overwrite', False)
        self.variables_config = self._load_variables_config(config)
        
        # Set up TileDB context based on storage type
        if storage_type == 's3':
            # S3 TileDB context with consolidation settings
            self.tiledb_config =tiledb.Config({# Timeout settings
                                                "sm.vfs.s3.connect_timeout_ms": config['tiledb']['s3_timeout_settings'].get('connect_timeout_ms', "10800"),
                                                "sm.vfs.s3.request_timeout_ms": config['tiledb']['s3_timeout_settings'].get('request_timeout_ms', "3000"),
                                                "sm.vfs.s3.connect_max_tries": config['tiledb']['s3_timeout_settings'].get('connect_max_tries', "5"),
                    
                                                # Memory budget settings
                                                "sm.memory_budget": config['tiledb']['consolidation_settings'].get('memory_budget', "5000000000"),
                                                "sm.memory_budget_var": config['tiledb']['consolidation_settings'].get('memory_budget_var', "2000000000"),
                                            
                                                # S3-specific configurations (if using S3)
                                                "vfs.s3.aws_access_key_id": credentials['AccessKeyId'],
                                                "vfs.s3.aws_secret_access_key": credentials['SecretAccessKey'],                                                
                                                "vfs.s3.endpoint_override": config['tiledb']['url'],
                                                "vfs.s3.region": 'eu-central-1',
                                            })
        elif storage_type == 'local':
            # Local TileDB context with consolidation settings
            self.tiledb_config = tiledb.Config({
                                                # Memory budget settings
                                                "sm.memory_budget": config['tiledb']['consolidation_settings'].get('memory_budget', "5000000000"),
                                                "sm.memory_budget_var": config['tiledb']['consolidation_settings'].get('memory_budget_var', "2000000000"),                                            
                                            })
        
        self.ctx = tiledb.Ctx(self.tiledb_config)
        
    def consolidate_fragments(self) -> None:
        """
        Consolidate fragments, metadata, and commit logs for both array 
        to optimize storage and access.
    
        Parameters:
        ----------
        consolidate : bool, default=True
            If True, perform fragment consolidation on both arrays.
        """
        try:
            logger.info(f"Consolidating process for array: {self.array_uri}")
            
            # Update configuration for fragment consolidation
            self.tiledb_config["sm.consolidation.mode"] = "fragments"
            self.tiledb_config["sm.vacuum.mode"] = "fragments"
            
            with tiledb.open(self.array_uri, 'r', ctx=self.ctx) as array_:
                
                # Generate the consolidation plan
                cons_plan = tiledb.ConsolidationPlan(self.ctx, array_, self.config['tiledb']['consolidation_settings'].get('fragment_size', 100_000_000))
            
            # Consolidate fragments
            for plan_ in cons_plan:
                tiledb.consolidate(self.array_uri, ctx=self.ctx, config=self.tiledb_config, fragment_uris=plan_['fragment_uris'])

            # Vacuum after fragment consolidation
            tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
            
            # Update configuration for metadata consolidation
            self.tiledb_config["sm.consolidation.mode"] = "array_meta"
            self.tiledb_config["sm.vacuum.mode"] = "array_meta"
            
            # Consolidate metadata
            tiledb.consolidate(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
            tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
            
            # Update configuration for commit log consolidation
            self.tiledb_config["sm.consolidation.mode"] = "fragment_meta"
            self.tiledb_config["sm.vacuum.mode"] = "fragment_meta"
            
            # Consolidate commit logs
            tiledb.consolidate(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
            tiledb.vacuum(self.array_uri, ctx=self.ctx, config=self.tiledb_config)
            logger.info(f"Consolidation complete for array: {self.array_uri}")
        
        except tiledb.TileDBError as e:
            logger.error(f"Error during consolidation of {self.array_uri}: {e}")

    def _create_arrays(self) -> None:
        """Define and create TileDB arrays based on configuration."""
        self._create_array(self.array_uri)
        self._add_variable_metadata()

    def _create_array(self, uri: str) -> None:
        """Creates a TileDB array with schema based on configuration."""
        if tiledb.array_exists(uri, ctx=self.ctx):
            if self.overwrite:
                tiledb.remove(uri, ctx=self.ctx)
                logger.info(f"Overwritten existing TileDB array at {uri}")
            else:
                logger.info(f"TileDB array already exists at {uri}. Skipping creation.")
                return
        
        domain = self._create_domain()
        attributes = self._create_attributes()
        schema = tiledb.ArraySchema(domain=domain, attrs=attributes, sparse=True)
        tiledb.Array.create(uri, schema, ctx=self.ctx)
        
    def _create_domain(self) -> tiledb.Domain:
        """
        Creates a TileDB domain based on spatial, temporal, and profile dimensions specified in the configuration.
        
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
            tiledb.Dim("time", domain=(time_min, time_max), tile=1825, dtype="int64")  # ~5-year tile for time
        ]

        return tiledb.Domain(*dimensions)
    
    def _create_attributes(self) -> List[tiledb.Attr]:
        """
        Creates TileDB attributes for gedi data.
        
        Returns:
        --------
        List[tiledb.Attr]
            A list of TileDB attributes configured with appropriate data types.
        
        Notes:
        ------
        - The `timestamp_ns` attribute is added to the array.
        """
        attributes = []
        
        # Add scalar variables
        for var_name, var_info in self.variables_config.items():
            if not var_info.get('is_profile', False):
                attributes.append(tiledb.Attr(name=var_name, dtype=var_info["dtype"]))
    
        # Add flattened profile variables dynamically
        for var_name, var_info in self.variables_config.items():
            if var_info.get('is_profile', False):
                profile_length = var_info.get('profile_length', 1)
                for i in range(profile_length):
                    attr_name = f"{var_name}_{i + 1}"
                    attributes.append(tiledb.Attr(name=attr_name, dtype=var_info["dtype"]))
        
        attributes.append(tiledb.Attr(name="timestamp_ns", dtype="int64"))
        
        return attributes

    def _add_variable_metadata(self) -> None:
        """
        Add metadata to the global TileDB arrays for each variable, including units and long_name.
        This information is pulled from the configuration.
        """
        # Add metadata to array
        with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
            for var_name, var_info in self.variables_config.items():
                try:
                    units = var_info.get("units", "unknown")
                    description = var_info.get("description", "No description available")
                    dtype = var_info.get("dtype", "unknown")
                    product_level = var_info.get("product_level", "unknown")
                    
                    array.meta[f"{var_name}.units"] = units
                    array.meta[f"{var_name}.description"] = description
                    array.meta[f"{var_name}.dtype"] = dtype
                    array.meta[f"{var_name}.product_level"] = product_level
                    if var_info.get('is_profile', False):
                        array.meta[f"{var_name}.profile_length"] = var_info.get("profile_length", 0)
                       
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
    
        # Prepare coordinates (dimensions)
        coords = {
            dim_name: (
                convert_to_days_since_epoch(granule_data[dim_name].values)
                if dim_name == 'time' else granule_data[dim_name].values
            )
            for dim_name in self.config["tiledb"]['dimensions']
        }
    
        # Extract scalar and profile data attributes
        data = {}
        for var_name, var_info in self.variables_config.items():
            if var_info.get('is_profile', False):
                # Process profile variables (e.g., rh_1, rh_2, etc.)
                profile_length = var_info.get('profile_length', 1)
                for i in range(profile_length):
                    expanded_var_name = f"{var_name}_{i + 1}"
                    if expanded_var_name in granule_data:
                        data[expanded_var_name] = granule_data[expanded_var_name].values
            else:
                # Process scalar variables
                if var_name in granule_data:
                    data[var_name] = granule_data[var_name].values

        data['timestamp_ns'] = (pd.to_datetime(granule_data['time']).astype('int64') // 1000).values

        # Write to the scalar array
        with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as array:
            dim_names = [dim.name for dim in array.schema.domain]
            dims = tuple(coords[dim_name] for dim_name in dim_names)
            array[dims] = data

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
            with tiledb.open(self.array_uri, mode="r", ctx=self.ctx) as scalar_array:
                scalar_metadata = {key: scalar_array.meta[key] for key in scalar_array.meta.keys() if "_status" in key}
        
            # Combine metadata from both arrays and check each granule
            for granule_id in granule_ids:
                granule_key = f"granule_{granule_id}_status"
                scalar_processed = scalar_metadata.get(granule_key, "") == "processed"
                
                # Set status as True only if both arrays mark the granule as processed
                granule_statuses[granule_id] = scalar_processed
        
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
            with tiledb.open(self.array_uri, mode="w", ctx=self.ctx) as scalar_array:
                
                scalar_array.meta[f"granule_{granule_key}_status"] = "processed"
                scalar_array.meta[f"granule_{granule_key}_processed_date"] = pd.Timestamp.utcnow().isoformat()
            
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
