# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import tiledb
import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, List, Optional
import os
import concurrent.futures
from dask.distributed import Client
from retry import retry

from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
    convert_to_days_since_epoch,
)
from gedidb.utils.tiledb_consolidation import SpatialConsolidationPlanner


# Configure the logger
logger = logging.getLogger(__name__)
logging.getLogger("botocore").setLevel(logging.WARNING)

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
                    
                    # S3 writting settings
                    "sm.vfs.s3.connect_timeout_ms": config["tiledb"][
                        "s3_settings"
                    ].get("connect_timeout_ms", "10800"),
                    "sm.vfs.s3.request_timeout_ms": config["tiledb"][
                        "s3_settings"
                    ].get("request_timeout_ms", "3000"),
                    "sm.vfs.s3.connect_max_tries": config["tiledb"][
                        "s3_settings"
                    ].get("connect_max_tries", "5"),
                    "vfs.s3.backoff_scale": config["tiledb"][
                        "s3_settings"
                    ].get("backoff_scale", "2.0"),  # Exponential backoff multiplier
                    "vfs.s3.backoff_max_ms": config["tiledb"][
                        "s3_settings"
                    ].get("backoff_max_ms", "120000"),  # Maximum backoff time of 120 seconds
                    "vfs.s3.multipart_part_size": config["tiledb"][
                        "s3_settings"
                    ].get("multipart_part_size", "52428800"),  # 50 MB
                    
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
        self, dataset: pd.DataFrame, chunk_size: float = 10
    ) -> Dict[tuple, pd.DataFrame]:
        """
        Splits a dataset into spatial chunks (quadrants) based on latitude and longitude.

        Parameters:
        ----------
        dataset : pd.DataFrame
            A DataFrame containing 'latitude' and 'longitude' columns.
        chunk_size : float, optional
            The size of each spatial chunk in degrees. Default is 10.

        Returns:
        --------
        Dict[tuple, pd.DataFrame]
            A dictionary where keys are tuples representing quadrant boundaries
            (lat_min, lat_max, lon_min, lon_max), and values are DataFrames containing
            the data in those quadrants.

        Raises:
        -------
        ValueError
            If the dataset does not contain 'latitude' or 'longitude' columns.
        """
        # Validate input columns
        required_columns = {"latitude", "longitude"}
        missing_columns = required_columns - set(dataset.columns)
        if missing_columns:
            raise ValueError(f"Dataset must contain columns: {missing_columns}")

        # Handle empty dataset
        if dataset.empty:
            return {}

        # Compute quadrant indices for grouping
        try:
            lat_quadrants = np.floor_divide(dataset["latitude"], chunk_size).astype(int)
            lon_quadrants = np.floor_divide(dataset["longitude"], chunk_size).astype(
                int
            )
        except KeyError as e:
            raise ValueError(f"Dataset is missing required column: {e}")

        # Group and create chunks
        quadrants = {}
        for (lat_idx, lon_idx), group in dataset.groupby(
            [lat_quadrants, lon_quadrants]
        ):
            lat_min = lat_idx * chunk_size
            lat_max = lat_min + chunk_size
            lon_min = lon_idx * chunk_size
            lon_max = lon_min + chunk_size
            quadrant_key = (lat_min, lat_max, lon_min, lon_max)
            quadrants[quadrant_key] = group.reset_index(drop=True)

        return quadrants

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
    
            # Perform consolidation using the provided parallel engine
            self._execute_consolidation(cons_plan, parallel_engine)
    
            # Consolidate and vacuum metadata, fragment metadata, and commit logs
            self._consolidate_and_vacuum("array_meta")
            self._consolidate_and_vacuum("fragment_meta")
            self._consolidate_and_vacuum("commits")
    
            logger.info(f"Consolidation complete for array: {self.array_uri}")
    
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

    def _execute_consolidation(self, cons_plan, parallel_engine: Optional[object] = None):
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
                capacity=self.config.get("tiledb", {}).get("capacity", 10000),
                cell_order=self.config.get("tiledb", {}).get("cell_order", "hilbert"),
            )
            tiledb.Array.create(uri, schema, ctx=self.ctx)
            logger.info(f"Successfully created TileDB array at {uri}")
        except ValueError as e:
            logger.error(f"Failed to create array: {e}")
            raise

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
        time_range = self.config.get("tiledb", {}).get("time_range", {})
        lat_min, lat_max = spatial_range.get("lat_min"), spatial_range.get("lat_max")
        lon_min, lon_max = spatial_range.get("lon_min"), spatial_range.get("lon_max")
        time_min = _datetime_to_timestamp_days(time_range.get("start_time"))
        time_max = _datetime_to_timestamp_days(time_range.get("end_time"))

        # Validate ranges
        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError(
                "Spatial and temporal ranges must be fully specified in the configuration."
            )
        if lat_min >= lat_max or lon_min >= lon_max:
            raise ValueError(
                "Invalid spatial range: lat_min must be less than lat_max and lon_min less than lon_max."
            )
        if time_min >= time_max:
            raise ValueError("Invalid time range: time_min must be less than time_max.")

        # Define dimensions
        dimensions = [
            tiledb.Dim(
                "latitude",
                domain=(lat_min, lat_max),
                tile=self.config.get("tiledb", {}).get("latitude_tile", 1),
                dtype="float64",
            ),
            tiledb.Dim(
                "longitude",
                domain=(lon_min, lon_max),
                tile=self.config.get("tiledb", {}).get("longitude_tile", 1),
                dtype="float64",
            ),
            tiledb.Dim(
                "time",
                domain=(time_min, time_max),
                tile=self.config.get("tiledb", {}).get("time_tile", 1825),
                dtype="int64",
            ),
        ]

        return tiledb.Domain(*dimensions)

    def _create_attributes(self) -> List[tiledb.Attr]:
        """
        Creates TileDB attributes for GEDI data based on configuration.

        Returns:
        --------
        List[tiledb.Attr]
            A list of TileDB attributes configured with appropriate data types.

        Notes:
        ------
        - The `timestamp_ns` attribute is always added to the array.
        """
        attributes = []
        if not self.variables_config:
            raise ValueError(
                "Variable configuration is missing. Cannot create attributes."
            )

        # Add scalar variables
        for var_name, var_info in self.variables_config.items():
            if not var_info.get("is_profile", False):
                attributes.append(tiledb.Attr(name=var_name, dtype=var_info["dtype"]))

        # Add profile variables
        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                profile_length = var_info.get("profile_length", 1)
                for i in range(profile_length):
                    attr_name = f"{var_name}_{i + 1}"
                    attributes.append(
                        tiledb.Attr(name=attr_name, dtype=var_info["dtype"])
                    )

        # Add timestamp attribute
        attributes.append(tiledb.Attr(name="timestamp_ns", dtype="int64"))

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

    @retry((tiledb.TileDBError, ConnectionError), tries=10, delay=5, backoff=3, logger=logger)
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

    def write_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write the parsed GEDI granule data to the global TileDB arrays.

        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data, with variable names matching the configuration.

        Raises:
        -------
        ValueError
            If required dimension data or critical variables are missing.
        """
        try:
            # Validate granule data
            self._validate_granule_data(granule_data)

            # Prepare coordinates (dimensions)
            coords = self._prepare_coordinates(granule_data)

            # Extract data for scalar and profile variables
            data = self._extract_variable_data(granule_data)

            # Write to the TileDB array
            self._write_to_tiledb(coords, data)

        except Exception as e:
            logger.error(f"Failed to process and write granule data: {e}")
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
        # Check for required dimensions
        missing_dims = [
            dim
            for dim in self.config["tiledb"]["dimensions"]
            if dim not in granule_data
        ]
        if missing_dims:
            raise ValueError(
                f"Granule data is missing required dimensions: {missing_dims}"
            )

    def _prepare_coordinates(self, granule_data: pd.DataFrame) -> Dict[str, np.ndarray]:
        """
        Prepare coordinate data for dimensions based on the granule DataFrame.

        Parameters:
        ----------
        granule_data : pd.DataFrame
            The DataFrame containing granule data.

        Returns:
        --------
        Dict[str, np.ndarray]
            A dictionary of coordinate arrays for each dimension.
        """
        return {
            dim_name: (
                convert_to_days_since_epoch(granule_data[dim_name].values)
                if dim_name == "time"
                else granule_data[dim_name].values
            )
            for dim_name in self.config["tiledb"]["dimensions"]
        }

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

        # Process scalar variables
        for var_name, var_info in self.variables_config.items():
            if not var_info.get("is_profile", False):
                if var_name in granule_data:
                    data[var_name] = granule_data[var_name].values

        # Process profile variables
        for var_name, var_info in self.variables_config.items():
            if var_info.get("is_profile", False):
                profile_length = var_info.get("profile_length", 1)
                for i in range(profile_length):
                    expanded_var_name = f"{var_name}_{i + 1}"
                    if expanded_var_name in granule_data:
                        data[expanded_var_name] = granule_data[expanded_var_name].values

        # Add timestamp
        data["timestamp_ns"] = (
            pd.to_datetime(granule_data["time"]).astype("int64") // 1000
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
            # Open scalar array and check metadata for granule statuses
            with tiledb.open(self.array_uri, mode="r", ctx=self.ctx) as scalar_array:
                scalar_metadata = {
                    key: scalar_array.meta[key]
                    for key in scalar_array.meta.keys()
                    if "_status" in key
                }

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
                scalar_array.meta[f"granule_{granule_key}_processed_date"] = (
                    pd.Timestamp.utcnow().isoformat()
                )

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
        for level in ["level_2a", "level_2b", "level_4a", "level_4c"]:
            level_vars = config.get(level, {}).get("variables", {})
            for var_name, var_info in level_vars.items():
                variables_config[var_name] = var_info

        return variables_config
