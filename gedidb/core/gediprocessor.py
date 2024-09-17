# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import logging
import yaml
import geopandas as gpd
from datetime import datetime
from collections import defaultdict
import dask
from dask.delayed import delayed
from dask.distributed import Client
from dask.diagnostics import ProgressBar
import numpy as np
from functools import wraps
import pandas as pd

from gedidb.utils.constants import GediProduct
from gedidb.downloader.data_downloader import H5FileDownloader, CMRDataDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import check_and_format_shape
from gedidb.core.gedigranule import GEDIGranule
from gedidb.core.gedimetadata import GEDIMetadataManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def log_execution(start_message=None, end_message=None):
    """
    A decorator to log the execution of a method.
    
    :param start_message: Custom start message for logging.
    :param end_message: Custom end message for logging.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_message = start_message or f"Executing {func.__name__}..."
            logger.info(log_message)
            result = func(*args, **kwargs)
            log_message = end_message or f"Finished {func.__name__}..."
            logger.info(log_message)
            return result
        return wrapper
    return decorator

class GEDIProcessor:
    """
    GEDIProcessor class is responsible for processing GEDI granules, handling metadata, 
    and writing data into the database.
    
    Attributes:
    ----------
    dask_client : Client
        Dask client for parallel processing.
    data_info : dict
        Configuration data loaded from the YAML file.
    sql_script : str
        SQL script to create database tables.
    metadata_handler : GEDIMetadataManager
        Handler for managing metadata operations.
    database_writer : GEDIDatabase
        Database writer for managing database operations.
    """

    def __init__(self, data_config_file: str, sql_config_file: str, n_workers: int = None, dask_client: Client = None):
        """
        Initialize the GEDIProcessor with configuration files and prepare the necessary components.

        :param data_config_file: YAML configuration file for data settings.
        :param sql_config_file: SQL file containing table creation queries.
        :param n_workers: Number of Dask workers. Default is None, which uses the default worker count.
        :param dask_client: Existing Dask client, or a new one will be created if None.
        """
        self.dask_client = dask_client or self._initialize_dask_client(n_workers=n_workers)

        # Load configurations and setup paths and components
        self.data_info = self._load_yaml_file(data_config_file)
        self.sql_script = self._load_sql_file(sql_config_file).format(**self._get_table_names())
        self._setup_paths_and_dates()

        # Initialize GEDI granule processor, metadata handler, and database writer
        self.granule_processor = GEDIGranule(self.db_path, self.download_path, self.parquet_path, self.data_info)
        self.metadata_handler = self._initialize_metadata_handler()
        self.metadata_handler.extract_all_metadata()
        self.database_writer = self._initialize_database_writer()

    def _initialize_dask_client(self, n_workers: int) -> Client:
        """Initialize and return a Dask client."""
        return Client(n_workers=n_workers)

    def _initialize_metadata_handler(self) -> GEDIMetadataManager:
        """Initialize and return the metadata handler."""
        return GEDIMetadataManager(
            metadata_info=self.data_info['earth_data_info']['METADATA_INFORMATION'],
            metadata_path=self.metadata_path,
            data_table_name=self.data_info['database']['tables']['shots']
        )

    def _initialize_database_writer(self) -> GEDIDatabase:
        """Initialize and return the database writer."""
        return GEDIDatabase(
            db_path=self.db_path,
            sql_script=self.sql_script,
            tables=self.data_info['database']['tables'],
            metadata_handler=self.metadata_handler
        )

    def _get_table_names(self) -> dict:
        """Return formatted table names for SQL queries."""
        return {
            "DEFAULT_GRANULE_TABLE": self.data_info["database"]["tables"]["granules"],
            "DEFAULT_SHOT_TABLE": self.data_info["database"]["tables"]["shots"],
            "DEFAULT_METADATA_TABLE": self.data_info["database"]["tables"]["metadata"],
            "DEFAULT_SCHEMA": self.data_info["database"]["schema"]
        }

    def _setup_paths_and_dates(self):
        """Set up paths and date information from the configuration."""
        self.download_path = self._ensure_directory(os.path.join(self.data_info['data_dir'], 'download'))
        self.parquet_path = self._ensure_directory(os.path.join(self.data_info['data_dir'], 'parquet'))
        self.metadata_path = self._ensure_directory(os.path.join(self.data_info['data_dir'], 'metadata'))
        self.db_path = self.data_info['database']['url']
        
        # Load and format region of interest geometry
        initial_geom = gpd.read_file(self.data_info['region_of_interest'])
        self.geom = check_and_format_shape(initial_geom, simplify=True)
        
        # Parse dates
        self.start_date = datetime.strptime(self.data_info['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.data_info['end_date'], '%Y-%m-%d')

    @staticmethod
    def _ensure_directory(path: str) -> str:
        """Ensure a directory exists and return its path."""
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _load_yaml_file(file_path: str) -> dict:
        """Load a YAML configuration file."""
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
        
    @staticmethod
    def _load_sql_file(file_path: str) -> str:
        """Load an SQL file containing table creation scripts."""
        with open(file_path, 'r') as file:
            return file.read()
    
    @log_execution(start_message="Processing requested granules...", end_message="Granules successfully processed")
    def compute(self):
        """Main method to download and process GEDI granules."""
        # Create the database schema
        self.database_writer._create_db()

        # Download and filter CMR data
        cmr_data = self._download_cmr_data()
        unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

        if unprocessed_cmr_data.empty:
            logger.info("All requested granules are already processed. No further computation needed.")
            return
        
        # Process the granules with Dask
        self._process_granules(unprocessed_cmr_data)

    def _download_cmr_data(self) -> pd.DataFrame:
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(self.geom, self.start_date, self.end_date, self.data_info['earth_data_info'])
        return downloader.download()
    
    def _filter_unprocessed_granules(self, cmr_data: pd.DataFrame) -> pd.DataFrame:
        """Filter out granules that have already been processed."""
        granule_ids = np.unique(cmr_data["id"]).tolist()
        processed_granules = self.granule_processor.get_processed_granules(granule_ids)
        return cmr_data[~cmr_data["id"].isin(processed_granules)]

    def _process_granules(self, unprocessed_cmr_data: pd.DataFrame):
        """
        Process unprocessed granules in parallel using Dask, including writing to the database.
        """
        name_url = unprocessed_cmr_data[["id", "name", "url", "product"]].to_records(index=False)
        granules = defaultdict(list)
        
        for granule in name_url:
            granule_id, name, url, product = granule
            granules[granule_id].append((url, product))
    
        downloader = H5FileDownloader(self.download_path)
        futures = []
    
        for granule_id, product_info in granules.items():
            # Create delayed tasks for downloading granules
            download_futures = []
            for url, product in product_info:
                download_futures.append(delayed(downloader.download)(granule_id, url, GediProduct(product)))
            
            download_futures_combined = delayed(list)(download_futures)
    
            # Log processing start
            logger.info(f"Processing granule: {granule_id}")
            
            # Process the granule
            process_future = delayed(self.granule_processor.process_granule)(download_futures_combined)
            
            # Write to the database
            write_future = delayed(self.database_writer._write_db)(process_future)
            
            # Append the combined process and write future to the list
            futures.append(write_future)
    
        # Compute the entire workflow with a single Dask computation and progress bar
        with ProgressBar():
            dask.compute(*futures)
        