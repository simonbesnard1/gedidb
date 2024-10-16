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
import dask
from dask.distributed import Client, LocalCluster, as_completed
import concurrent.futures
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
    """
    def decorator(func):
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
    """
    def __init__(self, data_config_file: str, sql_config_file: str, dask_client: Client = None, n_workers: int = None, memory_limit = '8GB'):
        """
        Initialize the GEDIProcessor with configuration files and prepare the necessary components.
        """
        # Initialize Dask client
        self.dask_client = dask_client or self._initialize_dask_client(n_workers=n_workers, memory_limit = memory_limit)

        # Load configurations and setup paths and components
        self.data_info = self._load_yaml_file(data_config_file)
        self.sql_script = self._load_sql_file(sql_config_file).format(**self._get_table_names())
        self._setup_paths_and_dates()
        
        # Initialize metadata handler and database writer
        self.metadata_handler = self._initialize_metadata_handler()
        self.database_writer = self._initialize_database_writer()

    def _initialize_metadata_handler(self):
        """
        Initialize and return the GEDIMetadataManager instance.
        """
        metadata_path = os.path.join(self.data_info['data_dir'], 'metadata')
        metadata_handler = GEDIMetadataManager(
            metadata_info=self.data_info['earth_data_info']['METADATA_INFORMATION'],
            metadata_path=metadata_path,
            data_table_name=self.data_info['database']['tables']['shots']
        )
        metadata_handler.extract_all_metadata()
        return metadata_handler

    def _initialize_database_writer(self):
        """
        Initialize and return the GEDIDatabase instance.
        """
        return GEDIDatabase(
            db_path=self.db_path,
            sql_script=self.sql_script,
            tables=self.data_info['database']['tables'],
            metadata_handler=self.metadata_handler
        )

    def _initialize_dask_client(self, n_workers: int = None, memory_limit:str = '8GB') -> Client:
        """Initialize and return a Dask client with a LocalCluster."""
        
        # Set Dask memory spill and memory limits via configuration
        dask.config.set({
            "distributed.worker.memory.target": 0.6,     # Spill to disk at 60% memory usage
            "distributed.worker.memory.spill": 0.7,      # More aggressive spilling at 70%
            "distributed.worker.memory.pause": 0.8,      # Pause new task scheduling at 80%
            "distributed.worker.memory.terminate": 0.9,  # Terminate worker if memory exceeds 90%
        })
            
        # Setup a LocalCluster with better memory management configurations
        cluster = LocalCluster(
            n_workers=n_workers,           # Number of workers (concurrent tasks)
            threads_per_worker=1,          # Each worker uses a single thread
            processes=True,                # Use processes instead of threads (better for CPU-bound tasks)
            memory_limit= memory_limit,    # Set a memory limit per worker, adjust based on system
            dashboard_address=':8787'      # Enable the Dask dashboard
        )
        
        # Initialize the Dask client with the cluster
        client = Client(cluster)
        
        # Log the Dask dashboard link for monitoring
        logger.info(f"Dask dashboard available at {client.dashboard_link}")
        
        return client


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
        """
        Main method to download and process GEDI granules.
        """
        # Initialize database components
        metadata_handler = GEDIMetadataManager(
            metadata_info=self.data_info['earth_data_info']['METADATA_INFORMATION'],
            metadata_path=self.metadata_path,
            data_table_name=self.data_info['database']['tables']['shots']
        )
        metadata_handler.extract_all_metadata()
        database_writer = GEDIDatabase(
            db_path=self.db_path,
            sql_script=self.sql_script,
            tables=self.data_info['database']['tables'],
            metadata_handler=metadata_handler
        )
        # Create the database schema
        database_writer._create_db()

        # Download and filter CMR data
        cmr_data = self._download_cmr_data()
        unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

        if not unprocessed_cmr_data:
            logger.info("All requested granules are already processed. No further computation needed.")
            return

        # Process the granules with Dask
        self._process_granules(unprocessed_cmr_data)

    def _download_cmr_data(self) -> pd.DataFrame:
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(self.geom, self.start_date, self.end_date, self.data_info['earth_data_info'])
        return downloader.download()
    
    def _filter_unprocessed_granules(self, cmr_data: dict) -> dict:
        """
        Filter out granules that have already been processed.
        """
        granule_ids = cmr_data.keys()
        granule_processor = GEDIGranule(self.db_path, self.download_path, self.parquet_path, self.data_info)
        processed_granules = granule_processor.get_processed_granules(granule_ids)
        
        # Filter the cmr_data dictionary, keeping only granules that are not in processed_granules
        unprocessed_granules = {granule_id: product_info for granule_id, product_info in cmr_data.items() if granule_id not in processed_granules}
        return unprocessed_granules

    def _process_granules(self, unprocessed_cmr_data: dict):
        """
        Process unprocessed granules in parallel using Dask, including writing to the database.
        """
        client = self.dask_client
        futures = []

        for granule_id, product_info in unprocessed_cmr_data.items():
            # Submit the task to process one granule
            future = client.submit(
                self.process_granule,
                self.database_writer,
                granule_id,
                product_info,
                self.data_info,
                self.download_path,
                self.parquet_path,
                self.db_path
            )
            futures.append(future)

        # Use `as_completed` to process futures as they finish
        for completed_future in as_completed(futures):
            try:
                _ = completed_future.result()  # Wait for the result
            except Exception as e:
                logger.error(f"Error processing granule: {e}")
                
    @staticmethod
    def process_granule(
        database_writer,
        granule_id,
        product_info,
        data_info,
        download_path,
        parquet_path,
        db_path
    ):
        """
        Processes a single granule by downloading, processing, and writing to the database.
        """
        # Download products
        downloader = H5FileDownloader(download_path)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    downloader.download, granule_id, url, GediProduct(product), parquet_path
                )
                for url, product in product_info
            ]
            download_results = [f.result() for f in futures]

        # Process granule
        granule_processor = GEDIGranule(db_path, download_path, parquet_path, data_info)
        process_results = granule_processor.process_granule(download_results)

        # Write to database
        database_writer._write_db(process_results)

    def close(self):
        """Close the Dask client and cluster."""
        if self.dask_client:
            self.dask_client.close()
            self.dask_client = None
            logger.info("Dask client and cluster have been closed.")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close resources."""
        self.close()
