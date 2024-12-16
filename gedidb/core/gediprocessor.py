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
from dask.distributed import Client, LocalCluster
import concurrent.futures
import pandas as pd
from typing import Optional

from gedidb.utils.constants import GediProduct
from gedidb.downloader.data_downloader import H5FileDownloader, CMRDataDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import check_and_format_shape, _temporal_tiling
from gedidb.core.gedigranule import GEDIGranule

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
    def __init__(self, config_file: str, credentials:Optional[dict]= None, dask_client: Client = None, n_workers: int = None, memory_limit = '8GB'):
        """
        Initialize the GEDIProcessor with configuration files and prepare the necessary components.
        """
        # Initialize Dask client
        self.dask_client = dask_client or self._initialize_dask_client(n_workers=n_workers, memory_limit = memory_limit)

        # Load configurations and setup paths and components
        self.data_info = self._load_yaml_file(config_file)
        self._setup_paths_and_dates()
        self.credentials = credentials

        # Initialize database writer
        self.database_writer = self._initialize_database_writer(credentials)

        # Create the database schema
        self.database_writer._create_arrays()

    def _initialize_database_writer(self, credentials:Optional[dict]):
        """
        Initialize and return the GEDIDatabase instance.
        """
        return GEDIDatabase(
            config=self.data_info, credentials=credentials
        )

    def _initialize_dask_client(self, n_workers: int = None, memory_limit: str = '8GB') -> Client:
        """Initialize and return a Dask client with a LocalCluster and adaptive scaling."""

        # Set Dask memory spill and memory limits via configuration
        dask.config.set({
            "distributed.worker.memory.target": 0.6,     # Spill to disk at 60% memory usage
            "distributed.worker.memory.spill": 0.7,      # More aggressive spilling at 70%
            "distributed.worker.memory.pause": 0.8,      # Pause new task scheduling at 80%
            "distributed.worker.memory.terminate": 0.9,  # Terminate worker if memory exceeds 90%
        })

        # Setup a LocalCluster with better memory management configurations
        cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=1,
            processes=True,
            memory_limit=memory_limit,
            dashboard_address=None
        )

        # Initialize the Dask client with the cluster
        client = Client(cluster)
        logger.info(f"Dask dashboard available at {client.dashboard_link}")

        return client

    def _setup_paths_and_dates(self):
        """Set up paths and date information from the configuration."""
        self.download_path = self._ensure_directory(os.path.join(self.data_info['data_dir'], 'download'))

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

    @log_execution(start_message="Processing requested granules...", end_message="Granules successfully processed")
    def compute(self, consolidate:bool=True):
        """
       Main method to download and process GEDI granules.

       Parameters:
       ----------
       consolidate : bool, default=True
           If True, consolidates fragments in the TileDB arrays after processing all granules.
       """
       # Download and filter CMR data
        cmr_data = self._download_cmr_data()
        unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

        if not unprocessed_cmr_data:
            if consolidate:
                self.database_writer.consolidate_fragments()
            logger.info("All requested granules are already processed. No further computation needed.")
            return

        self._process_granules(unprocessed_cmr_data)

        if consolidate:
            self.database_writer.consolidate_fragments()

    def _download_cmr_data(self) -> pd.DataFrame:
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(self.geom, self.start_date, self.end_date, self.data_info['earth_data_info'])
        return downloader.download()

    def _filter_unprocessed_granules(self, cmr_data: dict) -> dict:
        """
        Filter out granules that have already been processed.

        Parameters:
        ----------
        cmr_data : dict
            Dictionary of granule metadata from CMR API, with granule IDs as keys.

        Returns:
        --------
        dict
            A dictionary of unprocessed granules from the input `cmr_data`.
        """
        granule_ids = list(cmr_data.keys())
        processed_granules = self.database_writer.check_granules_status(granule_ids)

        # Filter to include only granules that have not been processed
        unprocessed_granules = {
            granule_id: product_info
            for granule_id, product_info in cmr_data.items()
            if not processed_granules.get(granule_id, False)  # Keep if not processed
        }

        return unprocessed_granules

    def _process_granules(self, unprocessed_cmr_data: dict):
        """
        Process unprocessed granules in parallel using Dask, including writing to the database.
        """
        client = self.dask_client

        # Add temporal tiling for unprocessed granules
        unprocessed_temporal_cmr_data = _temporal_tiling(unprocessed_cmr_data, self.data_info['tiledb']["temporal_tiling"])

        for timeframe, granules in unprocessed_temporal_cmr_data.items():
            futures = []
            granule_ids = []

            for granule_id, product_info in granules.items():
                # Submit granule processing task
                future = client.submit(
                    self.process_granule,
                    granule_id,
                    product_info,
                    self.data_info,
                    self.download_path
                )
                futures.append(future)
                granule_ids.append(granule_id)  # Track granule IDs for marking later

            # Gather processed granule data
            granule_data = client.gather(futures)

            # Collect valid data for writing
            valid_dataframes = [gdf for _, gdf in granule_data if gdf is not None]

            # Proceed only if there is valid data
            if valid_dataframes:
                concatenated_df = pd.concat(valid_dataframes, ignore_index=True)

                # Sort data into quadrants for spatial processing
                quadrants = self.database_writer.spatial_chunking(
                    concatenated_df, chunk_size=self.data_info['tiledb']["chunk_size"]
                )

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [executor.submit(self.process_quadrant, key, value) for key, value in quadrants.items()]
                    concurrent.futures.wait(futures)

                # Mark all granules as processed
                for granule_id in granule_ids:
                    self.database_writer.mark_granule_as_processed(granule_id)

    @staticmethod
    def process_granule(
        granule_id,
        product_info,
        data_info,
        download_path
    ):
        """
        Processes a single granule by downloading, processing, and writing to the database.
        """

        # Download products
        downloader = H5FileDownloader(download_path)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(
                    downloader.download, granule_id, url, GediProduct(product)
                )
                for url, product, _ in product_info
            ]
            download_results = [f.result() for f in futures]

        # Process granule
        granule_processor = GEDIGranule(download_path, data_info)
        return granule_processor.process_granule(download_results)

    def process_quadrant(self, key, value):
        try:
            # print the key and value and timestamp
            self.database_writer.write_granule(value)
        except Exception as e:
            # Log the error
            logger.error(f"Error writing granule for quadrant {key}: {e}")

            # Create a debug directory if it doesn't exist
            debug_dir = os.path.join(self.data_info["debug_dir"], "failed_granules")
            os.makedirs(debug_dir, exist_ok=True)

            # Save the problematic DataFrame for debugging
            debug_file = os.path.join(debug_dir, f"failed_quadrant_{key}.csv")
            value.to_csv(debug_file, index=False)


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



