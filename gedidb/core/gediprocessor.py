import os
import logging
import yaml
import geopandas as gpd
from datetime import datetime
from collections import defaultdict
import dask
from dask.delayed import delayed
from dask.distributed import Client
import numpy as np

from gedidb.utils.constants import GediProduct
from gedidb.downloader.data_downloader import H5FileDownloader, CMRDataDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import check_and_format_shape
from gedidb.core.gedimetadata import GEDIMetadataManager
from gedidb.core.gedigranule import GEDIGranule

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GEDIProcessor(GEDIDatabase):
    
    def __init__(self, data_config_file: str, sql_config_file: str):
        """
        Initialize the GEDIProcessor with configuration files and prepare the necessary paths and metadata handler.
        
        :param data_config_file: YAML configuration file for data settings.
        :param sql_config_file: SQL file containing table creation queries.
        """
        # Load configurations
        self.data_info = self._load_yaml_file(data_config_file)
        self.sql_script = self._load_sql_file(sql_config_file).format(**self._get_table_names())
        self.metadata_info = self.data_info['earth_data_info']['METADATA_INFORMATION']
        
        # Set up paths, dates, and metadata handler
        self._setup_paths_and_dates()
        self.metadata_handler = GEDIMetadataManager(metadata_info = self.metadata_info, metadata_path = self.metadata_path, 
                                                    data_table_name = self.data_info['database']['tables']['shots'])
        self.granule_processor = GEDIGranule(self.db_path, self.download_path, self.parquet_path, self.data_info)

        # Initialize the database connection via superclass
        super().__init__(
            db_path=self.db_path,
            sql_script=self.sql_script,
            tables=self.data_info['database']['tables'],
            metadata_handler=self.metadata_handler
        )
    
    def _get_table_names(self):
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
    def _ensure_directory(path):
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

    def compute(self):
        """Main method to download and process GEDI granules, no arguments needed."""
        
        # Create the database schema
        self._create_db()

        # Download and filter CMR data
        cmr_data = self._download_cmr_data()
        unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

        if unprocessed_cmr_data.empty:
            logger.info("All requested granules are already processed. No further computation needed.")
            return
        
        # Process the granules with Dask
        self._process_granules(unprocessed_cmr_data)

    def _download_cmr_data(self):
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(self.geom, self.start_date, self.end_date, self.data_info['earth_data_info'])
        return downloader.download()
    
    def _filter_unprocessed_granules(self, cmr_data):
        """Filter out granules that have already been processed."""
        granule_ids = np.unique(cmr_data["id"]).tolist()
        processed_granules = self.granule_processor.get_processed_granules(granule_ids)
        return cmr_data[~cmr_data["id"].isin(processed_granules)]

    @delayed
    def download_granule(self, granule_id, url, product, downloader):
        """Download a granule using the H5FileDownloader."""
        return downloader.download(granule_id, url, GediProduct(product))

    @delayed
    def process_granule(self, granule_id, download_futures):
        """Process the granule after all the products have been downloaded."""
        return self.granule_processor.process_granule(download_futures)

    @delayed
    def write_to_db(self, result):
        """Write the processed granule to the database."""
        return self._write_db(result)

    def _process_granules(self, unprocessed_cmr_data, n_workers=5):
        """Process unprocessed granules in parallel using Dask with controlled worker limits."""
        
        # Start a Dask client with the specified number of workers
        with Client(n_workers=n_workers) as client:
            
            # Extract necessary data from the unprocessed CMR data
            name_url = unprocessed_cmr_data[["id", "name", "url", "product"]].to_records(index=False)
            
            # Group the products by granule_id
            granules = defaultdict(list)
            for granule in name_url:
                granule_id, name, url, product = granule
                granules[granule_id].append((url, product))
            
            # Create a downloader instance
            downloader = H5FileDownloader(self.download_path)
            
            # List to store Dask delayed tasks
            futures = []
            
            # Loop through grouped granules (grouped by granule_id)
            for granule_id, product_info in granules.items():
                
                # Create download tasks for all products of this granule
                download_futures = [
                    self.download_granule(granule_id, url, product, downloader)
                    for url, product in product_info
                ]
                
                # Group the download futures into a single list for this granule
                download_futures_combined = delayed(lambda *args: args)(*download_futures)
                
                # Process all products for this granule together once they are downloaded
                process_future = self.process_granule(granule_id, download_futures_combined)
                
                # Append the final task (download + processing) to the futures list
                futures.append(process_future)
            
            # Trigger execution with Dask compute and run all tasks with n_workers workers
            results = dask.compute(*futures)
    
            # Write results to the database in parallel (post-processing)
            write_futures = [self.write_to_db(result) for result in results if result is not None]
            
            dask.compute(*write_futures)
