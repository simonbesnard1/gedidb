import os
import logging
import yaml
import geopandas as gpd
from datetime import datetime
from functools import wraps
import numpy as np
from pyspark.sql import SparkSession
import dask
from dask import delayed
from dask.distributed import progress

from gedidb.utils.constants import GediProduct
from gedidb.downloader.data_downloader import H5FileDownloader, CMRDataDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import check_and_format_shape
from gedidb.core.gedimetadata import GediMetadataManager
from gedidb.core.gedigranule import GEDIGranule

logger = logging.getLogger(__name__)

def log_execution(start_message=None, end_message=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger.info(start_message or f"Executing {func.__name__}...")
            result = func(*args, **kwargs)
            logger.info(end_message or f"Finished {func.__name__}...")
            return result
        return wrapper
    return decorator

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
        self.metadata_handler = GediMetadataManager(self.metadata_info, self.metadata_path, self.data_info)
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

    @log_execution(start_message="Starting computation process...", end_message='Data processing completed!')
    def compute(self):
        """Main method to download and process GEDI granules, no arguments needed."""
        
        self._create_db()

        cmr_data = self._download_cmr_data()

        unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)
        if unprocessed_cmr_data.empty:
            logger.info("All requested granules are already processed. No further computation needed.")
            return
        
        self._process_granules_with_dask(unprocessed_cmr_data)

    def _download_cmr_data(self):
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(self.geom, self.start_date, self.end_date, self.data_info['earth_data_info'])
        return downloader.download()
    
    def _filter_unprocessed_granules(self, cmr_data):
        """Filter out granules that have already been processed."""
        granule_ids = np.unique(cmr_data["id"]).tolist()
        processed_granules = self.granule_processor.get_processed_granules(granule_ids)
        return cmr_data[~cmr_data["id"].isin(processed_granules)]
    
    def _process_granules_with_dask(self, unprocessed_cmr_data):
        """Process unprocessed granules in parallel using Dask."""
        
        # Extract necessary data from the unprocessed CMR data
        name_url = unprocessed_cmr_data[["id", "name", "url", "product"]].to_records(index=False)
    
        # Create Dask delayed tasks for downloading granules
        downloader = H5FileDownloader(self.download_path)
        
        # List to store Dask delayed tasks
        futures = []
        
        # Loop through granules and create delayed tasks
        for granule in name_url:
            granule_id, name, url, product = granule
            
            # Delayed download task (returns list of (product, file) tuples)
            download_future = delayed(downloader.download)(granule_id, url, GediProduct(product))
            
            # Delayed processing task, chained to the download
            process_future = delayed(self.granule_processor.process_granule)(download_future)
            
            # Append the final task (download + processing) to the futures list
            futures.append(process_future)
    
        # Trigger execution with Dask compute and run all tasks in parallel
        results = dask.compute(*futures)
    
        # Optional: Show progress of the Dask computation
        progress(results)
    
        # Write results to the database in parallel (post-processing)
        write_futures = [delayed(self._write_db)(result) for result in results if result is not None]
        dask.compute(*write_futures)
        
    def _process_granules_with_spark(self, unprocessed_cmr_data):
        """Process unprocessed granules in parallel using Spark."""
        
        spark = self._create_spark_session()

        name_url = unprocessed_cmr_data[["id", "name", "url", "product"]].to_records(index=False)
        urls = spark.sparkContext.parallelize(name_url)
        
        downloader = H5FileDownloader(self.download_path)
        mapped_urls = urls.map(lambda x: downloader.download(x[0], x[2], GediProduct(x[3]))).groupByKey()

        processed_granules = mapped_urls.map(self.granule_processor.process_granule).filter(lambda x: x is not None)
        
        # Write results to the database
        granule_entries = processed_granules.coalesce(8).map(self._write_db)
        granule_entries.count()

        spark.stop()

    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session."""
        return (SparkSession.builder
                .appName("GEDI Processing")
                .config("spark.executor.instances", "4")
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "2g")
                .getOrCreate())