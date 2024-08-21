# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import concurrent.futures
import logging
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import pandas as pd
import yaml
from dask.distributed import Client

from gedidb.core.gedidatabase import GEDIDatabase
<<<<<<< HEAD
from gedidb.core.gedigranule import GEDIGranule
from gedidb.downloader.authentication import EarthDataAuthenticator
from gedidb.downloader.data_downloader import CMRDataDownloader, H5FileDownloader
from gedidb.utils.constants import GediProduct
from gedidb.utils.geo_processing import _temporal_tiling, check_and_format_shape

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("distributed").setLevel(logging.WARNING)
logging.getLogger("tornado").setLevel(logging.WARNING)
logger = logging.getLogger()


class GEDIProcessor:
    """
    GEDIProcessor class is responsible for processing GEDI granules, handling metadata,
    and writing data into the database.
    """

    def __init__(
        self,
        geometry: Union[gpd.GeoDataFrame, str] = None,
        start_date: str = None,
        end_date: str = None,
        config_file: str = None,
        earth_data_dir: str = None,
        credentials: Optional[dict] = None,
        parallel_engine: Optional[object] = None,
        log_dir: Optional[str] = None,
    ):
        """
        Initializes the GEDIProcessor.

        Parameters:
        -----------
        config_file : str
            Path to the configuration YAML file.
        earth_data_dir : str
            Directory containing EarthData credentials.
        credentials : dict, optional
            Credentials for accessing the database.
        parallel_engine : object, optional
            A parallelization engine such as `dask.distributed.Client` or
            `concurrent.futures.Executor`. Defaults to single-threaded.
        geometry : geopandas.GeoDataFrame, optional
            Geometry defining the region of interest.
        log_dir : str, optional
            Directory to store logs.
        """

        # Validate config_file
        if not config_file or not isinstance(config_file, str):
            raise ValueError(
                "The 'config_file' argument must be a valid, non-empty string pointing to the configuration file."
            )

        config_path = Path(config_file)
        if config_path.suffix.lower() != ".yml":
            raise ValueError(
                f"The configuration file must have a '.yml' extension. Provided: {config_file}"
            )
        if not config_path.exists():
            raise FileNotFoundError(
                f"The configuration file does not exist: {config_file}"
            )

        # Validate Earth data directory
        if not earth_data_dir or not isinstance(earth_data_dir, str):
            raise ValueError(
                "The 'earth_data_dir' argument must be a valid, non-empty string pointing to the directory where the Earthdata credentials are storred"
            )

        # Validate credentials
        if credentials is not None and not isinstance(credentials, dict):
            raise ValueError(
                "The 'credentials' argument must be a dictionary if provided."
            )

        # Validate parallel_engine
        if parallel_engine is not None and not (
            isinstance(parallel_engine, concurrent.futures.Executor)
            or isinstance(parallel_engine, Client)
        ):
            raise ValueError(
                "The 'parallel_engine' argument must be either a 'concurrent.futures.Executor', "
                "'dask.distributed.Client', or None."
            )

        # Validate log_dir
        if log_dir is not None and not isinstance(log_dir, str):
            raise ValueError("The 'log_dir' argument must be a string if provided.")

        # Validate geometry
        if geometry is None:
            raise ValueError("The 'geometry' parameter must be provided.")
        self.geom = self._validate_and_load_geometry(geometry)

        # Validate and parse dates
        if not start_date or not end_date:
            raise ValueError("Both 'start_date' and 'end_date' must be provided.")
        self.start_date = self._validate_and_parse_date(start_date, "start_date")
        self.end_date = self._validate_and_parse_date(end_date, "end_date")
        if self.start_date > self.end_date:
            raise ValueError(
                "'start_date' must be earlier than or equal to 'end_date'."
            )

        # Set up logging to file if log_dir is provided
        if log_dir:
            # Ensure the log directory exists
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(
                log_dir,
                f"gediprocessor_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            )

            # Create a FileHandler and set its level and format
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)

            # Add the FileHandler to the logger
            if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
                logger.addHandler(file_handler)

        # Load configurations and setup paths and components
        self.data_info = self._load_yaml_file(config_file)
        self.credentials = credentials

        # Validate Earthdata credentials directory
        earth_data_path = Path(earth_data_dir)
        if not earth_data_path.exists():
            raise FileNotFoundError(
                f"The specified Earth data credentials directory '{earth_data_dir}' does not exist. "
                "Please provide the correct directory or create the credentials using the EarthDataAuthenticator module."
            )

        # Validate Earthdata credentials in strict mode
        try:
            authenticator = EarthDataAuthenticator(
                earth_data_dir=earth_data_dir, strict=True
            )
            authenticator.authenticate()
        except FileNotFoundError as e:
            logger.error(e)
            raise

        # Initialize download_path
        self.download_path = self._ensure_directory(
            os.path.join(self.data_info["data_dir"], "download")
        )

        # Initialize database writer
        self.database_writer = self._initialize_database_writer(credentials)

        # Create the database schema
        self.database_writer._create_arrays()

        # Set the parallel engine
        self.parallel_engine = self._initialize_parallel_engine(parallel_engine)

    def _validate_and_load_geometry(self, geometry: object) -> gpd.GeoDataFrame:
        """
        Validates and loads the geometry from a file or GeoDataFrame.

        Parameters:
        ----------
        geometry : str or geopandas.GeoDataFrame
            Path to a GeoJSON file or a GeoDataFrame.

        Returns:
        --------
        geopandas.GeoDataFrame
            A validated and formatted GeoDataFrame.
        """
        if isinstance(geometry, gpd.GeoDataFrame):
            return check_and_format_shape(geometry, simplify=True)
        elif isinstance(geometry, str):
            if not os.path.exists(geometry):
                raise FileNotFoundError(f"Region file not found: {geometry}")
            gdf = gpd.read_file(geometry)
            return check_and_format_shape(gdf, simplify=True)
        else:
            raise ValueError(
                "Geometry must be a GeoDataFrame or a valid GeoJSON file path."
            )

    @staticmethod
    def _validate_and_parse_date(date_str: str, date_type: str) -> datetime:
        """
        Validates and parses a date string.

        Parameters:
        ----------
        date_str : str
            Date string in 'YYYY-MM-DD' format.
        date_type : str
            Type of the date being validated (e.g., 'start_date', 'end_date').

        Returns:
        --------
        datetime
            Parsed datetime object.
        """
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid format for {date_type}. Expected 'YYYY-MM-DD'.")

    def _initialize_database_writer(self, credentials: Optional[dict]):
        """
        Initialize and return the GEDIDatabase instance.
        """
        return GEDIDatabase(config=self.data_info, credentials=credentials)

    def _initialize_parallel_engine(self, parallel_engine: Optional[object]):
        """
        Initialize the parallel engine.

        Parameters:
        ----------
        parallel_engine : object, optional
            User-specified parallelization engine.

        Returns:
        --------
        object
            The initialized parallel engine. Defaults to single-threaded execution if no engine is provided.
        """
        if parallel_engine:
            logger.info("Using user-provided parallel engine.")
            return parallel_engine

        logger.info(
            "No parallel engine provided. Defaulting to single-threaded execution."
        )
        return concurrent.futures.ThreadPoolExecutor(max_workers=1)
=======
from gedidb.utils.geospatial_tools import ShapeProcessor


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
    
class GEDIGranuleProcessor(GEDIDatabase):
    
    def __init__(self, config_files: dict):
        self.load_all_configs(config_files)
        super().__init__(
            self.data_info['region_of_interest'], 
            self.data_info['start_date'], 
            self.data_info['end_date']
        )
        self.setup_paths_and_dates()
    
    def load_all_configs(self, config_files):
        """Load all configuration files."""
        self.data_info = self.load_config_file(config_files['data_info'])
        self.database_schema = self.load_config_file(config_files['database_schema'])

    def setup_paths_and_dates(self):
        """Set up paths and dates based on the configuration."""
        self.download_path = self.ensure_directory(self.data_info['download_path'])
        self.parquet_path = self.ensure_directory(self.data_info['parquet_path'])
        self.db_path = self.data_info['database_url']
        initial_geom = gpd.read_file(self.data_info['region_of_interest'])
        self.geom = ShapeProcessor(initial_geom).check_and_format(simplify=True)        
        self.start_date = datetime.strptime(self.data_info['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.data_info['end_date'], '%Y-%m-%d')
>>>>>>> progress config restructuring

    @staticmethod
    def _ensure_directory(path: str) -> str:
        """Ensure a directory exists and return its path."""
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _load_yaml_file(file_path: str) -> dict:
        """Load a YAML configuration file."""
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
<<<<<<< HEAD

    def compute(self, consolidate: bool = True, consolidation_type: str = "spatial"):
        """
        Main method to download and process GEDI granules.

        Parameters:
        ----------
        consolidate : bool, default=True
            If True, consolidates fragments in the TileDB arrays after processing all granules.
        consolidation_type : str, default='spatial'
            Type of consolidation to perform ('default' or 'spatial').
        """
=======
                
    @log_execution(start_message = "Starting computation process...", end_message='Data processing completed!')
    def compute(self):
        cmr_data = self.download_cmr_data().download()
        spark = self.create_spark_session()
    
        name_url = cmr_data[
            ["id", "name", "url", "product"]
        ].to_records(index=False)
    
        urls = spark.sparkContext.parallelize(name_url)
        downloader = H5FileDownloader(self.download_path)
    
        mapped_urls = urls.map(lambda x: downloader.download(x[0], x[2], GediProduct(x[3]))).groupByKey()
        processed_granules = mapped_urls.map(self._process_granule).filter(lambda x: x is not None)  # Filter out None values
        granule_entries = processed_granules.coalesce(8).map(self._write_db)
        granule_entries.count()
        spark.stop()

    def _process_granule(self, row: tuple[str, tuple[GediProduct, str]]):
        granule_key, granules = row
        outfile_path = self.get_output_path(granule_key)
    
        if os.path.exists(outfile_path):
            return self._prepare_return_value(granule_key, outfile_path, granules)
        
        gdf_dict = self._parse_granules(granules, granule_key)
        if not gdf_dict:
            logger.warning(f"Skipping granule {granule_key} due to missing or invalid data.")
            return None
    
        gdf = self._join_gdfs(gdf_dict)
        if gdf is None:
            logger.warning(f"Skipping granule {granule_key} due to issues during the join operation.")
            return None
    
        self.save_gdf_to_parquet(gdf, granule_key, outfile_path)
        return self._prepare_return_value(granule_key, outfile_path, granules)
    
    def get_output_path(self, granule_key):
        return os.path.join(self.parquet_path, f"filtered_granule_{granule_key}.parquet")
    
    def _prepare_return_value(self, granule_key, outfile_path, granules):
        return granule_key, outfile_path, sorted([fname[0] for fname in granules])
    
    def save_gdf_to_parquet(self, gdf, granule_key, outfile_path):
        gdf["granule"] = granule_key
        gdf.to_parquet(outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us")
    
    def _parse_granules(self, granules, granule_key):
        """Parse granules and handle None or invalid data."""
        gdf_dict = {}
        for product, file in granules:
            gdf = granule_parser.parse_h5_file(
                file, product, 
                quality_filter=self.quality_filter, 
                field_mapping=self.data_info['variables_selected'], 
                geom=self.geom
            )
            
            if gdf is not None:
                gdf = (gdf.rename(lambda x: f"{x}_{product}", axis=1)
                          .rename({f"shot_number_{product}": "shot_number"}, axis=1))
                gdf_dict[product] = gdf
            else:
                logging.info(f"Skipping product {product} for granule {granule_key} because parsing returned None.")
        
        # Validate GeoDataFrames
        valid_gdf_dict = {k: v for k, v in gdf_dict.items() if not v.empty and "shot_number" in v.columns}
        return valid_gdf_dict
    
    def _join_gdfs(self, gdf_dict):
        """Perform the join operations on the GeoDataFrames."""
>>>>>>> progress config restructuring
        try:

            # Download and filter CMR data
            cmr_data = self._download_cmr_data()
            unprocessed_cmr_data = self._filter_unprocessed_granules(cmr_data)

            if not unprocessed_cmr_data:
                logger.info("All requested granules are already processed.")
                if consolidate:
                    self.database_writer.consolidate_fragments(
                        consolidation_type=consolidation_type,
                        parallel_engine=None,
                    )
                return

            # Process unprocessed granules
            logger.info("Starting GEDI granules processing...")
            self._process_granules(unprocessed_cmr_data)

            # Consolidate fragments if required
            if consolidate:
                self.database_writer.consolidate_fragments(
                    consolidation_type=consolidation_type, parallel_engine=None
                )
<<<<<<< HEAD
            logger.info("GEDI granule processing completed successfully.")
        except Exception as e:
            # Log the exception with traceback
            logger.error("An error occurred: %s", e)
            logger.error("Traceback: %s", traceback.format_exc())
            raise
=======
            
            return (gdf.drop(
                        columns=[f"geometry_{GediProduct.L2A.value}", f"geometry_{GediProduct.L2B.value}", 
                                 f"geometry_{GediProduct.L4A.value}", f"geometry_{GediProduct.L4C.value}"])
                    .set_geometry("geometry_level1B")
                    .rename_geometry("geometry"))
        
        except KeyError as e:
            logging.error(f"Join operation failed due to missing product data: {e}")
            return None
        
    def _write_db(self, input):
        if input is None:
            return  # Early exit if input is None
    
        #field_to_column = {v: k for k, v in self.column_to_field.items()}    
        granule_key, outfile_path, included_files = input
        gedi_data = gpd.read_parquet(outfile_path)
        #gedi_data = gedi_data[list(field_to_column.keys())]
        #gedi_data = gedi_data.rename(columns=field_to_column)
        gedi_data = gedi_data.astype({"shot_number": "int64"})
        
        db_manager = DatabaseManager(db_url=self.db_path)
        
        # Ensure the database schema is correct and tables are created
        db_manager.create_tables()
        
        # Use the DatabaseManager to manage the connection and transaction
        engine = db_manager.get_connection()
        
        if engine:
            with engine.begin() as conn:
                self._write_granule_entry(conn, granule_key, outfile_path, included_files)
                self._write_gedi_data(conn, gedi_data)
                conn.commit()  # Note: In SQLAlchemy, committing is usually handled automatically with `begin()`
                del gedi_data
        else:
            print("Failed to create a database connection.")
    
            
    def _write_granule_entry(self, conn, granule_key, outfile_path, included_files):
        granule_entry = pd.DataFrame(
            data={
                "granule_name": [granule_key],
                "granule_file": [outfile_path],
                "l1b_file": [included_files[0]],
                "l2a_file": [included_files[1]],
                "l2b_file": [included_files[2]],
                "l4a_file": [included_files[3]],
                "l4c_file": [included_files[4]],
                "created_date": [pd.Timestamp.utcnow()],
            }
        )
        granule_entry.to_sql(
            name=self.database_schema['granules']['table_name'],
            con=conn,
            index=False,
            if_exists="append",
        )
>>>>>>> progress config restructuring

    def _download_cmr_data(self) -> pd.DataFrame:
        """Download the CMR metadata for the specified date range and region."""
        downloader = CMRDataDownloader(
            self.geom,
            self.start_date,
            self.end_date,
            self.data_info["earth_data_info"],
        )
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
        Process unprocessed granules in parallel using the selected parallelization engine.
        """
        # Check the temporal tiling configuration
        temporal_batching = self.data_info["tiledb"].get("temporal_batching", None)

        if temporal_batching == "daily" or temporal_batching == "weekly":
            # Apply temporal tiling based on the specified configuration
            unprocessed_temporal_cmr_data = _temporal_tiling(
                unprocessed_cmr_data, temporal_batching
            )
        elif temporal_batching is None:
            # No tiling, process all granules as a single batch
            unprocessed_temporal_cmr_data = {"all": unprocessed_cmr_data}
        else:
            # Raise an error for invalid temporal tiling options
            raise ValueError(
                f"Invalid temporal batching option: '{temporal_batching}'. "
                "It must be one of ['daily', 'weekly', None]."
            )

        if isinstance(self.parallel_engine, concurrent.futures.Executor):
            # Create the executor once
            with self.parallel_engine as executor:
                for (
                    timeframe,
                    granules,
                ) in unprocessed_temporal_cmr_data.items():
                    futures = [
                        executor.submit(
                            GEDIProcessor.process_single_granule,
                            granule_id,
                            product_info,
                            self.data_info,
                            self.download_path,
                        )
                        for granule_id, product_info in granules.items()
                    ]
                    results = [future.result() for future in futures]

                    # Collect valid data for writing
                    valid_dataframes = [gdf for _, gdf in results if gdf is not None]

                    if valid_dataframes:
                        concatenated_df = pd.concat(valid_dataframes, ignore_index=True)
                        quadrants = self.database_writer.spatial_chunking(
                            concatenated_df,
                            chunk_size=self.data_info["tiledb"]["chunk_size"],
                        )
                        for data in quadrants.values():
                            self.database_writer.write_granule(data)

                    # Collect processed granules
                    granule_ids = [ids_ for ids_, _ in results if ids_ is not None]
                    for granule_id in granule_ids:
                        self.database_writer.mark_granule_as_processed(granule_id)

        elif isinstance(self.parallel_engine, Client):
            for timeframe, granules in unprocessed_temporal_cmr_data.items():

                # Assume Dask client
                futures = [
                    self.parallel_engine.submit(
                        GEDIProcessor.process_single_granule,
                        granule_id,
                        product_info,
                        self.data_info,
                        self.download_path,
                    )
                    for granule_id, product_info in granules.items()
                ]
                results = self.parallel_engine.gather(futures)

                # Collect valid data for writing
                valid_dataframes = [gdf for _, gdf in results if gdf is not None]

                if valid_dataframes:
                    concatenated_df = pd.concat(valid_dataframes, ignore_index=True)
                    quadrants = self.database_writer.spatial_chunking(
                        concatenated_df,
                        chunk_size=self.data_info["tiledb"]["chunk_size"],
                    )
                    for data in quadrants.values():
                        self.database_writer.write_granule(data)

                # Collect processed granules
                granule_ids = [ids_ for ids_, _ in results if ids_ is not None]
                for granule_id in granule_ids:
                    self.database_writer.mark_granule_as_processed(granule_id)
        else:
            raise ValueError(
                "Unsupported parallel engine. Provide a 'concurrent.futures.Executor' or 'dask.distributed.Client'."
            )

    @staticmethod
    def process_single_granule(granule_id, product_info, data_info, download_path):
        """
        Processes a single granule by downloading and processing sequentially.

        Parameters:
        ----------
        granule_id : str
            ID of the granule to process.
        product_info : list
            List of tuples containing URL, product type, and additional information for the granule.
        data_info : dict
            Dictionary containing configuration and metadata.
        download_path : str
            Path to the directory where downloaded files are stored.

        Returns:
        --------
        tuple
            A tuple containing the granule ID and the processed granule data.
        """
        # Initialize the downloader
        downloader = H5FileDownloader(download_path)

        # Sequentially download each product
        download_results = []
        for url, product, _ in product_info:
            result = downloader.download(granule_id, url, GediProduct(product))
            download_results.append(result)

        # Process granule
        granule_processor = GEDIGranule(download_path, data_info)
        return granule_processor.process_granule(download_results)

    def close(self):
        """Close the parallelization engine if applicable."""
        if isinstance(self.parallel_engine, Client):
            # Close Dask client if it's the engine
            self.parallel_engine.close()
            self.parallel_engine = None
            logger.info("Dask client and cluster have been closed.")
        elif isinstance(self.parallel_engine, concurrent.futures.Executor):
            # Shutdown concurrent.futures executor if used
            self.parallel_engine.shutdown(wait=True)
            self.parallel_engine = None
            logger.info("ThreadPoolExecutor has been shut down.")
        else:
            logger.info("No parallel engine to close.")

    def __enter__(self):
        """Enter the runtime context related to this object."""
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context and close resources."""
        self.close()
