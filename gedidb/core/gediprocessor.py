import os
import logging
import yaml
import geopandas as gpd
import pandas as pd
from datetime import datetime
from functools import wraps
from sqlalchemy import text

from gedidb.utils.constants import GediProduct
from gedidb.database.db import DatabaseManager
from gedidb.processor import granule_parser
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.core.gedidatabase import GEDIDatabase
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

    @staticmethod
    def ensure_directory(path):
        """Ensure that a directory exists."""
        os.makedirs(path, exist_ok=True)
        return path
    
    @staticmethod
    def load_config_file(file_path: str = "field_mapping.yml") -> dict:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
                
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
                data_info=self.data_info
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
        try:
            gdf = gdf_dict[GediProduct.L2A.value]
            for product in [GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]:
                gdf = gdf.join(
                    gdf_dict[product.value].set_index("shot_number"),
                    on="shot_number",
                    how="inner",
                )
            
            return (gdf.drop(
                        columns=[f"geometry_{GediProduct.L2B.value}", f"geometry_{GediProduct.L4A.value}", f"geometry_{GediProduct.L4C.value}"])
                    .set_geometry("geometry_level2A")
                    .rename_geometry("geometry"))
        
        except KeyError as e:
            logging.error(f"Join operation failed due to missing product data: {e}")
            return None
        
    def _write_db(self, input):
        if input is None:
            return  # Early exit if input is None
    
        granule_key, outfile_path, included_files = input
        gedi_data = gpd.read_parquet(outfile_path)
        gedi_data = gedi_data.astype({"shot_number": "int64"})
    
        db_manager = DatabaseManager(db_url=self.db_path)
        
        # Ensure the database schema is correct and tables are created
        db_manager.create_tables()
        
        # Use the DatabaseManager to manage the connection and transaction
        engine = db_manager.get_connection()
    
        if engine:
            with engine.begin() as conn:
                # Retrieve or create the version ID
                version_id = self._get_or_create_version_id(conn)
    
                # Pass the version_id when writing the granule entry and GEDI data
                self._write_granule_entry(conn, granule_key, outfile_path, included_files, version_id)
                self._write_gedi_data(conn, gedi_data, version_id)
                conn.commit()
                del gedi_data
        else:
            print("Failed to create a database connection.")
    
    def _get_or_create_version_id(self, conn):
        """
        Retrieve the version_id for the current GEDI version or create it if it doesn't exist.
        """
        #TODO : need to be retrieve from the metadata or the granules h5 object.
        version = "2.0"
        release_date = datetime(2022, 5, 1)
        description = "GEDI Product Version 2"
    
        # Assuming `GediVersion` is the model class for the version table
        gedi_version = conn.execute(
            text("SELECT id FROM gedi_versions WHERE version = :version"),
            {"version": version}
        ).fetchone()
    
        if not gedi_version:
            conn.execute(
                text("INSERT INTO gedi_versions (version, release_date, description) VALUES (:version, :release_date, :description)"),
                {"version": version, "release_date": release_date, "description": description}
            )
            gedi_version = conn.execute(
                text("SELECT id FROM gedi_versions WHERE version = :version"),
                {"version": version}
            ).fetchone()
    
        return gedi_version.id

    def _write_granule_entry(self, conn, granule_key, outfile_path, included_files, version_id):
        granule_entry = pd.DataFrame(
            data={
                "granule_name": [granule_key],
                "version_id": [version_id],  # Include version ID
                "created_date": [pd.Timestamp.utcnow()],
            }
        )
        granule_entry.to_sql(
            name=self.database_schema['granules']['table_name'],
            con=conn,
            index=False,
            if_exists="append",
        )
    
    def _write_gedi_data(self, conn, gedi_data, version_id):
        # Add version_id to the gedi_data dataframe
        gedi_data['version_id'] = version_id
        
        gedi_data.to_postgis(
            name=self.database_schema['shots']['table_name'],
            con=conn,
            index=False,
            if_exists="append",
        )

    

