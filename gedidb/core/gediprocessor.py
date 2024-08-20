import os
import logging
import yaml
import geopandas as gpd
import pandas as pd
from datetime import datetime
from functools import wraps

from gedidb.utils.constants import GediProduct
from gedidb.database import db
from gedidb.processor import granule_parser
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import ShapeProcessor


def log_execution(start_message=None, end_message=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_message = start_message or f"Executing {func.__name__}..."
            print(log_message)
            result = func(*args, **kwargs)
            log_message = end_message or f"Finished {func.__name__}..."
            print(log_message)
            return result
        return wrapper
    return decorator
    
class GEDIGranuleProcessor(GEDIDatabase):
    
    def __init__(self, database_config_file: str = None, schema_config_file: str = None, column_to_field_config_file: str = None, quality_config_file: str = None, field_mapping_config_file: str = None):

        self.COLUMN_TO_FIELD = self.load_config_file(column_to_field_config_file)
        self.database_structure = self.load_config_file(database_config_file)
        self.database_schema = self.load_config_file(schema_config_file)
        self.quality_filter_config = self.load_config_file(quality_config_file)
        self.field_mapping = self.load_config_file(field_mapping_config_file)
        
        super().__init__(self.database_structure['region_of_interest'], self.database_structure['start_date'], self.database_structure['end_date'])
        
        self.sql_connector = self.database_structure['sql_connector']
        self.save_cmr_data = self.database_structure['save_cmr_data']
        self.download_path = self.database_structure['download_path']
        os.makedirs(self.download_path, exist_ok=True)
        self.parquet_path = self.database_structure['parquet_path']
        os.makedirs(self.parquet_path, exist_ok=True)
        self.delete_h5_files = self.database_structure['delete_h5_files']
        self.db_path = self.database_structure['database_url']
        initial_geom = gpd.read_file(self.database_structure['region_of_interest'])
        self.geom = ShapeProcessor(initial_geom).check_and_format(simplify=True)        
        self.start_date = datetime.strptime(self.database_structure['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.database_structure['end_date'], '%Y-%m-%d')
    
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
        outfile_path = os.path.join(self.parquet_path, f"filtered_l1b_l2ab_l4ac_{granule_key}.parquet")
        
        # If the output file already exists, return early
        if os.path.exists(outfile_path):
            return granule_key, outfile_path, sorted([fname[0] for fname in granules])
    
        gdf_dict = self._parse_granules(granules, granule_key)
        if not gdf_dict:
            logging.warning(f"Skipping granule {granule_key} due to missing or invalid data.")
            return None
        
        gdf = self._join_gdfs(gdf_dict)
        if gdf is None:
            logging.warning(f"Skipping granule {granule_key} due to issues during the join operation.")
            return None
    
        gdf["granule"] = granule_key
        gdf.to_parquet(outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us")
        
        return granule_key, outfile_path, sorted([fname[0] for fname in granules])
    
    def _parse_granules(self, granules, granule_key):
        """Parse granules and handle None or invalid data."""
        gdf_dict = {}
        for product, file in granules:
            gdf = granule_parser.parse_h5_file(
                file, product, 
                quality_filter=self.quality_filter_config, 
                field_mapping=self.field_mapping, 
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
        try:
            gdf = gdf_dict[GediProduct.L1B.value]
            for product in [GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]:
                gdf = gdf.join(
                    gdf_dict[product.value].set_index("shot_number"),
                    on="shot_number",
                    how="inner",
                )
            
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
    
        field_to_column = {v: k for k, v in self.COLUMN_TO_FIELD.items()}
    
        granule_key, outfile_path, included_files = input
        gedi_data = gpd.read_parquet(outfile_path)
        gedi_data = gedi_data[list(field_to_column.keys())]
        gedi_data = gedi_data.rename(columns=field_to_column)
        gedi_data = gedi_data.astype({"shot_number": "int64"})
    
        # Assuming you have the database connection logic here...
        with db.get_db_conn(db_url=self.db_path).begin() as conn:
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
            
            gedi_data.to_postgis(
                name=self.database_schema['shots']['table_name'],
                con=conn,
                index=False,
                if_exists="append",
            )
            conn.commit()
            del gedi_data
        return granule_entry

