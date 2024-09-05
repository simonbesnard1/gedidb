import os
import logging
import yaml
import geopandas as gpd
from datetime import datetime
from functools import wraps
from sqlalchemy import Table, MetaData


from gedidb.utils.constants import GediProduct
from gedidb.database.db import DatabaseManager
from gedidb.processor import granule_parser
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.geospatial_tools import ShapeProcessor
from gedidb.utils.gedi_metadata import GediMetaDataExtractor

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
    
    def __init__(self, data_config_file: str, sql_config_file:str):
        self.data_info = self.load_yaml_file(data_config_file)
        self.sql_script = self.load_sql_file(sql_config_file)
        self.metadata_info = self.data_info['earth_data_info']['METADATA_INFORMATION']
        
        super().__init__(
            self.data_info['region_of_interest'], 
            self.data_info['start_date'], 
            self.data_info['end_date']
        )
        self.setup_paths_and_dates()
        self.extract_all_metadata()
        
    def setup_paths_and_dates(self):
        """Set up paths and dates based on the configuration."""
        self.download_path = self.ensure_directory(os.path.join(self.data_info['data_dir'], 'download'))
        self.parquet_path = self.ensure_directory(os.path.join(self.data_info['data_dir'], 'parquet'))
        self.metadata = self.ensure_directory(os.path.join(self.data_info['data_dir'], 'metadata'))        
        self.db_path = self.data_info['database_url']
        initial_geom = gpd.read_file(self.data_info['region_of_interest'])
        self.geom = ShapeProcessor(initial_geom).check_and_format(simplify=True)        
        self.start_date = datetime.strptime(self.data_info['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.data_info['end_date'], '%Y-%m-%d')
        
    def extract_and_store_metadata(self, product_type: str):
        """
        Extract metadata for a specific GEDI product type and save it as a YAML file.
        
        :param product_type: The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
        """
        # Get the URL for the given product type
        url = self.data_info['earth_data_info']['METADATA_INFORMATION'].get(product_type)
        
        if url is None:
            raise ValueError(f"No URL found for product type '{product_type}'.")

        # Create the output file path in the self.metadata directory
        output_file = os.path.join(self.metadata, f"gedi_{product_type.lower()}_metadata.yaml")
        
        # Instantiate and run the metadata extractor
        extractor = GediMetaDataExtractor(url, output_file, data_type=product_type)
        extractor.run()

        print(f"Metadata for {product_type} stored at {output_file}")

    def extract_all_metadata(self):
        """
        Loop through all product types in the METADATA_INFORMATION and extract metadata for each.
        """
        for product_type in self.metadata_info.keys():
            try:
                self.extract_and_store_metadata(product_type)
            except Exception as e:
                print(f"Failed to extract metadata for {product_type}: {e}")

    @staticmethod
    def ensure_directory(path):
        """Ensure that a directory exists."""
        os.makedirs(path, exist_ok=True)
        return path
    
    @staticmethod
    def load_yaml_file(file_path: str = "field_mapping.yml") -> dict:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
        
    @staticmethod
    def load_sql_file(file_path: str = "field_mapping.yml") -> dict:
        with open(file_path, 'r') as file:
            return file.read()        
                
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

    def _process_granule(self, row: tuple[str, tuple[GediProduct, str, str]]):
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
                    lsuffix=f'_{product.value}'  # Right suffix
                )


            # Identify columns to drop (those with suffixes from the right joins)
            columns_to_drop = [col for col in gdf.columns if any(col.endswith(suffix) for suffix in
                                                                 [f'_{GediProduct.L2B.value}',
                                                                  f'_{GediProduct.L4A.value}',
                                                                  f'_{GediProduct.L4C.value}'])]

            # Drop the identified duplicate columns
            gdf = gdf.drop(columns=columns_to_drop)

            # Set the geometry column and rename it
            gdf = gdf.set_geometry("geometry")

            return gdf
        
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
        db_manager.create_tables(sql_script=self.sql_script)
        
        # Use the DatabaseManager to manage the connection and transaction
        engine = db_manager.get_connection()
    
        if engine:
            with engine.begin() as conn:
                self._write_gedi_data(conn, gedi_data)
                
                self._write_metadata(conn, gedi_data.columns)

                conn.commit()
                del gedi_data
        else:
            print("Failed to create a database connection.")
    
    def _write_gedi_data(self, conn, gedi_data):
        # Add version_id to the gedi_data dataframe
        
        gedi_data.to_postgis(
            name=self.data_info['table_names']['shots'],
            con=conn,
            index=False,
            # TODO: remove this
            if_exists="append",
        )
        
    def _write_metadata(self, conn, variables):
        """Insert metadata information into the metadata table."""
        metadata_table = Table(self.data_info['table_names']['metadata'], MetaData(), autoload_with=conn)
        
        # Insert metadata for each variable in the data
        for var in variables:
            if var in self.metadata['variables']:
                var_meta = self.metadata['variables'][var]
                insert_stmt = metadata_table.insert().values(
                    variable_name=var,
                    description=var_meta.get('description', ''),
                    data_type=var_meta.get('data_type', 'unknown'),
                    units=var_meta.get('units', ''),
                    source_table=self.data_info['table_names']['shots']
                )
                conn.execute(insert_stmt)

    

