import os
import yaml
import geopandas as gpd
import pandas as pd
from datetime import datetime
from functools import wraps

from GEDItools.utils.constants import GediProduct
from GEDItools.database import db
from GEDItools.processor import granule_parser
from GEDItools.utils.spark_session import create_spark
from GEDItools.downloader.data_downloader import H5FileDownloader, CMRDataDownloader


# Decorator for logging
def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Executing {func.__name__}...")
        result = func(*args, **kwargs)
        print(f"Finished {func.__name__}.")
        return result

    return wrapper


class GEDIDatabase:
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date

    @log_execution
    def download_cmr_data(self):
        return CMRDataDownloader(self.geom, start_date=self.start_date, end_date=self.end_date)

    @log_execution
    def create_spark_session(self):
        return create_spark()
    
class GEDIGranuleProcessor(GEDIDatabase):
    
    def __init__(self, database_config_file: str = None, column_to_field_config_file: str = None, quality_config_file: str = None, field_mapping_config_file: str = None):

        self.COLUMN_TO_FIELD = self.load_config_file(column_to_field_config_file)
        self.database_structure = self.load_config_file(database_config_file)
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
        self.geom = gpd.read_file(self.database_structure['region_of_interest'])
        self.start_date = datetime.strptime(self.database_structure['start_date'], '%Y-%m-%d')
        self.end_date = datetime.strptime(self.database_structure['end_date'], '%Y-%m-%d')
    
    @staticmethod
    def load_config_file(file_path: str = "field_mapping.yml") -> dict:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
                
    @log_execution
    def compute(self):
        cmr_data = self.download_cmr_data().download()
        spark = self.create_spark_session()

        name_url = cmr_data[
            ["id", "name", "url", "product"]
        ].to_records(index=False)

        urls = spark.sparkContext.parallelize(name_url)
        downloader = H5FileDownloader(self.download_path)

        mapped_urls = urls.map(lambda x: downloader.download(x[0], x[2], GediProduct(x[3]))).groupByKey()
        processed_granules = mapped_urls.map(self._process_granule)
        granule_entries = processed_granules.coalesce(8).map(self._write_db)
        granule_entries.count()
        spark.stop()
        print("done")

    def _process_granule(self, row: tuple[str, tuple[GediProduct, str]]):
        granule_key, granules = row
        included_files = sorted([fname[0] for fname in granules])
        outfile_path = os.path.join(self.parquet_path, f"filtered_l1b_l2ab_l4ac_{granule_key}.parquet")
        return_value = (granule_key, outfile_path, included_files)
        if os.path.exists(outfile_path):
            return return_value

        gdfs = {}
        # Parse each file and run per-product filtering
        for product, file in granules:
            gdfs[product] = (
                granule_parser.parse_h5_file(file, product, quality_filter=self.quality_filter_config, 
                                             field_mapping = self.field_mapping)
                .rename(lambda x: f"{x}_{product}", axis=1)
                .rename({f"shot_number_{product}": "shot_number"}, axis=1)
            )
            
        gdf = (
            gdfs[GediProduct.L1B.value]
            .join(
                gdfs[GediProduct.L2A.value].set_index("shot_number"),
                on="shot_number",
                how="inner",
            )
            .join(
                gdfs[GediProduct.L2B.value].set_index("shot_number"),
                on="shot_number",
                how="inner",
            )
            .join(
                gdfs[GediProduct.L4A.value].set_index("shot_number"),
                on="shot_number",
                how="inner",
            )
            .join(
                gdfs[GediProduct.L4C.value].set_index("shot_number"),
                on="shot_number",
                how="inner",
            )            
            .drop(["geometry_level2A", "geometry_level2B", "geometry_level4A", "geometry_level4C"], axis=1)
            .set_geometry("geometry_level1B")
            .rename_geometry("geometry")
        )

        gdf["granule"] = granule_key
        gdf.to_parquet(outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us")

        return return_value

    def _write_db(self, input):

        field_to_column = {v: k for k, v in self.COLUMN_TO_FIELD.items()}

        granule_key, outfile_path, included_files = input
        gedi_data = gpd.read_parquet(outfile_path)

        assert isinstance(gedi_data.empty, object)
        if gedi_data.empty:
            print("empty")
            return
        gedi_data = gedi_data[list(field_to_column.keys())]
        gedi_data = gedi_data.rename(columns=field_to_column)
        gedi_data = gedi_data.astype({"shot_number": "int64"})
        print(f"Writing granule: {granule_key}")

        with db.get_db_conn(db_url= self.db_path).begin() as conn:
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
                name="gedi_granules",
                con=conn,
                index=False,
                if_exists="append",
            )
            conn.commit()
            del gedi_data
        return granule_entry
