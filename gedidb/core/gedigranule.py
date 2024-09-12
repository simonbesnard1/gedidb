import os
import logging
import pathlib
from sqlalchemy import Table, MetaData, select
from pyspark.sql import SparkSession

from gedidb.utils.constants import GediProduct
from gedidb.granule import granule_parser
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.database.db import DatabaseManager

logger = logging.getLogger(__name__)

class GEDIGranule:
    def __init__(self, db_path: str, download_path: str, parquet_path: str, data_info: dict):
        """
        Initialize the GEDIGranuleProcessor class.

        :param db_path: Database URL path.
        :param download_path: Path where granules will be downloaded.
        :param parquet_path: Path where processed granules will be saved as parquet.
        :param data_info: Dictionary containing relevant information about data (e.g., table names).
        """
        self.db_path = db_path
        self.download_path = download_path
        self.parquet_path = parquet_path
        self.data_info = data_info
        self.db_manager = DatabaseManager(db_url=self.db_path)
        
    def get_processed_granules(self, granule_ids):
        """
        Check which granules have already been processed and stored in the database.

        :param granule_ids: A list of granule IDs to check.
        :return: A set of processed granule IDs.
        """
        engine = self.db_manager.get_connection()

        if engine:
            with engine.begin() as conn:
                granules_table = Table(self.data_info['database']['tables']['granules'], MetaData(), autoload_with=conn)
                query = select(granules_table.c.granule_name).where(granules_table.c.granule_name.in_(granule_ids))
                result = conn.execute(query)

        return {row[0] for row in result}

    def process_granule(self, row: tuple[str, tuple[GediProduct, str, str]]):
        """
        Process a granule by parsing, joining, and saving it.

        :param row: Tuple containing granule key and granule information.
        :return: Tuple containing the granule key, output path, and list of included files.
        """        
        # Extract the granule key from the first element
        granule_key = row[0][0]  # The first tuple's first element is the granule_key
    
        # Extract the granules (all products) from the row
        granules = [item[1] for item in row]  # The second element of each tuple is the product data

        outfile_path = self.get_output_path(granule_key)

        if os.path.exists(outfile_path):
            return self._prepare_return_value(granule_key, outfile_path, granules)
        
        gdf_dict = self.parse_granules(granules, granule_key)

        if not gdf_dict:
            logger.warning(f"Skipping granule {granule_key} due to missing or invalid data.")
            return None

        gdf = self._join_gdfs(gdf_dict)
        if gdf is None:
            logger.warning(f"Skipping granule {granule_key} due to join issues.")
            return None

        self.save_gdf_to_parquet(gdf, granule_key, outfile_path)
        return self._prepare_return_value(granule_key, outfile_path, granules)

    def get_output_path(self, granule_key):
        """
        Generate the output path for a processed granule.

        :param granule_key: Granule identifier key.
        :return: Full path to the parquet file for the granule.
        """
        return os.path.join(self.parquet_path, f"filtered_granule_{granule_key}.parquet")

    def _prepare_return_value(self, granule_key, outfile_path, granules):
        """
        Prepare the return value after processing a granule.

        :param granule_key: Granule key.
        :param outfile_path: Output path for the processed granule.
        :param granules: List of granule files processed.
        :return: Tuple of granule key, output path, and list of files.
        """
        return granule_key, outfile_path, sorted([fname[0] for fname in granules])

    def save_gdf_to_parquet(self, gdf, granule_key, outfile_path):
        """
        Save the processed GeoDataFrame to a parquet file.

        :param gdf: GeoDataFrame containing processed granule data.
        :param granule_key: Granule key.
        :param outfile_path: Path to save the parquet file.
        """
        gdf["granule"] = granule_key
        gdf.to_parquet(outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us")

    def parse_granules(self, granules, granule_key):
        """
        Parse granules and return a dictionary of GeoDataFrames.

        :param granules: List of granule products and file paths.
        :param granule_key: Granule key.
        :return: Dictionary of GeoDataFrames for each product.
        """
        gdf_dict = {}
        for product, file in granules:
    
            gdf = granule_parser.parse_h5_file(file, product, data_info=self.data_info)
            if gdf is not None:
                gdf_dict[product] = gdf
            else:
                logger.info(f"Skipping product {product} for granule {granule_key} due to parsing failure.")

        valid_gdf_dict = {k: v for k, v in gdf_dict.items() if not v.empty and "shot_number" in v.columns}
        return valid_gdf_dict

    def _join_gdfs(self, gdf_dict):
        """
        Join multiple GeoDataFrames based on the shot number.

        :param gdf_dict: Dictionary of GeoDataFrames for each product.
        :return: Joined GeoDataFrame.
        """
        try:
            gdf = gdf_dict[GediProduct.L2A.value]
    
            for product in [GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]:
                gdf = gdf.join(
                    gdf_dict[product.value].set_index("shot_number"),
                    on="shot_number",
                    how="inner",
                    rsuffix=f'_{product.value}'
                )
    
            columns_to_drop = [
                col for col in gdf.columns
                if any(col.endswith(suffix) for suffix in [f'_{GediProduct.L2B.value}', f'_{GediProduct.L4A.value}', f'_{GediProduct.L4C.value}'])
            ]
    
            gdf = gdf.drop(columns=columns_to_drop)
            gdf = gdf.set_geometry("geometry")
    
            return gdf

        except KeyError as e:
            logger.error(f"Join operation failed due to missing product data: {e}")
            return None

    def _create_spark_session(self) -> SparkSession:
        """Create and return a Spark session."""
        return (SparkSession.builder
                .appName("GEDI Processing")
                .config("spark.executor.instances", "4")
                .config("spark.executor.cores", "4")
                .config("spark.executor.memory", "4g")
                .config("spark.driver.memory", "2g")
                .getOrCreate())