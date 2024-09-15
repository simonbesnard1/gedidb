import os
import logging
from sqlalchemy import Table, MetaData, select
import yaml

from gedidb.metadata.metadata_operations import GEDIMetaDataDownloader

logger = logging.getLogger(__name__)

class GEDIMetadataManager:
    def __init__(self, metadata_info: dict, metadata_path: str, data_table_name: str = 'shots_table'):
        """
        Initialize the GediMetadataManager with metadata information, the path to store metadata files, 
        and the name of the data table for metadata insertion.

        :param metadata_info: A dictionary mapping product types to their metadata URLs.
        :param metadata_path: The path where metadata YAML files will be saved.
        :param data_table_name: The name of the data table where shot information is stored.
        """
        self.metadata_info = metadata_info
        self.metadata_path = metadata_path
        self.data_table_name = data_table_name

        # Ensure the metadata path exists
        if not os.path.exists(self.metadata_path):
            os.makedirs(self.metadata_path)
        
    def extract_and_store_metadata(self, product_type: str):
        """
        Extract metadata for a specific GEDI product type and save it as a YAML file.
        
        :param product_type: The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
        """
        url = self.metadata_info.get(product_type)
        if not url:
            logger.warning(f"No URL found for product type '{product_type}'. Skipping metadata extraction.")
            return

        output_file = os.path.join(self.metadata_path, f"gedi_{product_type.lower()}_metadata.yaml")
        extractor = GEDIMetaDataDownloader(url, output_file, data_type=product_type)
        extractor.build_metadata()

        logger.info(f"Metadata for {product_type} stored at {output_file}")

    def extract_all_metadata(self):
        """
        Loop through all product types and extract metadata for each.
        """
        if not self.metadata_info or not self.metadata_path:
            raise ValueError("Metadata info or path is not set.")

        for product_type in self.metadata_info:
            self.extract_and_store_metadata(product_type)

    @staticmethod
    def load_metadata_file(metadata_path, product_type: str):
        """
        Load the metadata file for a specific product type.

        :param metadata_path: The directory where metadata files are stored.
        :param product_type: The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
        :return: Parsed YAML data as a dictionary.
        """
        metadata_file = os.path.join(metadata_path, f"gedi_{product_type.lower()}_metadata.yaml")
        if not os.path.exists(metadata_file):
            logger.warning(f"Metadata file for {product_type} not found. Skipping.")
            return None

        with open(metadata_file, 'r') as file:
            return yaml.safe_load(file)

    @staticmethod
    def get_columns_in_data(conn, data_table_name):
        """Get all columns (variables) from the shots table."""
        data_table = Table(data_table_name, MetaData(), autoload_with=conn)
        return data_table.columns.keys()

    @staticmethod
    def variable_exists_in_metadata(conn, metadata_table, variable_name):
        """Check if a variable already exists in the metadata table."""
        query = select(metadata_table.c.sds_name).where(metadata_table.c.sds_name == variable_name)
        return conn.execute(query).fetchone() is not None

    @staticmethod
    def insert_metadata(conn, metadata_table, variable_name, var_meta, data_table_name):
        """
        Insert metadata into the metadata table.

        :param conn: Database connection.
        :param metadata_table: The metadata table to insert data into.
        :param variable_name: The name of the variable being inserted.
        :param var_meta: The metadata associated with the variable.
        :param data_table_name: The source table where the variable is located.
        """
        insert_stmt = metadata_table.insert().values(
            sds_name=variable_name,
            description=var_meta.get('Description', ''),
            units=var_meta.get('Units', ''),
            source_table=data_table_name
        )
        conn.execute(insert_stmt)
        logger.info(f"Inserted metadata for variable '{variable_name}'.")

