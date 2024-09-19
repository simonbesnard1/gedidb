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
from sqlalchemy import Table, MetaData, select
import yaml

from gedidb.metadata.metadata_operations import GEDIMetaDataDownloader

logger = logging.getLogger(__name__)

class GEDIMetadataManager:
    """
    GEDIMetadataManager handles the extraction, storage, and insertion of metadata for various GEDI product types.
    
    Attributes:
    ----------
    metadata_info : dict
        Dictionary mapping GEDI product types (e.g., 'L2A', 'L2B', etc.) to their metadata URLs.
    metadata_path : str
        Path where metadata YAML files will be saved.
    data_table_name : str
        The name of the data table where shot information is stored.
    """

    def __init__(self, metadata_info: dict, metadata_path: str, data_table_name: str = 'shots_table'):
        """
        Initialize the GEDIMetadataManager with metadata information, the path to store metadata files,
        and the name of the data table for metadata insertion.

        Parameters:
        ----------
        metadata_info : dict
            A dictionary mapping product types to their metadata URLs.
        metadata_path : str
            The path where metadata YAML files will be saved.
        data_table_name : str, optional
            The name of the data table where shot information is stored. Default is 'shots_table'.
        """
        self.metadata_info = metadata_info
        self.metadata_path = metadata_path
        self.data_table_name = data_table_name

        # Ensure the metadata path exists
        os.makedirs(self.metadata_path, exist_ok=True)

    def extract_and_store_metadata(self, product_type: str):
        """
        Extract metadata for a specific GEDI product type and save it as a YAML file.

        Parameters:
        ----------
        product_type : str
            The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
        """
        url = self.metadata_info.get(product_type)
        if not url:
            logger.warning(f"No URL found for product type '{product_type}'. Skipping metadata extraction.")
            return

        output_file = os.path.join(self.metadata_path, f"gedi_{product_type.lower()}_metadata.yaml")
        
        if os.path.exists(output_file):
            return 

        extractor = GEDIMetaDataDownloader(url, output_file, data_type=product_type)
        extractor.build_metadata()
        
    def extract_all_metadata(self):
        """
        Extract metadata for all product types listed in the metadata_info dictionary.
        """
        if not self.metadata_info or not self.metadata_path:
            raise ValueError("Metadata info or path is not set.")

        for product_type in self.metadata_info:
            self.extract_and_store_metadata(product_type)

    @staticmethod
    def load_metadata_file(metadata_path: str, product_type: str):
        """
        Load the metadata file for a specific GEDI product type.

        Parameters:
        ----------
        metadata_path : str
            The directory where metadata files are stored.
        product_type : str
            The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').

        Returns:
        -------
        dict or None
            Parsed YAML data as a dictionary, or None if the file does not exist.
        """
        metadata_file = os.path.join(metadata_path, f"gedi_{product_type.lower()}_metadata.yaml")
        if not os.path.exists(metadata_file):
            logger.warning(f"Metadata file for {product_type} not found. Skipping.")
            return None

        with open(metadata_file, 'r') as file:
            return yaml.safe_load(file)

    @staticmethod
    def get_columns_in_data(conn, data_table_name: str):
        """
        Retrieve all columns (variables) from a data table.

        Parameters:
        ----------
        conn : sqlalchemy.engine.Connection
            Active database connection.
        data_table_name : str
            Name of the data table from which to retrieve columns.

        Returns:
        -------
        list
            List of column names in the specified data table.
        """
        data_table = Table(data_table_name, MetaData(), autoload_with=conn)
        return data_table.columns.keys()

    @staticmethod
    def variable_exists_in_metadata(conn, metadata_table, variable_name: str) -> bool:
        """
        Check if a variable already exists in the metadata table.

        Parameters:
        ----------
        conn : sqlalchemy.engine.Connection
            Active database connection.
        metadata_table : sqlalchemy.Table
            Metadata table object.
        variable_name : str
            The variable name to check for.

        Returns:
        -------
        bool
            True if the variable exists, False otherwise.
        """
        query = select(metadata_table.c.sds_name).where(metadata_table.c.sds_name == variable_name)
        return conn.execute(query).fetchone() is not None

    @staticmethod
    def insert_metadata(conn, metadata_table, variable_name: str, var_meta: dict, data_table_name: str):
        """
        Insert metadata for a variable into the metadata table.

        Parameters:
        ----------
        conn : sqlalchemy.engine.Connection
            Active database connection.
        metadata_table : sqlalchemy.Table
            Metadata table object.
        variable_name : str
            The name of the variable being inserted.
        var_meta : dict
            Dictionary containing metadata associated with the variable.
        data_table_name : str
            The name of the source data table for the variable.
        """
        insert_stmt = metadata_table.insert().values(
            sds_name=variable_name,
            description=var_meta.get('Description', ''),
            units=var_meta.get('Units', ''),
            product=var_meta.get('Product', ''),
            source_table=data_table_name
        )
        conn.execute(insert_stmt)
