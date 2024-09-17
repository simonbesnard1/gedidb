# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import geopandas as gpd
import pandas as pd
from sqlalchemy import Table, MetaData
from gedidb.database.db import DatabaseManager

# Constants for product types
PRODUCT_TYPES = ['L2A', 'L2B', 'L4A', 'L4C']

class GEDIDatabase:
    """
    GEDIDatabase handles the creation, management, and data writing operations 
    for a GEDI (Global Ecosystem Dynamics Investigation) database.

    Attributes:
    ----------
    db_path : str
        The database connection URL.
    sql_script : str, optional
        SQL script to create tables in the database.
    tables : dict
        Dictionary containing table names for granules, shots, and metadata.
    metadata_handler : GEDIMetadataManager
        Handler responsible for managing and writing metadata.
    """

    def __init__(self, db_path: str, sql_script: str = None, tables=None, metadata_handler=None):
        """
        Initialize the GEDIDatabase class.

        Parameters:
        ----------
        db_path : str
            The database connection URL.
        sql_script : str, optional
            SQL script to create tables in the database.
        tables : dict
            Dictionary containing table names for granules, shots, and metadata.
        metadata_handler : GEDIMetadataManager
            Handler for managing and writing metadata to the database.
        """
        self.db_path = db_path
        self.sql_script = sql_script
        self.tables = tables
        self.metadata_handler = metadata_handler
        
    def _create_db(self):
        """
        Create the database schema using the provided SQL script.
        """
        db_manager = DatabaseManager(db_url=self.db_path)
        db_manager.create_tables(sql_script=self.sql_script)

    def _write_db(self, input):
        """
        Write data to the database.

        This function is intended to be used as a Dask task. It initializes a 
        new database connection inside the task.

        Parameters:
        ----------
        input : tuple
            Tuple containing the granule key, output file path, and included files.
        """
        if input is None:
            return

        granule_key, outfile_path, included_files = input

        # Load the data to write
        granule_entry = pd.DataFrame({
            "granule_name": [granule_key],
            "created_date": [pd.Timestamp.utcnow()],
        })

        # Load the GEDI data from a parquet file
        gedi_data = gpd.read_parquet(outfile_path)
        gedi_data = gedi_data.astype({"shot_number": "int64"})

        # Initialize the connection inside the task
        db_manager = DatabaseManager(db_url=self.db_path)
        engine = db_manager.get_connection()

        # Write data to the database
        with engine.begin() as conn:
            self._write_granule_entry(conn, granule_entry)
            self._write_gedi_data(conn, gedi_data)
            self.write_all_metadata(conn)

    def _write_granule_entry(self, conn, granule_entry):
        """
        Write the granule entry into the database.

        Parameters:
        ----------
        conn : Connection
            The database connection object.
        granule_entry : pandas.DataFrame
            DataFrame containing the granule data to be written.
        """
        granule_entry.to_sql(
            name=self.tables['granules'],
            con=conn,
            index=False,
            if_exists="append",
        )

    def _write_gedi_data(self, conn, gedi_data):
        """
        Write the GEDI data into the database.

        Parameters:
        ----------
        conn : Connection
            The database connection object.
        gedi_data : geopandas.GeoDataFrame
            GeoDataFrame containing the GEDI data.
        """
        gedi_data.to_postgis(
            name=self.tables['shots'],
            con=conn,
            index=False,
            if_exists="append",
        )

    def write_all_metadata(self, conn):
        """
        Write metadata for all product types into the metadata table.

        Parameters:
        ----------
        conn : Connection
            The database connection object.
        """
        for product_type in PRODUCT_TYPES:
            self.write_metadata(conn, product_type)

    def write_metadata(self, conn, product_type):
        """
        Write metadata for a specific product type into the metadata table.

        Parameters:
        ----------
        conn : Connection
            The database connection object.
        product_type : str
            The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
        """
        # Load the metadata for the specific product type
        metadata = self.metadata_handler.load_metadata_file(self.metadata_handler.metadata_path, product_type)
        if not metadata:
            return

        # Fetch the metadata table from the database
        metadata_table = Table(self.tables['metadata'], MetaData(), autoload_with=conn)
        data_columns = self.metadata_handler.get_columns_in_data(conn, self.tables['shots'])

        # Write the metadata for each column in the data
        for variable_name in data_columns:
            var_meta = next(
                (item for item in metadata.get('Layers_Variables', []) if item.get('SDS_Name') == variable_name), None
            )
            if var_meta is None:
                continue
            # Avoid duplicate entries
            if self.metadata_handler.variable_exists_in_metadata(conn, metadata_table, variable_name):
                continue

            # Insert metadata into the table
            self.metadata_handler.insert_metadata(conn, metadata_table, variable_name, var_meta, self.tables['shots'])
