import geopandas as gpd
import pandas as pd
from sqlalchemy import Table, MetaData
from gedidb.database.db import DatabaseManager

PRODUCT_TYPES = ['L2A', 'L2B', 'L4A', 'L4C']  # Constants for product types

class GEDIDatabase:
    def __init__(self, db_path: str, sql_script: str = None, tables=None, metadata_handler=None):
        self.db_path = db_path
        self.sql_script = sql_script
        self.tables = tables
        self.metadata_handler = metadata_handler

    def _create_db(self):
        """
        Create the database schema.
        """
        db_manager = DatabaseManager(db_url=self.db_path)
        db_manager.create_tables(sql_script=self.sql_script)

    def _write_db(self, input):
        """
        Dask task to write data to the database. It initializes a new database connection inside the task.
        :param input: Tuple containing granule key, output file path, and included files.
        """
        if input is None:
            return
        
        granule_key, outfile_path, included_files = input
        
        # Load the data to write
        granule_entry = pd.DataFrame({
            "granule_name": [granule_key],
            "created_date": [pd.Timestamp.utcnow()],
        })
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
        """
        gedi_data.to_postgis(
            name=self.tables['shots'],
            con=conn,
            index=False,
            if_exists="append",
        )

    def write_all_metadata(self, conn):
        """
        Write metadata for all products into the metadata table.
        """
        for product_type in PRODUCT_TYPES:
            self.write_metadata(conn, product_type)

    def write_metadata(self, conn, product_type):
        """
        Write metadata for a specific product type.
        """
        metadata = self.metadata_handler.load_metadata_file(self.metadata_handler.metadata_path, product_type)
        if not metadata:
            return

        metadata_table = Table(self.tables['metadata'], MetaData(), autoload_with=conn)
        data_columns = self.metadata_handler.get_columns_in_data(conn, self.tables['shots'])

        for variable_name in data_columns:
            var_meta = next(
                (item for item in metadata.get('Layers_Variables', []) if item.get('SDS_Name') == variable_name), None
            )
            if var_meta is None:
                continue
            if self.metadata_handler.variable_exists_in_metadata(conn, metadata_table, variable_name):
                continue
            self.metadata_handler.insert_metadata(conn, metadata_table, variable_name, var_meta, self.tables['shots'])
