import geopandas as gpd
from functools import wraps
import logging
import pandas as pd
from sqlalchemy import Table, MetaData

from gedidb.database.db import DatabaseManager

logger = logging.getLogger(__name__)

PRODUCT_TYPES = ['L2A', 'L2B', 'L4A', 'L4C']  # Constants for product types

def log_execution(start_message=None, end_message=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_message = start_message or f"Executing {func.__name__}..."
            logger.info(log_message)
            result = func(*args, **kwargs)
            log_message = end_message or f"Finished {func.__name__}..."
            logger.info(log_message)
            return result
        return wrapper
    return decorator

class GEDIDatabase:
    def __init__(self, db_path: str, sql_script: str = None, tables=None, metadata_handler=None):
        """
        Initialize the GEDIDatabase class.

        :param db_path: The database connection URL.
        :param sql_script: SQL script to create tables.
        :param tables: Dictionary containing table names for granules and shots.
        :param metadata_handler: Handler class for metadata operations, passed as a dependency.
        """
        self.db_path = db_path
        self.sql_script = sql_script
        self.tables = tables
        self.metadata_handler = metadata_handler
        self.db_manager = DatabaseManager(db_url=self.db_path)
        self.engine = self.db_manager.get_connection()
        
    @log_execution(start_message="Creating database tables...", end_message="Tables created")
    def _create_db(self):
        if self.sql_script:
            self.db_manager.create_tables(sql_script=self.sql_script)

    @log_execution(start_message="Writing data to the database...", end_message="Data successfully written")
    def _write_db(self, input):
        """
        Write granule and GEDI data into the database.

        :param input: Tuple containing granule key, output file path, and included files.
        """
        if input is None:
            return

        granule_key, outfile_path, included_files = input
        granule_entry = pd.DataFrame({
            "granule_name": [granule_key],
            "created_date": [pd.Timestamp.utcnow()],
        })

        gedi_data = gpd.read_parquet(outfile_path)
        gedi_data = gedi_data.astype({"shot_number": "int64"})

        if self.engine:
            with self.engine.begin() as conn:
                self._write_granule_entry(conn, granule_entry)
                self._write_gedi_data(conn, gedi_data)

                # Write metadata for all products
                self.write_all_metadata(conn)

                conn.commit()
        else:
            logger.error("Failed to create a database connection.")

    def _write_granule_entry(self, conn, granule_entry):
        """
        Write granule entry into the database.

        :param conn: Database connection.
        :param granule_entry: DataFrame containing granule data.
        """
        granule_entry.to_sql(
            name=self.tables['granules'],
            con=conn,
            index=False,
            if_exists="append",
        )

    def _write_gedi_data(self, conn, gedi_data):
        """
        Write GEDI data into the database.

        :param conn: Database connection.
        :param gedi_data: GeoDataFrame containing GEDI data.
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

        :param conn: Database connection.
        """
        for product_type in PRODUCT_TYPES:
            self.write_metadata(conn, product_type)

    def write_metadata(self, conn, product_type):
        """
        Write metadata for a specific product type into the metadata table.

        :param conn: Database connection.
        :param product_type: The product type (e.g., 'L2A', 'L2B', 'L4A', 'L4C').
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
                logger.info(f"No metadata found for variable '{variable_name}'. Skipping.")
                continue

            if self.metadata_handler.variable_exists_in_metadata(conn, metadata_table, variable_name):
                logger.info(f"Variable '{variable_name}' already exists in the metadata table. Skipping.")
                continue

            self.metadata_handler.insert_metadata(conn, metadata_table, variable_name, var_meta, self.tables['shots'])
