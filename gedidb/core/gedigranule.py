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
import shutil
import pandas as pd

from gedidb.utils.constants import GediProduct
from gedidb.granule import granule_parser
from gedidb.database.db import DatabaseManager

logger = logging.getLogger(__name__)

class GEDIGranule:
    """
    GEDIGranule handles the processing and management of GEDI granules, including parsing, joining, 
    and saving the data to parquet files, as well as querying processed granules from a database.
    
    Attributes:
    ----------
    db_path : str
        The database connection URL.
    download_path : str
        Path where granules are downloaded.
    parquet_path : str
        Path where processed granules are saved as parquet files.
    data_info : dict
        Dictionary containing relevant information about data, such as table names.
    """
    
    def __init__(self, db_path: str, download_path: str, parquet_path: str, data_info: dict):
        """
        Initialize the GEDIGranule class.

        Parameters:
        ----------
        db_path : str
            Database URL path.
        download_path : str
            Path where granules are downloaded.
        parquet_path : str
            Path where processed granules are saved as parquet files.
        data_info : dict
            Dictionary containing relevant information about data, such as table names.
        """
        self.db_path = db_path
        self.download_path = download_path
        self.parquet_path = parquet_path
        self.data_info = data_info

    def get_processed_granules(self, granule_ids):
        """
        Query the database to check which granules have already been processed.

        Parameters:
        ----------
        granule_ids : list
            List of granule IDs to check.

        Returns:
        -------
        set
            Set of granule IDs that have already been processed.
        """
        db_manager = DatabaseManager(db_url=self.db_path)
        engine = db_manager.get_connection()

        if engine:
            with engine.connect() as conn:
                granules_table = Table(self.data_info['database']['tables']['granules'], MetaData(), autoload_with=conn)
                query = select(granules_table.c.granule_name).where(granules_table.c.granule_name.in_(granule_ids))
                result = conn.execute(query)

                return {row[0] for row in result}

    def process_granule(self, row):
        """
        Process a granule by parsing, joining, and saving it to a parquet file.

        Parameters:
        ----------
        row : tuple
            Tuple containing the granule key and product data.
        
        Returns:
        -------
        tuple or None
            Tuple containing the granule key, output path, and list of processed files, or None if processing fails.
        """
        granule_key = row[0][0]
        granules = [item[1] for item in row]
        outfile_path = self.get_output_path(granule_key)

        if os.path.exists(outfile_path):
            logger.info(f"Parquet file for granule {granule_key} already exists. Skipping h5 files processing.")
            return self._prepare_return_value(granule_key, outfile_path, granules)

        gdf_dict = self.parse_granules(granules, granule_key)
        
        if not gdf_dict:
            self._write_empty_granule_to_db(granule_key)
            return None
    
        gdf = self._join_gdfs(gdf_dict, granule_key)
        if gdf is None:
            self._write_empty_granule_to_db(granule_key)
            return None

        self.save_gdf_to_parquet(gdf, granule_key, outfile_path)
        return self._prepare_return_value(granule_key, outfile_path, granules)

    def get_output_path(self, granule_key):
        """
        Generate the output path for a processed granule.

        Parameters:
        ----------
        granule_key : str
            Granule identifier key.

        Returns:
        -------
        str
            Full path to the parquet file for the granule.
        """
        return os.path.join(self.parquet_path, f"filtered_granule_{granule_key}.parquet")

    @staticmethod
    def _prepare_return_value(granule_key, outfile_path, granules):
        """
        Prepare the return value after processing a granule.

        Parameters:
        ----------
        granule_key : str
            Granule key.
        outfile_path : str
            Output path for the processed granule.
        granules : list
            List of granule files processed.

        Returns:
        -------
        tuple
            Tuple of granule key, output path, and sorted list of files.
        """
        return granule_key, outfile_path, sorted([fname[0] for fname in granules])

    @staticmethod
    def save_gdf_to_parquet(gdf, granule_key, outfile_path):
        """
        Save the processed GeoDataFrame to a parquet file.

        Parameters:
        ----------
        gdf : geopandas.GeoDataFrame
            GeoDataFrame containing processed granule data.
        granule_key : str
            Granule key.
        outfile_path : str
            Path to save the parquet file.
        """
        gdf["granule"] = granule_key
        gdf.to_parquet(outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us")

    def parse_granules(self, granules, granule_key):
        """
        Parse granules and return a dictionary of GeoDataFrames.

        Parameters:
        ----------
        granules : list
            List of granule products and file paths.
        granule_key : str
            Granule key.

        Returns:
        -------
        dict
            Dictionary of GeoDataFrames for each product.
        """
        gdf_dict = {}
        granule_dir = os.path.join(self.download_path, granule_key)
        
        for product, file in granules:
            gdf = granule_parser.parse_h5_file(file, product, data_info=self.data_info)
            if gdf is not None:
                gdf_dict[product] = gdf
            
            else:
                logger.info(f"Skipping product {product} for granule {granule_key} due to parsing failure.")
    
        try:
            shutil.rmtree(granule_dir)
        except Exception as e:
            logger.error(f"Error deleting directory {granule_dir}: {e}")
        
        # Filter valid data frames
        return {k: v for k, v in gdf_dict.items() if not v.empty and "shot_number" in v.columns}

    @staticmethod
    def _join_gdfs(gdf_dict, granule_key):
        """
        Join multiple GeoDataFrames based on the shot number. Ensure that L2A, L2B, L4A, and L4C are available.
    
        Parameters:
        ----------
        gdf_dict : dict
            Dictionary of GeoDataFrames for each product.
        granule_key: str
            The granule ID.
    
        Returns:
        -------
        geopandas.GeoDataFrame or None
            Joined GeoDataFrame or None if the required data is missing or if the join fails.
        """
        # Ensure that all required products are available in the gdf_dict
        required_products = [GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]
        
        # Check if all required products are available and non-empty
        for product in required_products:
            if product.value not in gdf_dict or gdf_dict[product.value].empty:
                logger.warning(f"Product {product.value} is missing or empty for granule {granule_key}.")
                return None  # If any product is missing or empty, return None
    
        # Proceed with the join since all required products are present
        gdf = gdf_dict[GediProduct.L2A.value]
    
        for product in [GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]:
            product_gdf = gdf_dict[product.value]
            gdf = gdf.join(
                product_gdf.set_index("shot_number"),
                on="shot_number",
                how="inner",
                rsuffix=f'_{product.value}'
            )
    
        # Conditionally drop redundant columns after joining
        suffixes = [f'_{GediProduct.L2B.value}', f'_{GediProduct.L4A.value}', f'_{GediProduct.L4C.value}']
        columns_to_drop = [col for col in gdf.columns if any(col.endswith(suffix) for suffix in suffixes)]
        if columns_to_drop:
            gdf = gdf.drop(columns=columns_to_drop)
    
        # Ensure that geometry is correctly set, if available
        if "geometry" in gdf.columns:
            gdf = gdf.set_geometry("geometry")
        
        return gdf if not gdf.empty else None
      
    def _write_empty_granule_to_db(self, granule_key):
        """
        Write an empty granule entry to the database indicating it was processed without valid data.
        
        Parameters:
        ----------
        granule_key : str
            Granule identifier key to be written to the database.
        """
        # Initialize the database connection manager
        db_manager = DatabaseManager(db_url=self.db_path)
        engine = db_manager.get_connection()
        
        if engine:
            with engine.connect() as conn:
                # Create a DataFrame with the granule entry
                granule_entry = pd.DataFrame({
                    "granule_name": [granule_key],
                    'status': 'empty',
                    "created_date": [pd.Timestamp.utcnow()],
                })
                
                # Write the entry to the SQL table using to_sql
                granule_entry.to_sql(
                    name=self.data_info['database']['tables']['granules'],
                    con=conn,
                    index=False,
                    if_exists="append",
                )
                
                # Log the successful insertion
                logger.info(f"Empty granule entry for {granule_key} written to the database.")

