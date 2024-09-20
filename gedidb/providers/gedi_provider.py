# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import pandas as pd
import xarray as xr
import numpy as np
import logging
from typing import Optional, List, Union
import geopandas as gpd

from gedidb.database.db_query import SQLQueryBuilder, GediDataBuilder

# Default dimensions for the GEDI data
DEFAULT_DIMS = ["shot_number", "beam_name", 'absolute_time', 'geometry']

# Configure logging
logger = logging.getLogger(__name__)

class GEDIProvider:
    """
    Class to interact with GEDI data, query the database, and return data in Pandas or Xarray formats.
    """

    def __init__(self, config_file: str, table_name: str, metadata_table: str):
        """
        Initialize the GEDIProvider with database configuration and table information.
        
        :param config_file: Path to the configuration file.
        :param table_name: The name of the table containing GEDI data.
        :param metadata_table: The name of the metadata table for the GEDI data.
        """
        self.db = GediDataBuilder(data_config_file=config_file)
        self.table_name = table_name
        self.metadata_table = metadata_table

    def get_available_variables(self) -> pd.DataFrame:
        """
        Retrieve specific columns (sds_name, description, units) from the metadata table and return them as a dictionary.

        :return: A dictionary with sds_name as keys and description, units as values.
        """
        try:
            # Query the selected columns from the metadata table
            query = f"SELECT sds_name, description, units, product FROM {self.metadata_table};"
            selected_metadata_df = pd.read_sql(query, con=self.db.engine)

            # Convert the DataFrame to a dictionary with sds_name as the key
            metadata_dict = selected_metadata_df.set_index('sds_name')[['description', 'units', 'product']].to_dict(orient='index')
            
            return metadata_dict
        except Exception as e:
            logger.error(f"Failed to retrieve metadata as dictionary: {e}")
            raise

    def query_data(
        self, 
        variables: List[str], 
        geometry: Optional[gpd.GeoDataFrame] = None, 
        start_time: Optional[str] = None, 
        end_time: Optional[str] = None, 
        limit: Optional[int] = None, 
        force: bool = False, 
        order_by: Optional[List[str]] = None
    ) -> Union[pd.DataFrame, pd.DataFrame]:
        """
        Query GEDI data from the database using the specified variables and conditions.

        :param variables: List of variables to query.
        :param geometry: Geometry to filter spatial data.
        :param start_time: Start time for temporal filtering.
        :param end_time: End time for temporal filtering.
        :param limit: Limit the number of results.
        :param force: Force the query without conditions (may return large datasets).
        :param order_by: List of columns to order by.
        :return: A tuple containing the data and metadata as Pandas DataFrames.
        """
        logger.info(f"Querying GEDI data with variables: {variables}")
        
        query_builder = SQLQueryBuilder(
            table_name=self.table_name,
            metadata_table=self.metadata_table,
            columns=variables + DEFAULT_DIMS,
            geometry=geometry,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            force=force,
            order_by=order_by
        )

        try:
            result = self.db.query(query_builder=query_builder, use_geopandas=True)
            return result['data'], result['metadata']
        except Exception as e:
            logger.error(f"Failed to query GEDI data: {e}")
            raise

    @staticmethod
    def to_xarray(df: pd.DataFrame, metadata: pd.DataFrame) -> xr.Dataset:
        """
        Convert a Pandas DataFrame to an Xarray Dataset, with metadata attached.

        :param df: The data as a Pandas DataFrame.
        :param metadata: The metadata as a Pandas DataFrame.
        :return: The data as an Xarray Dataset.
        """
        
        # Identify dimensions and variables
        dimensions = DEFAULT_DIMS
        variables = [col for col in df.columns if col not in dimensions]
    
        # Handle lists in variables, create a new dimension for list-based variables if necessary
        max_len = 0
        for var in variables:
            if isinstance(df[var].iloc[0], list):
                max_len = max(max_len, max(df[var].apply(len)))
    
        # Create the data_vars dictionary, padding list-based variables if needed
        data_vars = {}
        for var in variables:
            if isinstance(df[var].iloc[0], list):
                data_array = np.full((len(df), max_len), np.nan)  # Initialize with NaNs
                for i, row in enumerate(df[var]):
                    data_array[i, :len(row)] = row
                data_vars[var] = (["shot_number", "profile_points"], data_array)
            else:
                data_vars[var] = (["shot_number"], df[var].values)
    
        # Convert the dimensions to Xarray coordinates
        coords = {dim: (["shot_number"], df[dim].values) for dim in dimensions if dim != 'geometry'}
    
        # Extract latitude and longitude from the geometry column
        longitude = np.array([geom.x for geom in df.geometry])
        latitude = np.array([geom.y for geom in df.geometry])
    
        # Include latitude and longitude in Xarray coordinates
        coords['latitude'] = (["shot_number"], latitude)
        coords['longitude'] = (["shot_number"], longitude)
    
        # Create the Xarray Dataset
        dataset = xr.Dataset(data_vars=data_vars, coords=coords)
    
        # Add metadata as attributes for each variable
        for var in variables:
            if var in metadata['sds_name'].values:  # Assuming 'sds_name' is the variable name in metadata
                var_metadata = metadata[metadata['sds_name'] == var].iloc[0]
                dataset[var].attrs['description'] = var_metadata.get('description', '')
                dataset[var].attrs['units'] = var_metadata.get('units', '')
                dataset[var].attrs['product'] = var_metadata.get('product', '')
                dataset[var].attrs['source_table'] = var_metadata.get('source_table', '')
                dataset[var].attrs['created_date'] = var_metadata.get('created_date', '')
    
        return dataset
    
    def get_data(
        self, 
        variables: List[str], 
        geometry: Optional[gpd.GeoDataFrame] = None, 
        start_time: Optional[str] = None, 
        end_time: Optional[str] = None, 
        limit: Optional[int] = None, 
        force: bool = False, 
        order_by: Optional[List[str]] = None, 
        return_type: str = "xarray"
    ) -> Union[pd.DataFrame, xr.Dataset]:
        """
        Get the dataset as either a Pandas DataFrame or Xarray Dataset.

        :param variables: List of variables to query.
        :param geometry: Optional spatial filter.
        :param start_time: Optional start time for temporal filtering.
        :param end_time: Optional end time for temporal filtering.
        :param limit: Limit the number of rows returned.
        :param force: If True, force the query even without conditions.
        :param order_by: Columns to order by.
        :param return_type: Specify return type, either 'pandas' or 'xarray'.
        :return: The queried data as a Pandas DataFrame or Xarray Dataset.
        """
        
        df, metadata = self.query_data(
            variables=variables,
            geometry=geometry,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            force=force,
            order_by=order_by
        )
    
        if return_type == "pandas":
            return df
        elif return_type == "xarray":
            return self.to_xarray(df, metadata)
        else:
            logger.error(f"Invalid return_type '{return_type}'. Choose either 'pandas' or 'xarray'.")
