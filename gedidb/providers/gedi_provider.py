import pandas as pd
import xarray as xr
import numpy as np

from gedidb.providers.db_query import SQLQueryBuilder, GediDataBuilder


DEFAULT_DIMS = ["shot_number", "beam_name", 'absolute_time', 'geometry']


class GEDIProvider:
    def __init__(self, config_file: str, table_name: str, metadata_table:str):
        # Load the database configuration
        self.db = GediDataBuilder(data_config_file=config_file)
        self.table_name = table_name
        self.metadata_table = metadata_table

    def query_data(self, variables, geometry=None, start_time=None, end_time=None, limit=None, force=False, order_by=None):
        # Create the query builder
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
    
        # Execute the query and get both data and metadata
        result = self.db.query(query_builder=query_builder, use_geopandas=True)
    
        return result['data'], result['metadata']

    @staticmethod
    def to_xarray(df: pd.DataFrame, metadata: pd.DataFrame) -> xr.Dataset:
        # Identify dimensions and variables
        dimensions = DEFAULT_DIMS
        variables = [col for col in df.columns if col not in dimensions]
    
        # Handle lists in variables (adding an additional dimension if needed)
        max_len = 0
        for var in variables:
            if isinstance(df[var].iloc[0], list):
                max_len = max(max_len, max(df[var].apply(len)))
    
        # Step 2: Create the data_vars dictionary, padding the list-based variables
        data_vars = {}
        for var in variables:
            if isinstance(df[var].iloc[0], list):
                data_array = np.full((len(df), max_len), np.nan)  # Initialize with NaNs
                for i, row in enumerate(df[var]):
                    data_array[i, :len(row)] = row
                data_vars[var] = (["shot_number", "profile_points"], data_array)
            else:
                data_vars[var] = (["shot_number"], df[var].values)
    
        # Convert the dimensions to xarray coordinates
        coords = {dim: (["shot_number"], df[dim].values) for dim in dimensions if dim != 'geometry'}
    
        # Extract latitude and longitude from the geometry column
        longitude = np.array([geom.x for geom in df.geometry])
        latitude = np.array([geom.y for geom in df.geometry])
    
        # Include latitude and longitude in xarray coordinates
        coords['latitude'] = (["shot_number"], latitude)
        coords['longitude'] = (["shot_number"], longitude)
    
        # Create the xarray Dataset
        dataset = xr.Dataset(data_vars=data_vars, coords=coords)
    
        # Add metadata as attributes for each variable
        for var in variables:
            if var in metadata['sds_name'].values:  # Assuming 'SDS_Name' is the variable name in the metadata
                var_metadata = metadata[metadata['sds_name'] == var].iloc[0]
                dataset[var].attrs['description'] = var_metadata.get('description')
                dataset[var].attrs['units'] = var_metadata.get('units')
                dataset[var].attrs['source_table'] = var_metadata.get('source_table')
                dataset[var].attrs['created_at'] = var_metadata.get('created_at')
    
        return dataset
    
    def get_dataset(self, variables, geometry=None, start_time=None, end_time=None, 
                limit=None, force=False, order_by=None, return_type="xarray"):
        """
        Get the dataset as a Pandas DataFrame or Xarray Dataset.
        """
        # Query the data and metadata
        df, metadata = self.query_data(
            variables=variables,
            geometry=geometry,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            force=force,
            order_by=order_by
        )
    
        # Return based on the return_type argument
        if return_type == "pandas":
            return df
        elif return_type == "xarray":
            return self.to_xarray(df, metadata)
        else:
            raise ValueError(f"Invalid return_type '{return_type}'. Choose either 'pandas' or 'xarray'.")

