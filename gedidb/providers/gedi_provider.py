import pandas as pd
import xarray as xr
import numpy as np

from gedidb.providers.db_query import SQLQueryBuilder, GediDatabase


class GEDIProvider:
    def __init__(self, config_file: str, table_name: str):
        # Load the database configuration
        self.db = GediDatabase(data_config_file=config_file)
        self.table_name = table_name

    def query_data(self, columns, geometry=None, start_time=None, end_time=None, limit=None, force=False, order_by=None):
        # Create the query builder
        query_builder = SQLQueryBuilder(
            table_name=self.table_name,
            columns=columns,
            geometry=geometry,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            force=force,
            order_by=order_by
        )

        # Execute the query
        return self.db.query(self.table_name, query_builder=query_builder, use_geopandas=True)

    def to_xarray(self, df: pd.DataFrame) -> xr.Dataset:
        # Identify dimensions and variables
        dimensions = ["shot_number", "beam_name", 'absolute_time', 'geometry']
        variables = [col for col in df.columns if col not in dimensions]

        # Handle lists in variables (adding an additional dimension if needed)
        data_vars = {}
        for var in variables:
            if isinstance(df[var].iloc[0], list):
                # Create an additional dimension (e.g., profile_points)
                max_len = max(df[var].apply(len))  # Find the max length of the lists
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

        return dataset

    def get_dataset(self, columns, geometry=None, start_time=None, end_time=None, limit=None, force=False, order_by=None) -> xr.Dataset:
        df = self.query_data(
            columns=columns,
            geometry=geometry,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            force=force,
            order_by=order_by
        )

        return self.to_xarray(df)