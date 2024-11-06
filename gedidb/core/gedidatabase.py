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
from enum import Enum

from gedidb.database.db import DatabaseManager

# Constants for product types
PRODUCT_TYPES = ['L2A', 'L2B', 'L4A', 'L4C']


class GranuleStatus(Enum):
    EMPTY = "empty"
    PROCESSED = "processed"

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
<<<<<<< HEAD
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
=======
        config : dict
            Configuration dictionary.
        """
        self.config = config
        self.scalar_array_uri = os.path.join(config['tiledb']['s3_bucket'], 'scalar_array_uri')
        self.profile_array_uri = os.path.join(config['tiledb']['s3_bucket'], 'profile_array_uri')
        self.overwrite = config['tiledb']['overwrite']
        self.variables_config = self._load_variables_config(config)
        
        # Initialize boto3 session and TileDB context for S3
        session = boto3.Session()
        creds = session.get_credentials()
        self.ctx = tiledb.Ctx({
            "vfs.s3.aws_access_key_id": creds.access_key,
            "vfs.s3.aws_secret_access_key": creds.secret_key,
            "vfs.s3.endpoint_override": config['tiledb']['endpoint_override'],
            "vfs.s3.region": "eu-central-1"
        })

    def _create_arrays(self) -> None:
        """Define and create scalar and profile TileDB arrays based on configuration."""
        self._create_array(self.scalar_array_uri, scalar=True)
        self._create_array(self.profile_array_uri, scalar=False)
        self._add_variable_metadata()

    def _create_array(self, uri: str, scalar: bool) -> None:
        """Creates a TileDB array (scalar or profile) with schema based on configuration."""
        if tiledb.array_exists(uri, ctx=self.ctx):
            if self.overwrite:
                tiledb.remove(uri, ctx=self.ctx)
                logger.info(f"Overwritten existing  {'scalar' if scalar else 'profile'} TileDB array at {uri}")
            else:
                logger.info(f"{'scalar' if scalar else 'profile'} TileDB array already exists at {uri}. Skipping creation.")
                return
        
        domain = self._create_domain(scalar)
        attributes = self._create_attributes(scalar)
        schema = tiledb.ArraySchema(domain=domain, attrs=attributes, sparse=True)
        tiledb.Array.create(uri, schema, ctx=self.ctx)
        
    def _create_domain(self, scalar: bool) -> tiledb.Domain:
        """Creates TileDB domain based on spatial, temporal, and profile dimensions from config."""
        spatial_range = self.config.get("tiledb", {}).get("spatial_range", {})
        lat_min, lat_max = spatial_range.get("lat_min"), spatial_range.get("lat_max")
        lon_min, lon_max = spatial_range.get("lon_min"), spatial_range.get("lon_max")
        time_range = self.config.get("tiledb", {}).get("time_range", {})
        time_min = self._datetime_to_timestamp(time_range.get("start_time"))
        time_max = self._datetime_to_timestamp(time_range.get("end_time"))
        
        # Validate ranges to prevent undefined domain errors
        if None in (lat_min, lat_max, lon_min, lon_max, time_min, time_max):
            raise ValueError("Spatial and temporal ranges must be fully specified in the configuration.")
        
        dimensions = [
            tiledb.Dim("latitude", domain=(lat_min, lat_max), tile=1, dtype="float64"),
            tiledb.Dim("longitude", domain=(lon_min, lon_max), tile=1, dtype="float64"),
            tiledb.Dim("time", domain=(time_min, time_max), tile=int(1e6), dtype="int64")
        ]
    
        if not scalar:
            max_profile_length = self.config['tiledb'].get('max_profile_length', 100)
            dimensions.append(tiledb.Dim("profile_point", domain=(0, max_profile_length), tile=None, dtype="int32"))
        
        return tiledb.Domain(*dimensions)


    def _create_attributes(self, scalar: bool) -> List[tiledb.Attr]:
        """Creates attributes based on scalar or profile configuration."""
        attributes = []
        for var_name, var_info in self.variables_config.items():
            dtype = var_info.get("dtype", "float64")
            is_profile = var_info.get("is_profile", False)
            
            if scalar and not is_profile:
                attributes.append(tiledb.Attr(name=var_name, dtype=dtype))
            elif not scalar and is_profile:
                attributes.append(tiledb.Attr(name=var_name, dtype=dtype))
    
        # Add shot_number to the profile attributes if it's not already included
        if not scalar:
            attributes.append(tiledb.Attr(name="shot_number", dtype="int64"))  # Adjust dtype if necessary
    
        return attributes

    def _add_variable_metadata(self) -> None:
>>>>>>> optimise tiledb schema with two arrays
        """
        Create the database schema using the provided SQL script.
        """
<<<<<<< HEAD
        db_manager = DatabaseManager(db_url=self.db_path)
        db_manager.create_tables(sql_script=self.sql_script)

    def _write_db(self, input):
=======
        # Add metadata to scalar array
        with tiledb.open(self.scalar_array_uri, mode="w", ctx=self.ctx) as array:
            array.meta["array_type"] = "scalar"
            for var_name, var_info in self.variables_config.items():
                if not var_info.get('is_profile', False):
                    try:
                        units = var_info.get("units", "unknown")
                        description = var_info.get("description", "No description available")
                        dtype = var_info.get("dtype", "unknown")
                        product_level = var_info.get("product_level", "unknown")
                        
                        array.meta[f"{var_name}.units"] = units
                        array.meta[f"{var_name}.description"] = description
                        array.meta[f"{var_name}.dtype"] = dtype
                        array.meta[f"{var_name}.product_level"] = product_level 
                    except KeyError as e:
                        logger.warning(f"Missing metadata key for {var_name}: {e}")
    
        # Add metadata to profile array
        with tiledb.open(self.profile_array_uri, mode="w", ctx=self.ctx) as array:
            array.meta["array_type"] = "profile"
            for var_name, var_info in self.variables_config.items():
                if var_info.get('is_profile', False):
                    try:
                        units = var_info.get("units", "unknown")
                        description = var_info.get("description", "No description available")
                        dtype = var_info.get("dtype", "unknown")
                        product_level = var_info.get("product_level", "unknown")
                        
                        array.meta[f"{var_name}.units"] = units
                        array.meta[f"{var_name}.description"] = description
                        array.meta[f"{var_name}.dtype"] = dtype
                        array.meta[f"{var_name}.product_level"] = product_level 
                    except KeyError as e:
                        logger.warning(f"Missing metadata key for {var_name}: {e}")

                
    def write_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write the parsed GEDI granule data to the global TileDB arrays.
    
        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data, with variable names matching the configuration.
    
        Raises:
        -------
        ValueError if required dimension data is missing.
        """
        # Check if all required dimensions are present
        missing_dims = [dim for dim in self.config["tiledb"]['dimensions'] if dim not in granule_data]
        if missing_dims:
            raise ValueError(f"Granule data is missing required dimension data: {missing_dims}")
    
        # Prepare and write scalar data
        self.write_scalar_granule(granule_data)
    
        # Prepare and write profile data
        profile_vars = [var_name for var_name, var_info in self.variables_config.items() if var_info.get('is_profile', False)]
        if profile_vars:
            self.write_profile_granule(granule_data, profile_vars)
        
    def write_scalar_granule(self, granule_data: pd.DataFrame) -> None:
        """
        Write scalar data to the scalar TileDB array.
    
        Parameters:
        ----------
        granule_data : pd.DataFrame
            DataFrame containing the granule data.
>>>>>>> optimise tiledb schema with two arrays
        """
        Write data to the database.

        This function is intended to be used as a Dask task. It initializes a 
        new database connection inside the task.

        Parameters:
        ----------
        input : tuple
            Tuple containing the granule key, output file path, and included files.
        """
<<<<<<< HEAD
        if input is None:
            return

        granule_key, outfile_path, included_files = input

        # Load the data to write
        granule_entry = pd.DataFrame({
            "granule_name": [granule_key],
            'status': GranuleStatus.PROCESSED.value,
            "created_date": [pd.Timestamp.utcnow()],
        })
=======
        
        # Convert 'time' to integer timestamps in microseconds
        time_array = (pd.to_datetime(granule_data['time']).astype('int64') // 1000).values
        lat_array = granule_data['latitude'].values
        lon_array = granule_data['longitude'].values
        shot_numbers = granule_data['shot_number'].values
    
        # Determine maximum profile length across all observations
        max_profile_length = granule_data[profile_vars].map(len).values.max()
        num_observations = len(granule_data)
        
        # Initialize data arrays with NaNs to max profile length
        data_arrays = {var_name: np.full((num_observations, max_profile_length), np.nan, dtype=np.float32) for var_name in profile_vars}
    
        # Fill each profile variable with padded data
        for var_name in profile_vars:
            for i, var_data in enumerate(granule_data[var_name]):
                data_arrays[var_name][i, :len(var_data)] = var_data  # Pad as needed
        
        # Flatten data arrays for TileDB input
        for var_name in data_arrays:
            data_arrays[var_name] = data_arrays[var_name].flatten()
        
        # Create coordinate arrays
        latitudes = np.repeat(lat_array, max_profile_length)
        longitudes = np.repeat(lon_array, max_profile_length)
        times = np.repeat(time_array, max_profile_length)
        profile_points = np.tile(np.arange(max_profile_length), num_observations)
        
        # Flatten shot_numbers to align with profile data and add it as a variable
        shot_numbers_repeated = np.repeat(shot_numbers, max_profile_length)
        data_arrays['shot_number'] = shot_numbers_repeated
        
        coords = {
            'latitude': latitudes,
            'longitude': longitudes,
            'time': times,
            'profile_point': profile_points
        }
        
        return coords, data_arrays
>>>>>>> optimise tiledb schema with two arrays

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
