# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from typing import Any, List, Optional, Union
import geopandas as gpd
import pandas as pd
import pyproj
from sqlalchemy import inspect
from geoalchemy2 import Geometry  # Required to prevent warnings on column types
import yaml
import warnings
import logging

from gedidb.utils.constants import WGS84
from gedidb.database.db import DatabaseManager

# Configure logging
logger = logging.getLogger(__name__)

class QueryPredicate:
    """Base class to represent query predicates such as LIKE or RegEx."""
    
    def __init__(self, value: Any):
        self.value = value

class Like(QueryPredicate):
    predicate = "LIKE"

class RegEx(QueryPredicate):
    predicate = "~"


class SQLQueryBuilder:
    """A class to construct SQL queries with optional spatial and temporal filters."""

    def __init__(
        self,
        table_name: str,
        metadata_table: str,
        columns: Union[str, List[str]] = "*",
        geometry: Optional[gpd.GeoDataFrame] = None,
        crs: str = WGS84,
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: Optional[int] = None,
        force: bool = False,
        order_by: List[str] = [],
        **filters
    ):
        self.table_name = table_name
        self.metadata_table = metadata_table
        self.columns = columns
        self.geometry = geometry
        self.crs = crs
        self.start_time = start_time
        self.end_time = end_time
        self.limit = limit
        self.force = force
        self.order_by = order_by
        self.filters = filters

    def _build_conditions(self) -> List[str]:
        """Build SQL conditions for spatial, temporal, and filter-based queries."""
        conditions = []

        # Temporal conditions
        if self.start_time and self.end_time:
            conditions.append(f"(absolute_time BETWEEN '{self.start_time}' AND '{self.end_time}')")

        # Spatial conditions
        if self.geometry is not None:
            crs = pyproj.CRS.from_user_input(self.crs)
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="__len__ for multi-part geometries is deprecated and will be removed in Shapely 2.0",
                )

                # Flatten the geometry to WKT strings and create spatial conditions
                wkt_strings = self.geometry.to_wkt().values.flatten()
                queries = [f"ST_Intersects(geometry, ST_GeomFromText('{geom}', {crs.to_epsg()}))" for geom in wkt_strings]
                conditions.append(f"({' OR '.join(queries)})")

        # Filter conditions
        for column, value in self.filters.items():
            comparator = "="
            if isinstance(value, list):
                comparator = "IN"
                value = [self._escape_value(v) for v in value]
            elif isinstance(value, QueryPredicate):
                comparator = value.predicate
                value = self._escape_value(value.value)
            else:
                value = self._escape_value(value)
            conditions.append(f"({column} {comparator} {value})")

        return conditions

    @staticmethod
    def _escape_value(value: Any) -> Any:
        """Escape string values for use in SQL queries."""
        if isinstance(value, str):
            return f"'{value}'"
        return value

    def build(self) -> str:
        """Build the final SQL query with conditions, limits, and ordering."""
        # Check if 'all' is specified for columns, meaning select all columns
        if self.columns == 'all':
            columns = '*'
        else:
            # If specific columns are provided, join them into a string
            columns = ', '.join(self.columns)
        
        # Build conditions, limits, and ordering as before
        conditions = self._build_conditions()
        condition = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        limits = f" LIMIT {self.limit}" if self.limit is not None else ""
        order = " ORDER BY " + ", ".join([(f"{x[1:]} DESC" if x[0] == "-" else x) for x in self.order_by]) if self.order_by else ""
    
        # Construct the final SQL query
        sql_query = f"SELECT {columns} FROM {self.table_name}{condition}{order}{limits}"
    
        # Add safety check if no conditions, limits, or force
        if not self.force and not condition and not limits:
            raise UserWarning("Warning! This will load the entire table. To proceed set `force`=True.")
    
        # Log and return the query
        logger.debug(f"Generated SQL query: {sql_query}")
        return sql_query

    def build_metadata_query(self, variable_names) -> str:
        """
        Build a query to fetch metadata for the given variable names from the metadata table.

        :param variable_names: List of variable names to fetch metadata for.
        :return: SQL query string for fetching metadata.
        """
        variable_list = ', '.join([f"'{var}'" for var in variable_names])
        metadata_query = f"SELECT * FROM {self.metadata_table} WHERE SDS_Name IN ({variable_list})"
        logger.debug(f"Generated metadata query: {metadata_query}")
        return metadata_query


class GediDataBuilder:
    """Database connector for querying GEDI data and metadata."""

    def __init__(self, data_config_file: str):
        """
        Initialize the GediDataBuilder with a data configuration file.
        
        :param data_config_file: Path to the YAML configuration file.
        """
        self.data_info = self.load_yaml_file(data_config_file)
        self.engine = DatabaseManager(self.data_info['database']['url'], echo=False).create_engine()
        self.inspector = inspect(self.engine)
        self.allowed_cols = {
            table_name: {col["name"] for col in self.inspector.get_columns(table_name)}
            for table_name in self.inspector.get_table_names()
        }
        
    @staticmethod
    def load_yaml_file(file_path: str) -> dict:
        """
        Load the YAML configuration file.
        
        :param file_path: Path to the YAML file.
        :return: Parsed YAML content as a dictionary.
        """
        try:
            with open(file_path, 'r') as file:
                data = yaml.safe_load(file)
                return data
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {file_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML file: {file_path}. Error: {e}")
            raise

    def query(self, query_builder: Optional[SQLQueryBuilder] = None, use_geopandas: bool = False) -> dict:
        """
        Query the database and retrieve both data and metadata.

        :param query_builder: The SQLQueryBuilder object to construct the SQL query.
        :param use_geopandas: Specify if geopandas should be used for result.
        :return: A dictionary with 'data' containing the result dataframe and 'metadata' containing the metadata.
        """
        if query_builder is None:
            raise ValueError("A valid SQLQueryBuilder object must be provided.")
        
        # Build and execute the SQL query
        sql_query = query_builder.build()

        if use_geopandas:
            data_df = gpd.read_postgis(sql_query, con=self.engine, geom_col="geometry")
        else:
            data_df = pd.read_sql(sql_query, con=self.engine)

        # Fetch metadata for the variables
        variable_names = query_builder.columns
        metadata_query = query_builder.build_metadata_query(variable_names)
        metadata_df = pd.read_sql(metadata_query, con=self.engine)

        return {"data": data_df, "metadata": metadata_df}
