from typing import Any, List, Optional, Union
import geopandas as gpd
import pandas as pd
import pyproj
from sqlalchemy import inspect
from geoalchemy2 import Geometry # required to prevent warnings on column types
import yaml
import warnings


from gedidb.utils.constants import WGS84
from gedidb.database.db_creation import DatabaseManager


class QueryPredicate:
    def __init__(self, value: Any):
        self.value = value


class Like(QueryPredicate):
    predicate = "LIKE"


class RegEx(QueryPredicate):
    predicate = "~"


class SQLQueryBuilder:
    def __init__(
        self,
        table_name: str,
        metadata_table:str,
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
        
                # Flatten the 2D array and get WKT strings
                wkt_strings = self.geometry.to_wkt().values.flatten()
                
                queries = []
                for geom in wkt_strings:
                    queries.append(f"ST_Intersects(geometry, ST_GeomFromText('{geom}', {crs.to_epsg()}))")
                
                # Combine the conditions correctly
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
        if isinstance(value, str):
            return f"'{value}'"
        return value

    def build(self) -> str:
        conditions = self._build_conditions()

        # Combining conditions
        condition = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        
        # Setting limits
        limits = f" LIMIT {self.limit}" if self.limit is not None else ""
        
        # Order by clauses
        order = "" if not self.order_by else " ORDER BY "
        order += ", ".join([(f"{x[1:]} DESC" if x[0] == "-" else x) for x in self.order_by])
        
        # Final SQL query
        sql_query = f"SELECT {', '.join(self.columns)} FROM {self.table_name}{condition}{order}{limits}"
        
        if not self.force and not condition and not limits:
            raise UserWarning("Warning! This will load the entire table. To proceed set `force`=True.")
        
        return sql_query
    
    def build_metadata_query(self, variable_names) -> str:
        """
        Build a query to fetch metadata for the given variable names from the metadata table.
        
        :param variable_names: List of variable names to fetch metadata for.
        :return: SQL query string for fetching metadata.
        """
        variable_list = ', '.join([f"'{var}'" for var in variable_names])
        return f"SELECT * FROM {self.metadata_table} WHERE SDS_Name IN ({variable_list})"


class GediDataBuilder:
    """Database connector for the GEDI DB."""

    def __init__(self, data_config_file):
        self.data_info = self.load_yaml_file(data_config_file)
        self.engine = DatabaseManager(self.data_info['database_url'], echo=False).create_engine()
        self.inspector = inspect(self.engine)

        self.allowed_cols = {}
        for table_name in self.inspector.get_table_names():
            allowed_cols = {
                col["name"] for col in self.inspector.get_columns(table_name)
            }
            self.allowed_cols[table_name] = allowed_cols
            
    @staticmethod
    def load_yaml_file(file_path: str = "field_mapping.yml") -> dict:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    def query(
        self,
        query_builder: Optional[SQLQueryBuilder] = None,
        use_geopandas: bool = False,
    ) -> dict:
        """
        Query the database and retrieve both data and metadata.

        Args:
            query_builder (Optional[SQLQueryBuilder]): The SQLQueryBuilder object to construct the SQL query.
            use_geopandas (bool): Specify if geopandas should be used for result.

        Returns:
            dict: A dictionary with 'data' containing the result dataframe and 'metadata' containing the metadata.
        """

        if query_builder is not None:
            # Build and execute the query to get the data
            sql_query = query_builder.build()
            if use_geopandas:
                data_df = gpd.read_postgis(sql_query, con=self.engine, geom_col="geometry")
            else:
                data_df = pd.read_sql(sql_query, con=self.engine)

            # Fetch the metadata for the variables in the data
            variable_names = query_builder.columns
            metadata_query = query_builder.build_metadata_query(variable_names)
            metadata_df = pd.read_sql(metadata_query, con=self.engine)
            
            return {"data": data_df, "metadata": metadata_df}
        else:
            raise ValueError("A valid SQLQueryBuilder object must be provided.")


