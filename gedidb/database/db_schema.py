from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, BigInteger, Integer, String, Float, DateTime, SmallInteger, ARRAY

from geoalchemy2 import Geometry


class Base(DeclarativeBase):
    pass


def create_columns_from_config(config):
    columns = {}
    
    # Map of column types for dynamic column creation
    column_type_map = {
        "BigInteger": BigInteger,
        "Integer": Integer,
        "String": String,
        "Float": Float,
        "DateTime": DateTime,
        "SmallInteger": SmallInteger,
        "ARRAY": ARRAY,
        "Geometry": Geometry
    }
    
    for col in config["columns"]:
        col_type = column_type_map[col["type"]]
        
        # Handle string lengths
        if col["type"] == "String":
            col_type = col_type(col.get("length", 255))
        
        # Handle geometry types
        elif col["type"] == "Geometry":
            col_type = col_type("POINT", srid=4326)
        
        # Handle ARRAY types
        elif col["type"] == "ARRAY":
            # The nested type for the ARRAY
            nested_type = column_type_map[col["nested_type"]]
            col_type = col_type(nested_type)
        
        # Create column
        columns[col["name"]] = Column(
            col_type,
            primary_key=col.get("primary_key", False),
            nullable=col.get("nullable", True),
            autoincrement=col.get("autoincrement", True)
        )

class Shots(Base):
    __tablename__ = "filtered_l2ab_l4a_shots"  # Default value, will be overridden by config

    def __init__(self):
        # Retrieve the configuration from the parent class
        self.config = self.database_structure['shots']

        # Update the table name if specified in the config
        if "table_name" in self.config:
            self.__tablename__ = self.config["table_name"]

        # Dynamically add columns based on the config
        self._setup_columns()

    def _setup_columns(self):
        columns = create_columns_from_config(self.config)
        for name, column in columns.items():
            setattr(self.__class__, name, column)
    
class Granules(Base):
    __tablename__ = "gedi_granules"  # Default value, will be overridden by config

    def __init__(self):
        # Retrieve the configuration from the parent class
        self.config = self.database_structure['granules']

        # Update the table name if specified in the config
        if "table_name" in self.config:
            self.__tablename__ = self.config["table_name"]

        # Dynamically add columns based on the config
        self._setup_columns()

    def _setup_columns(self):
        columns = create_columns_from_config(self.config)
        for name, column in columns.items():
            setattr(self.__class__, name, column)
