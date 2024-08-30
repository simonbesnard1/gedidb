from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import BigInteger, Integer, String, Float, DateTime, SmallInteger, ARRAY
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declared_attr


class Base(DeclarativeBase):
    pass

class DynamicSchemaBuilder:
    def __init__(self, config_file):
        self.config = self.config_file

    def _create_columns(self, columns_config):
        """Dynamically create columns based on the configuration."""
        columns = {}
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

        for col_name, col_attributes in columns_config.items():
            col_type = column_type_map[col_attributes["type"]]

            # Handle additional parameters based on type
            if col_attributes["type"] == "String":
                col_type = col_type(col_attributes.get("length", 255))
            elif col_attributes["type"] == "Geometry":
                col_type = col_type(col_attributes.get("geometry_type", "POINT"), srid=col_attributes.get("srid", 4326))
            elif col_attributes["type"] == "ARRAY":
                nested_type = column_type_map[col_attributes["nested_type"]]
                col_type = col_type(nested_type)

            columns[col_name] = mapped_column(
                col_type,
                primary_key=col_attributes.get("primary_key", False),
                nullable=col_attributes.get("nullable", True),
                autoincrement=col_attributes.get("autoincrement", True)
            )

        return columns

    def build_schema(self):
        """Build and return SQLAlchemy model classes for both shots and granules."""
        models = {}

        for table_type, config in self.config.items():
            columns = self._create_columns(config['columns'])
            class_name = f"{table_type.capitalize()}_{config['table_name'].capitalize()}"

            class DynamicSchema(Base):
                __tablename__ = config['table_name']

                @declared_attr
                def __table_args__(cls):
                    return {'extend_existing': True}

                # Add the dynamically created columns to the class
                locals().update(columns)

            # Dynamically create the model class with a unique name
            DynamicSchema.__name__ = class_name
            models[table_type] = DynamicSchema

        return models
