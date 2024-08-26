from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship
from sqlalchemy import BigInteger, Integer, String, Float, DateTime, SmallInteger, ARRAY, ForeignKey
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declared_attr

class Base(DeclarativeBase):
    pass

class DynamicSchemaBuilder:
    def __init__(self, config):
        self.config = config

    def _create_columns(self, columns_config, versioned=False):
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

        # If versioning is enabled, add a foreign key to the version table
        if versioned:
            columns['version_id'] = mapped_column(Integer, ForeignKey('gedi_versions.id'))

        return columns

    def build_schema(self):
        """Build and return SQLAlchemy model classes for both shots and granules."""
        models = {}

        # Create the Version table
        class GediVersion(Base):
            __tablename__ = "gedi_versions"
            id = mapped_column(Integer, primary_key=True)
            version = mapped_column(String(10), nullable=False)
            release_date = mapped_column(DateTime, nullable=False)
            description = mapped_column(String(255))

            shots = relationship("Shot", back_populates="version")
            granules = relationship("Granule", back_populates="version")

        models['GediVersion'] = GediVersion

        # Create the Shot and Granule tables with a foreign key to the version table
        for table_type, config in self.config.items():
            columns = self._create_columns(config['columns'], versioned=True)
            class_name = f"{table_type.capitalize()}_{config['table_name'].capitalize()}"

            class DynamicSchema(Base):
                __tablename__ = config['table_name']

                @declared_attr
                def __table_args__(cls):
                    return {'extend_existing': True}

                # Add the dynamically created columns to the class
                locals().update(columns)

                # Relationships
                version = relationship("GediVersion", back_populates=f"{table_type}s")

            # Dynamically create the model class with a unique name
            DynamicSchema.__name__ = class_name
            models[table_type] = DynamicSchema

        return models
