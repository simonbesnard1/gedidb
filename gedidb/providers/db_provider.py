from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
from gedidb.database.db import DatabaseManager

class DatabaseInfoRetriever:
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def get_table_names(self) -> list[str]:
        """
        Retrieve the names of all tables in the database.

        Returns:
        - list[str]: A list of table names.
        """
        engine = self.db_manager.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
                    return [row[0] for row in result]
            except SQLAlchemyError as e:
                print(f"Error retrieving table names: {e}")
                return []
        else:
            print("Cannot retrieve table names because the engine could not be established.")
            return []

    def get_table_schema(self, table_name: str) -> dict:
        """
        Retrieve the schema of a specific table.

        Parameters:
        - table_name (str): The name of the table.

        Returns:
        - dict: A dictionary representing the table schema.
        """
        engine = self.db_manager.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"))
                    return {row[0]: row[1] for row in result}
            except SQLAlchemyError as e:
                print(f"Error retrieving schema for table {table_name}: {e}")
                return {}
        else:
            print("Cannot retrieve table schema because the engine could not be established.")
            return {}

    def get_row_count(self, table_name: str) -> int:
        """
        Retrieve the number of rows in a specific table.

        Parameters:
        - table_name (str): The name of the table.

        Returns:
        - int: The number of rows in the table.
        """
        engine = self.db_manager.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    return result.scalar()
            except SQLAlchemyError as e:
                print(f"Error retrieving row count for table {table_name}: {e}")
                return 0
        else:
            print("Cannot retrieve row count because the engine could not be established.")
            return 0

    def get_column_data(self, table_name: str, columns) -> list[dict]:
        """
        Retrieve the data of specific columns from a table.

        Parameters:
        - table_name (str): The name of the table.
        - columns (Union[str, list[str]]): A column name or a list of column names to retrieve data from.

        Returns:
        - list[dict]: A list of dictionaries where each dictionary represents a row.
        """
        if isinstance(columns, str):
            columns = [columns]

        engine = self.db_manager.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    columns_str = ", ".join(columns)
                    result = conn.execute(text(f"SELECT {columns_str} FROM {table_name}"))
                    data = {column: [] for column in columns}
                    for row in result:
                        for column, value in zip(columns, row):
                            data[column].append(value)
                    return data
            except SQLAlchemyError as e:
                print(f"Error retrieving data for columns {columns} from table {table_name}: {e}")
                return []
        else:
            print("Cannot retrieve column data because the engine could not be established.")
            return []
