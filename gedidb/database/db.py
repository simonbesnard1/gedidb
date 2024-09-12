import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

class DatabaseManager:
    def __init__(self, db_url: str, echo: bool = False):
        """
        Initialize the DatabaseManager with the given database URL and echo flag.

        Parameters:
        - db_url (str): The database connection URL.
        - echo (bool): If True, the engine will log all SQL statements.
        """
        self.db_url = db_url
        self.echo = echo
        self.engine = None

    def create_engine(self):
        """
        Create and return a SQLAlchemy engine for connecting to the database.

        Returns:
        - sqlalchemy.engine.Engine: A SQLAlchemy engine instance.
        """
        try:
            self.engine = sqlalchemy.create_engine(self.db_url, echo=self.echo)
            return self.engine
        except SQLAlchemyError as e:
            print(f"Error creating the engine: {e}")
            self.engine = None
            return None

    def get_connection(self):
        """
        Get the SQLAlchemy engine, creating it if necessary.

        Returns:
        - sqlalchemy.engine.Engine: A SQLAlchemy engine instance.
        """
        if self.engine is None:
            return self.create_engine()
        return self.engine

    def create_tables(self, sql_script):
        """
        Create tables in the database based on the models defined in Base metadata.

        Uses the SQLAlchemy engine to connect to the database and issue the necessary
        SQL commands to create the tables.
        """
        engine = self.get_connection()
        if engine:
            try:
                
                with engine.begin() as conn:
                    # Base.metadata.create_all(conn)
                    conn.execute(text(sql_script))

            except SQLAlchemyError as e:
                print(f"Error creating tables: {e}")
        else:
            print("Cannot create tables because the engine could not be established.")

    def execute_query(self, query: str):
        """
        Execute a raw SQL query on the database.

        Parameters:
        - query (str): The SQL query to execute.

        Returns:
        - ResultProxy: The result of the query.
        """
        engine = self.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    return result
            except SQLAlchemyError as e:
                print(f"Error executing query: {e}")
                return None
        else:
            print("Cannot execute query because the engine could not be established.")
            return None
