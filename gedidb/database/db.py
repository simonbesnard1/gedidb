import sqlalchemy
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    """
    DatabaseManager class responsible for managing database connections, creating tables, 
    and executing queries using SQLAlchemy.
    """

    def __init__(self, db_url: str, echo: bool = False):
        """
        Initialize the DatabaseManager with the given database URL and optional echo flag.

        :param db_url: The database connection URL.
        :param echo: If True, SQLAlchemy will log all SQL statements. Default is False.
        """
        self.db_url = db_url
        self.echo = echo
        self.engine = None

    def create_engine(self) -> sqlalchemy.engine.Engine:
        """
        Create and return a SQLAlchemy engine for connecting to the database.

        :return: A SQLAlchemy engine instance, or None if an error occurs.
        """
        try:
            self.engine = sqlalchemy.create_engine(self.db_url, echo=self.echo)
            return self.engine
        except SQLAlchemyError as e:
            logger.error(f"Error creating the engine: {e}")
            self.engine = None
            return None

    def get_connection(self) -> sqlalchemy.engine.Engine:
        """
        Get the SQLAlchemy engine, creating it if necessary.

        :return: A SQLAlchemy engine instance, or None if an error occurs.
        """
        if self.engine is None:
            return self.create_engine()
        return self.engine

    def create_tables(self, sql_script: str):
        """
        Create tables in the database based on the provided SQL script.

        :param sql_script: The SQL script used to create the necessary tables.
        """
        engine = self.get_connection()
        if engine:
            try:
                with engine.begin() as conn:
                    conn.execute(text(sql_script))
                    logger.info("Tables created successfully.")
            except SQLAlchemyError as e:
                logger.error(f"Error creating tables: {e}")
        else:
            logger.error("Cannot create tables because the engine could not be established.")

    def execute_query(self, query: str):
        """
        Execute a raw SQL query on the database.

        :param query: The SQL query to execute.
        :return: The result of the query, or None if an error occurs.
        """
        engine = self.get_connection()
        if engine:
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    return result
            except SQLAlchemyError as e:
                logger.error(f"Error executing query: {e}")
                return None
        else:
            logger.error("Cannot execute query because the engine could not be established.")
            return None
