import os
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError

def get_db_conn(db_url: str = None, echo: bool = False):
    """
    Creates a SQLAlchemy engine for connecting to the database.

    Parameters:
    - db_url (str): The database URL. If not provided, it tries to retrieve from environment variables.
    - echo (bool): If True, the engine will log all statements.

    Returns:
    - sqlalchemy.engine.Engine: A SQLAlchemy engine instance.
    """
    if db_url is None:
        db_user = os.getenv('DB_USER', 'default_user')
        db_password = os.getenv('DB_PASSWORD', 'default_password')
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'default_db')
        
        db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    try:
        engine = sqlalchemy.create_engine(db_url, echo=echo)
        return engine
    except SQLAlchemyError as e:
        print(f"Error creating the engine: {e}")
        return None
