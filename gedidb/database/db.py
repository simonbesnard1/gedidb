from gedidb.database.db_schema import Base

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
    try:
        engine = sqlalchemy.create_engine(db_url, echo=echo)
        return engine
    except SQLAlchemyError as e:
        print(f"Error creating the engine: {e}")
        return None
        
def create_tables():
    with get_engine().begin() as conn:
        Base.metadata.create_all(conn)
    return
