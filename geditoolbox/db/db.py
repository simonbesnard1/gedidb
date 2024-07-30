import sqlalchemy

# TODO: config
def get_db_conn():
    return sqlalchemy.create_engine(f"postgresql://glmadmin:SimonGFZ@mefe27:5434/glmdb", echo=True)
