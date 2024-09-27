.. for doctest:
    >>> import gedidb as gdb

.. _database.connect:

###################
Database connection
###################

Connecting to the Database in `gedidb`
--------------------------------------

Once the database is set up, you can connect to it within `gedidb`. Here's an example:

.. code-block:: python

    import gedidb as gdb

    # Provide the necessary connection details
    connection_config = {
        "dbname": "gedi_db",
        "user": "gedi_user",
        "password": "your_password",
        "host": "localhost",
        "port": 5432
    }

    # Create a connection to the database
    gedi_provider = gdb.GEDIProvider(config_file="path/to/your/data_config.yml", 
                                     shot_table="your_shot_table", 
                                     metadata_table="your_metadata_table")

Ensure that the connection details match those of your PostgreSQL/PostGIS setup.
