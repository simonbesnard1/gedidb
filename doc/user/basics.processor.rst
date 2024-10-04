.. for doctest:
    >>> import gedidb as gdb

.. _basics.processor:

###############
Data processing
###############

Downloading, processing and storing GEDI data to the database
-------------------------------------------------------------

This pre-written code can be used and adapted. ::
    from gedidb.database.db_builder import GEDIGranuleProcessor

    #%% Initiate database builder
    database_builder = GEDIGranuleProcessor(
    data_config_file = './config_files/data_config.yml',
    sql_config_file='./config_files/db_scheme.sql'
    )

    #%% Process GEDI data
    database_builder.compute()

The ``.compute()`` method automatically:

- creates all needed tables in the database
- fetches all granules for the given spatial and temporal parameters
- downloads every .h5 file for the products L2A, L2B, L4A & L4C
- extracts the predefined data in ``data_config`` and merges all products into a ``.parquet`` file
- writes the contents of the ``.parquet`` file into the database
