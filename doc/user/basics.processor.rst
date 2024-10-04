.. for doctest:
    >>> import gedidb as gdb

.. _basics.processor:

###############
Data Processing
###############

Downloading, processing, and storing GEDI data in the database
--------------------------------------------------------------

The GEDIProcessor class is a comprehensive tool designed to handle the full pipeline of downloading, processing, and writing GEDI data into a database. The processor orchestrates the following tasks:

- **Initialization**: Setting up the environment for data processing, including paths, database connections, and configurations.
- **Granule Download**: Downloading the .h5 granule files from multiple GEDI products (L2A, L2B, L4A, L4C).
- **Data Processing**: Parsing the downloaded granule files and applying filtering based on quality control flags.
- **Database Writing**: Merging the processed data from multiple products and writing the result to the PostgreSQL database.

Here’s how you can use the GEDIProcessor in your own workflow:

**Example Usage:**

This pre-written code can be used and adapted:

.. code-block:: python

    from gedidb.core.gediprocessor import GEDIProcessor

    # Initiate the GEDIProcessor
    processor = GEDIProcessor(
        data_config_file='./config_files/data_config.yml',
        sql_config_file='./config_files/db_scheme.sql',
        n_workers=4
    )

    # Process GEDI data and store it in the database
    processor.compute()

The ``.compute()`` method automatically:

- **Creates all needed tables** in the database using the schema provided in the `sql_config_file`.
- **Downloads all granules** for the given spatial and temporal parameters (defined in the data configuration file).
- **Downloads every .h5 file** for the products L2A, L2B, L4A, and L4C.
- **Applies quality filters** defined in the configuration and processes the data.
- **Merges all products** for each granule into a single parquet file.
- **Writes the data** from the parquet file into the database.

### Core Functions

**Initialization** (`__init__` method):

- The GEDIProcessor is initialized with a YAML configuration file and SQL schema file.
- Paths for downloaded granules, parquet files, and metadata are set up based on the configuration.
- A Dask cluster is set up for parallel processing, allowing the use of multiple workers for more efficient computation.

**Granule Downloading**:

- GEDIProcessor uses the `H5FileDownloader` class to download all relevant GEDI products (L2A, L2B, L4A, L4C).
- Files are stored in a user-specified directory for further processing.

**Granule Processing**:

- Each granule is parsed and processed using the `GEDIGranule` class.
- Quality filtering is applied based on pre-defined criteria for each granule.
- The different products (L2A, L2B, L4A, and L4C) are merged into a single dataset based on shot number.

**Writing to Database**:

- Processed granules are written to the PostgreSQL database in the format defined by the SQL schema.
- Both the metadata and the shot-level data are stored in separate tables.
  
### Advanced Customization

The GEDIProcessor can be customized by altering the configuration files:

- **data_config_file**: Define the spatial and temporal extents, as well as the products to process.
- **sql_config_file**: Define the schema for the database tables where the data will be stored.
- **n_workers**: Adjust the number of workers in the Dask cluster to control parallelization.

