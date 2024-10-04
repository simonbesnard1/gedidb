.. _basics.processor:

###############
Data Processing
###############

Downloading, processing, and storing GEDI data in the database
--------------------------------------------------------------

The `GEDIProcessor` class in **gediDB** is designed to manage the entire workflow for handling GEDI data. This includes downloading granules, processing the data, and storing the results in a PostgreSQL database. Below is an outline of its functionality:

- **Initialization**: Set up paths, configurations, and database connections for efficient data processing.
- **Granule Download**: Automatically download the `.h5` granule files for multiple GEDI products (L2A, L2B, L4A, and L4C).
- **Data Processing**: Apply quality filtering, merge data from different products, and prepare for storage.
- **Database Writing**: Store processed data in the PostgreSQL/PostGIS database with proper metadata.

Example usage
-------------

Here’s how you can use the `GEDIProcessor` in your workflow:

.. code-block:: python

    from gedidb.core.gediprocessor import GEDIProcessor

    # Initialize the GEDIProcessor
    processor = GEDIProcessor(
        data_config_file='./config_files/data_config.yml',
        sql_config_file='./config_files/db_scheme.sql',
        n_workers=4  # Adjust the number of parallel workers
    )

    # Process GEDI data and store it in the database
    processor.compute()

When you call the ``.compute()`` method, the processor handles the entire pipeline:

- **Table creation**: Automatically generates the necessary tables in your database based on the schema file.
- **Granule downloading**: Downloads the specified granules within the temporal and spatial parameters defined in the `data_config.yml`.
- **Product merging**: Merges the `.h5` files for products (L2A, L2B, L4A, L4C) into a unified parquet file.
- **Quality filtering**: Applies predefined quality filters to the data for clean results.
- **Database writing**: Saves the processed data in PostgreSQL/PostGIS format.

Core Functions
##############

1. **Initialization (__init__ method)**

The processor is initialized with two configuration files:

  - ``data_config.yml``: Specifies key parameters such as spatial and temporal boundaries, product information, and filtering criteria.
  - ``db_scheme.sql``: Provides the database schema for the tables where data will be stored.

It sets up paths for storing downloaded granules, parquet files, and metadata, and initiates a Dask cluster for parallel processing based on the number of workers specified (``n_workers``).

2. **Granule Downloading**

   - The processor utilizes the ``H5FileDownloader`` class to download all necessary GEDI products, including L2A, L2B, L4A, and L4C.
   - Granules are stored in a predefined download directory for subsequent processing.

3. **Granule Processing**

   Each granule is parsed and processed by the `GEDIGranule` class, which:
   
   - Applies filters based on quality flags (e.g., sensitivity, degrade flag).
   - Merges data across products (L2A, L2B, L4A, L4C) using shot number as the primary key.

   The processed data is converted into a parquet file for efficient storage and retrieval.

4. **Writing to Database**:

   The processed data is written to the PostgreSQL/PostGIS database in two main tables:

     - **Granule metadata**: Stores information about the granule itself (e.g., granule ID, spatial extent, processing time).
     - **Shot-level data**: Contains detailed shot-level information from the merged products, allowing for robust geospatial analysis.

Example Workflow
################

To give you an idea of the workflow, here’s a quick breakdown of what happens when `processor.compute()` is executed:

1. **Setup and Table Creation**:
   The database tables are set up as per the SQL schema file, ensuring that all required tables for storing metadata and shot-level data are ready.
   
2. **Granule Downloading**:
   Using the spatial and temporal parameters defined in the `data_config.yml` file, the relevant granules are downloaded from NASA’s CMR (Common Metadata Repository).

3. **Data Processing**:
   Each granule goes through the filtering process. Invalid data (based on quality flags) is excluded. The L2A, L2B, L4A, and L4C products are merged into a single dataset for each granule.

4. **Storage**:
   The final parquet files are written to the PostgreSQL/PostGIS database, making them available for future queries via the `GEDIProvider` class.

Advanced Customization
######################

The GEDIProcessor class is highly configurable through its input files and parameters:

- **`data_config.yml`**: Modify this file to specify:
  - Spatial extent (region of interest)
  - Time range (start and end dates)
  - Quality filters (such as sensitivity thresholds)
  
  You can find the example `data_config.yml` file in the :ref:`basics.setup` section.

- **`db_scheme.sql`**: Update the schema to fit your specific database structure, whether you're working with existing datasets or customizing it for new research.

- **`n_workers`**: Adjust the number of parallel processes to match the computational resources available. The higher the number, the more granules are processed concurrently.

Performance Considerations
##########################

Using Dask for parallel processing allows gediDB to scale easily, especially when dealing with large datasets. However, make sure your system has sufficient memory to handle multiple workers and large `.h5` files. You can always adjust the `n_workers` parameter based on your system’s capabilities.

By combining spatial, temporal, and quality filtering, GEDIProcessor provides a streamlined and efficient way to handle GEDI data, ensuring researchers can focus on analysis rather than data wrangling.

