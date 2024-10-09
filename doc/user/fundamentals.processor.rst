.. _fundamentals-processor:

###############
Data Processing
###############

The :py:class:`gedidb.GEDIProcessor` class in gediDB manages the entire workflow of downloading, processing, and storing GEDI data in a PostgreSQL database. This section outlines the key functions of :py:class:`gedidb.GEDIProcessor`, example usage, core functions, and customization options for efficient GEDI data handling.

Overview of GEDIProcessor workflow
----------------------------------

The :py:class:`gedidb.GEDIProcessor` class handles the following tasks:

- **Initialization**: Sets up paths, configurations, and database connections.
- **Granule downloading**: Automatically downloads `.h5` granule files for multiple GEDI products (L2A, L2B, L4A, and L4C).
- **Data processing**: Applies quality filtering, merges products, and prepares data for storage.
- **Database writing**: Stores processed data in PostgreSQL with proper metadata for easy querying.

Example usage
-------------

Below is a quick example of using the :py:class:`gedidb.GEDIProcessor` in a workflow:

.. code-block:: python

   import gedidb as gdb

   data_config_file = "./config_files/data_config.yml"
   sql_config_file = './config_files/db_scheme.sql'
   n_workers = 5

   if __name__ == "__main__":

       # Initialize the GEDIProcessor and compute
       with gdb.GEDIProcessor(
           data_config_file=data_config_file,
           sql_config_file=sql_config_file,
           n_workers=n_workers
       ) as processor:
           processor.compute()


Processing workflow
-------------------

The ``compute()`` method of :py:class:`gedidb.GEDIProcessor` initiates the following workflow, using configuration settings defined in `data_config.yml` and `db_scheme.sql`:

1. **Setup and initialization**:

   - The `GEDIProcessor` is initialized with the `data_config.yml` file for parameters like spatial and temporal boundaries, product details, and filtering criteria.
   - Database tables are created based on the schema in `db_scheme.sql`, ensuring that all tables required for granule metadata, shot-level data, and additional metadata are in place.
   - Paths are set up for storing granules, parquet files, and metadata, and a Dask cluster is initialized for parallel processing based on the specified `n_workers`.

2. **Granule downloading**:

   - The `H5FileDownloader` class downloads `.h5` granule files for the GEDI products (L2A, L2B, L4A, L4C) within the spatial and temporal boundaries specified in `data_config.yml`.
   - Granules are stored in a designated directory for further processing.

3. **Data processing**:

   - Each granule is parsed and processed by the `GEDIGranule` class, which applies quality filtering based on flags like sensitivity and degrade status. See :ref:`fundamentals-filters` for more details on the different filters applied. 
   - Data from different products is merged using shot numbers as the primary key, resulting in a unified dataset per granule.
   - The processed data is saved as a parquet file for efficient storage and retrieval.

4. **Database writing**:

   - Processed data is stored in the PostgreSQL/PostGIS database across main tables:

     - **Granule Table**: Contains granule-level information (e.g., ID, spatial extent, processing time).
     - **Shot Table**: Stores detailed shot-level data, supporting robust geospatial analysis.
     - **Metadata Table**: Includes descriptive metadata for interpreting stored variables.

Advanced customization options
------------------------------

The :py:class:`gedidb.GEDIProcessor` class is highly configurable, allowing you to tailor data processing to your specific needs:

- **`data_config.yml`**: Modify this file to specify:

  - Spatial extent (region of interest)
  - Time range (start and end dates)
  - Quality filters (e.g., sensitivity thresholds)
  

- **`db_scheme.sql`**: Update the schema to fit your specific database structure, especially if you’re working with existing datasets or need custom tables.


  For details on configuration files, refer to the :ref:`fundamentals-setup` page.

Performance considerations
--------------------------

Using Dask for parallel processing enables gediDB to scale efficiently, particularly when working with large datasets. However, ensure that your system has sufficient memory for handling multiple workers and large `.h5` files. Adjust the `n_workers` parameter as needed to match your system’s capabilities.