.. _fundamentals-processor:

###############
Data Processing
###############

The :py:class:`gedidb.GEDIProcessor` class in gediDB manages the entire workflow of downloading, processing, and storing GEDI data in either a local or s3-based tileDB. This section outlines the key functions of :py:class:`gedidb.GEDIProcessor`, example usage, core functions, and customization options for efficient GEDI data handling.

Overview of GEDIProcessor workflow
----------------------------------

The :py:class:`gedidb.GEDIProcessor` class handles the following tasks:

- **Initialization**: Sets up paths, configurations, and database connections.
- **Granule downloading**: Automatically downloads `.h5` granule files for multiple GEDI products (L2A, L2B, L4A, and L4C).
- **Data processing**: Applies quality filtering, merges products, and prepares data for storage.
- **Database writing**: Stores processed data in a tileSB array with proper metadata for easy querying.

Example usage
-------------

Below is a quick example of using the :py:class:`gedidb.GEDIProcessor` in a workflow:

.. code-block:: python

   import gedidb as gdb

   config_file = 'path/to/config_file.yml'
   geometry = 'path/to/test.geojson'
   
   # Initialize the GEDIProcessor and compute
   concurrent_engine= concurrent.futures.ThreadPoolExecutor(max_workers=10)

   # Initialize the GEDIProcessor and compute
   with gdb.GEDIProcessor(
       config_file=config_file,
       geometry=geometry,
       start_date='2020-01-01',
       end_date='2020-12-31',   
       earth_data_dir= ''/path/to/earthdata_credential_folder',
       parallel_engine=concurrent_engine, 
   ) as processor:
       processor.compute(consolidate=True)


Processing workflow
-------------------

The :py:class:`compute()` method of :py:class:`gedidb.GEDIProcessor` initiates the following workflow, using configuration settings defined in `data_config.yml`:

1. **Setup and initialization**:

   - The :py:class:`gedidb.GEDIProcessor` is initialized with the `data_config.yml` file for parameters like spatial and temporal boundaries, product details, and filtering criteria.
   - Database tables are created based on the parameters in `data_config.yml`, ensuring that the tileDB required for granule storage is in place and properly configured.
   - Paths are set up for storing granules.

2. **Granule downloading**:

   - The :py:class:`gedidb.CMRDataDownloader` class handles granule querying and ensures that all required products (e.g., L2A, L2B, L4A, L4C) are included for each granule ID. 
   - The :py:class:`gedidb.H5FileDownloader` class downloads `.h5` granule files for the GEDI products (L2A, L2B, L4A, L4C) within the spatial and temporal boundaries specified in `data_config.yml`. Each product for one granule are downloaded sequentially. 
   - Granules are stored in a designated directory for further processing.

3. **Data processing**:

   - The granule downloading as well as the processing is done in parallel, each future is processing data of a temporal tile defined in the `data_config.yml`. The number of workers from the `parallel_engine` define how many granules are processed at the same time.  
   - Each granule is parsed and processed by the :py:class:`gedidb.GEDIGranule` class, which applies quality filtering based on flags like sensitivity and degrade status. See :ref:`fundamentals-filters` for more details on the different filters applied.
   - Data from different products is merged using shot numbers as the primary key, resulting in a unified dataset per granule.

4. **Database writing**:

   - Processed data is stored in either a local or s3-based tileDB database across different fragments.
   - The processed data is split up into spatial chunks defined in the `data_config.yml` file, and each chunk is stored in a separate fragment in the database.
   - Metadata is added to the database to facilitate easy querying and retrieval of data.
   - After processing all granules, the database is optimized for efficient querying and data retrieval by consolidation. The consolidation plan can be defined in the `data_config.yml` file.

Advanced customization options
------------------------------

The :py:class:`gedidb.GEDIProcessor` class is highly configurable, allowing you to tailor data processing to your specific needs:

- **`data_config.yml`**: Modify this file to specify:

  - Database configuration details
  - Variables list for each GEDI product (L2A, L2B, L4A, L4C)
  
  For details on configuration files, refer to the :ref:`fundamentals-setup` page.

Performance considerations
--------------------------

Using parallel engines (e.g., Dask) for parallel processing enables gediDB to scale efficiently, particularly when working with large datasets. However, ensure that your system has sufficient memory for handling multiple workers and large `.h5` files. 
