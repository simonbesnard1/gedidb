.. _overview:

################
Quick Overview
################

This section provides brief examples of using :py:class:`gedidb.GEDIProcessor` and :py:class:`gedidb.GEDIProvider` to process and query **GEDI** data. For advanced features and detailed use cases, refer to the :ref:`fundamentals`.

Start by importing the **gedidb** package:

.. code-block:: python

    import gedidb as gdb

Processing GEDI Data
--------------------

To process GEDI data, specify paths to a ``YAML`` configuration file (`data_config_file`) and an ``SQL`` schema file (`sql_config_file`). See :ref:`fundamentals-setup` for more information on the data configuration files.

This setup initiates the download, processing, and storage of GEDI data in your database.

.. code-block:: python

    # Paths to configuration files
    data_config_file = 'path/to/data_config_file.yml'
    sql_config_file = 'path/to/db_schema_file.sql'
    
    # Process GEDI data with 4 parallel workers
    with gdb.GEDIProcessor(data_config_file, sql_config_file, n_workers=4) as processor:
        processor.compute()

In this example, the :py:class:`gedidb.GEDIProcessor` performs:

- **Downloading** GEDI L2A-B and L4A-C products.
- **Filtering** data by quality.
- **Storing** the processed data in the database.

The ``n_workers=4`` argument directs **Dask** to process four data granules in parallel.

Querying GEDI Data
------------------

Once the data is processed and stored, use :py:class:`gedidb.GEDIProvider` to query it. The results can be returned in either **Xarray** or **Pandas** format, providing flexibility for various workflows.

Example query using :py:class:`gedidb.GEDIProvider`:

.. code-block:: python

    # Path to the data configuration file
    data_config_file = 'path/to/data_config_file.yml'

    # Database table names
    shot_table = 'shot_table_name'
    metadata_table = 'metadata_table_name'
    
    # Create GEDIProvider instance
    provider = gdb.GEDIProvider(config_file=data_config_file,
                                table_name=shot_table,
                                metadata_table=metadata_table)

    # Define variables to query
    variables = ["rh", "pavd_z", "pai"]
    
    # Query data with filters
    dataset = provider.get_data(variables=variables, geometry=None, 
                                start_time="2018-01-01", end_time="2023-12-31", 
                                limit=100, force=True, order_by=["-shot_number"], 
                                return_type='xarray')

This ``provider.get_data()`` function allows you to:

- **Select specific columns** (e.g., `rh`, `pavd_z`, `pai`).
- **Apply spatial and temporal filters** using `geometry`, `start_time`, and `end_time`.
- **Sort data** with the `order_by` parameter (e.g., descending `shot_number`).
- **Return data** in either `xarray` or `pandas` format based on `return_type`.

This functionality offers a flexible, scalable approach to querying GEDI data, streamlining its integration into your data workflows.

---

