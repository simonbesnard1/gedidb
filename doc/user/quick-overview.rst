.. _overview:

################
Quick Overview
################

This section provides quick examples of how to use the :py:class:`gedidb.GEDIProcessor` and :py:class:`gedidb.GEDIProvider` objects to process and query **GEDI** data. For a more detailed explanation, refer to the full documentation.

Start by importing the **gedidb** package using its commonly used abbreviation:

.. code-block:: python

    import gedidb as gdb

Processing GEDI Data
--------------------

You can process GEDI data by supplying paths to the configuration files: a YAML file (`data_config_file`) and an SQL schema file (`sql_config_file`). This will trigger the downloading, processing, and storage of data into your database.

.. code-block:: python

    # Define paths to configuration files
    data_config_file = 'your_path/to/data_config_file.yml'
    sql_config_file = 'your_path/to/db_scheme_file.sql'
    
    # Process GEDI data using 4 workers
    with gdb.GEDIProcessor(data_config_file, sql_config_file, n_workers=4) as processor:
        processor.compute()

This example performs the following tasks:

- **Downloads** GEDI L2A-B and L4A-C products.
- **Filters** data based on quality.
- **Writes** the processed data into the database.

The argument `n_workers=4` indicates that four granules will be processed in parallel using **Dask**. Once the processing is complete, you can access the data using the :py:class:`gedidb.GEDIProvider`.

Querying GEDI Data
------------------

Once the data has been processed and stored in the database, you can easily query it using the :py:class:`gedidb.GEDIProvider`. The queried data can be returned in either **Xarray** or **Pandas** format.

Here’s an example of querying GEDI data:

.. code-block:: python

    # Define path to the configuration file
    data_config_file = 'your_path/to/data_config_file.yml'

    # Define database table names
    shot_table = 'name_of_shot_table'
    metadata_table = 'name_of_metadata_table'
    
    # Instantiate the GEDIProvider
    provider = gdb.GEDIProvider(config_file=data_config_file,
                                table_name=shot_table,
                                metadata_table=metadata_table)

    # Define the columns (variables) to query
    variables = ["rh", "pavd_z", "pai"]
    
    # Query the data
    dataset = provider.get_data(variables=variables, geometry=None, 
                                start_time="2018-01-01", end_time="2023-12-31", 
                                limit=100, force=True, order_by=["-shot_number"], 
                                return_type='xarray')

In this example, the `provider.get_data()` method allows you to:

- **Specify the columns** (variables) you want to query (e.g., `rh`, `pavd_z`, and `pai`).
- **Filter data** by a spatial `geometry`, a time range (`start_time` and `end_time`), and other parameters.
- **Sort the data** by specifying the `order_by` parameter (e.g., by `shot_number`).
- **Return the data** in either `xarray` or `pandas` format, depending on the `return_type`.

This functionality allows you to work with GEDI data in a scalable and flexible manner, easily integrating it into your data workflows.

---

This **Quick Overview** gives you a glimpse of the key functionalities of **gediDB**, making it easy to process, query, and analyze GEDI data. For more detailed use cases and advanced features, be sure to check the :ref:`fundamentals` for more details.
