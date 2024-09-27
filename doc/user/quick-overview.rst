################
Quick Overview
################

This section provides quick examples of how to use the :py:class:`gedidb.GEDIProcessor` and :py:class:`gedidb.GEDIProvider` objects. More detailed explanations can be found in the full documentation.

Start by importing `gedidb` with its commonly used abbreviation:

.. code-block:: python

    import gedidb as gdb

Processing GEDI Data
--------------------

You can process GEDI data from scratch by providing paths to the ``.yml`` data configuration file (**path_data_config**) and the ``.sql`` database schema file (**path_db_scheme**). This will trigger the downloading, processing, and writing of the data into the database:

.. code-block:: python

    # Define paths to configuration files
    path_data_config = 'your_path/to/data_config_file'
    path_db_scheme = 'your_path/to/db_scheme_file'
    
    # Initialize the GEDI database processor
    database_builder = gdb.GEDIProcessor(data_config_file=path_data_config, 
                                         sql_config_file=path_db_scheme)
    
    # Process GEDI data using 4 workers
    database_builder.compute(n_workers=4)

In this example, we download, filter for quality, and write the L2A-B and L4A-C GEDI data to the database. The ``n_workers=4`` argument ensures that four granules are processed in parallel using the :py:class:`dask.distributed.Client`. Once processed, the data becomes accessible through the :py:class:`gedidb.GEDIProvider` object.

Reading GEDI Data
-----------------

`gedidb` supports two data types: :py:class:`~xarray.Dataset` and :py:class:`~pandas.DataFrame`.

Here’s an example of querying the data:

.. code-block:: python

    # Define path to configuration file
    path_data_config = 'your_path/to/data_config_file'

    # Define the database table names
    shot_table = 'name_of_shot_table'
    metadata_table = 'name_of_metadata_table'
    
    # Instantiate the GEDIProvider
    provider = gdb.GEDIProvider(config_file=path_data_config,
                                shot_table=shot_table,
                                metadata_table=metadata_table)

    # Define the columns to query and additional parameters
    vars_selected = ["rh", "pavd_z", "pai"]
    dataset = provider.get_data(variables=vars_selected, geometry=None, 
                                start_time="2018-01-01", end_time="2023-12-31", 
                                limit=100, force=True, order_by=["-shot_number"], 
                                return_type='xarray')

Depending on the ``return_type`` (`xarray` or `pandas`), the `provider.get_data()` method returns either an :py:class:`~xarray.Dataset` or a :py:class:`~pandas.DataFrame`. 

The output will contain the selected variables, filtered shots within the defined `geometry` (if provided), and the specified time range (`start_time` to `end_time`).
