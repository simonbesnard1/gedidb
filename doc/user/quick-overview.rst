.. _overview:

################
Quick Overview
################

This section provides brief examples of using :py:class:`gedidb.GEDIProcessor` and :py:class:`gedidb.GEDIProvider` to process and query **GEDI** data. For advanced features and detailed use cases, refer to the :ref:`fundamentals`.

.. note::

   **First install `gedidb`**:

   .. code-block:: bash

      pip install gedidb[full]
      # or minimal
      pip install gedidb

   Verify the installation:

   .. code-block:: bash

      python -c "import gedidb; print(gedidb.__version__)"

   For system dependencies, optional extras, and troubleshooting,
   see :ref:`installing`.

Start by importing the **gedidb** package:

.. code-block:: python

    import gedidb as gdb
    import concurrent.futures

Processing GEDI Data
--------------------

To process GEDI data, specify paths to a ``YAML`` configuration file (`config_file`). See :ref:`fundamentals-setup` for more information on configuration files.

This setup initiates the **download, processing, and storage** of GEDI data into your database.

.. code-block:: python

    # Paths to configuration files
    config_file = 'path/to/data_config.yml'
    geometry = 'path/to/test.geojson'

    # Initialize a parallel engine
    concurrent_engine = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    # Initialize the GEDIProcessor and compute
    with gdb.GEDIProcessor(
        config_file=config_file,
        geometry=geometry,
        start_date='2020-01-01',
        end_date='2020-12-31',
        earth_data_dir='/path/to/earthdata_credential/folder',
        parallel_engine=concurrent_engine,
    ) as processor:
        processor.compute(consolidate=True)

In this example, the :py:class:`gedidb.GEDIProcessor` performs:

- **Downloading** GEDI L2A–B and L4A–C products.
- **Filtering** data by quality.
- **Storing** the processed data as sparse arrays in the TileDB database.

Querying GEDI Data
------------------

Once the data is processed and stored, use :py:class:`gedidb.GEDIProvider` to query it. The results can be returned in either **Xarray** or **Pandas** format, providing flexibility for different workflows.

Example query using :py:class:`gedidb.GEDIProvider`:

.. code-block:: python
    
    import geopandas as gpd
    import gedidb as gdb

    # Create GEDIProvider instance
    provider = gdb.GEDIProvider(
        storage_type='local',
        local_path="path/to/your/database/"
    )

    # Load region of interest
    region_of_interest = gpd.read_file('./data/geojson/BR-Sa1.geojson')

    # Define the columns to query and additional parameters
    vars_selected = ["agbd", "rh"]

    # Query the database
    gedi_data = provider.get_data(
        variables=vars_selected,
        query_type="bounding_box",
        geometry=region_of_interest,
        start_time="2018-01-01",
        end_time="2024-07-25",
        return_type='xarray'
    )

This :py:meth:`provider.get_data()` function allows you to:

- **Select specific variables** (e.g., `agbd`, `rh` metrics).
- **Apply spatial and temporal filters** using `geometry`, `start_time`, and `end_time`.
- **Return data** in either `xarray` or `pandas` format based on `return_type`.

This functionality offers a flexible, scalable approach to querying GEDI data, streamlining its integration into reproducible geospatial workflows.

---
