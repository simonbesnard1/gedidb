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

To process GEDI data, specify paths to a ``YAML`` configuration file (`config_file`). See :ref:`fundamentals-setup` for more information on the data configuration files.

This setup initiates the download, processing, and storage of GEDI data in your database.

.. code-block:: python

    # Paths to configuration files
    config_file = 'path/to/config_file.yml'

    if __name__ == "__main__":
        # Process GEDI data with 4 parallel workers
        with gdb.GEDIProcessor(config_file, n_workers = 4) as processor:
            processor.compute()

In this example, the :py:class:`gedidb.GEDIProcessor` performs:

- **Downloading** GEDI L2A-B and L4A-C products.
- **Filtering** data by quality.
- **Storing** the processed data in the tileDB database.

The ``n_workers=4`` argument directs **Dask** to process four data granules in parallel.

Querying GEDI Data
------------------

Once the data is processed and stored, use :py:class:`gedidb.GEDIProvider` to query it. The results can be returned in either **Xarray** or **Pandas** format, providing flexibility for various workflows.

Example query using :py:class:`gedidb.GEDIProvider`:

.. code-block:: python

    # Create GEDIProvider instance
    provider = gdb.GEDIProvider()

    # Define variables to query
    variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]

    # Query data with filters
    dataset = provider.get_data(variables = variables,
                                geometry = None,
                                start_time = "2018-01-01",
                                end_time = "2023-12-31",
                                return_type = 'xarray')

This ``provider.get_data()`` function allows you to:

- **Select specific columns** (e.g., `wsci_z_pi_lower`, `wsci_z_pi_upper`).
- **Apply spatial and temporal filters** using `geometry`, `start_time`, and `end_time`.
- **Return data** in either `xarray` or `pandas` format based on `return_type`.

This functionality offers a flexible, scalable approach to querying GEDI data, streamlining its integration into your data workflows.

---

