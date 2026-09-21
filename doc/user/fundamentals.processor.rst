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
- **Database writing**: Stores processed data in a TileDB array with proper metadata for easy querying.

Example usage
-------------

Below is a quick example of using the :py:class:`gedidb.GEDIProcessor` in a workflow:

.. code-block:: python

   import gedidb as gdb

   config_file = 'path/to/data_config.yml'
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

   The granule downloading process consists of two main components:

   - **CMR Data Querying**: The :py:class:`gedidb.CMRDataDownloader` class queries NASA's CMR service for GEDI granules within the specified spatial and temporal bounds. It retrieves granule metadata and ensures that all required products (L2A, L2B, L4A, L4C) are consistently available for each granule ID. A retry mechanism is implemented to handle inconsistencies across products.
   - **Granule File Downloading**: The :py:class:`gedidb.H5FileDownloader` class downloads `.h5` granule files using a robust, resumable process. It supports partial downloads with a temporary `.part` file and only renames files to `.h5` upon successful completion. The class also handles network failures and retries failed downloads to ensure reliability.

   Granules are stored in structured directories, with each granule ID having separate subdirectories containing its corresponding GEDI product files.

3. **Data processing**:

   The processing pipeline efficiently handles GEDI granules by downloading, parsing, filtering, and merging data in parallel.

   - **Parallel Processing**:

     - Both granule downloading and processing are performed concurrently.
     - Each worker processes data for a **temporal tile** as defined in `data_config.yml`.
     - The number of workers is controlled by the `parallel_engine` setting, determining how many granules are processed simultaneously.

   - **Granule Parsing & Quality Filtering**:

     - Each granule is parsed and processed by the :py:class:`gedidb.GEDIGranule` class.
     - Quality filtering is applied using flags such as **sensitivity** and **degrade status**.
     - For more details on filtering criteria, refer to :ref:`fundamentals-filters`.

   - **Data Merging & Structuring**:

     - GEDI products (L2A, L2B, L4A, L4C) are merged using **shot numbers** as the primary key.
     - The merging process ensures that only granules containing all required products are retained.
     - The resulting unified dataset is prepared for writing to **TileDB**.


4. **Database writing**:

   The processed GEDI data is written to a **TileDB database**, ensuring efficient storage and retrieval.

   - **Data Storage**: Processed data is stored in either a local or S3-based TileDB database, distributed across different fragments.
   - **Spatial Chunking**: The data is partitioned into spatial chunks as defined in `data_config.yml`, with each chunk stored in a separate fragment.
   - **Writing Process**:
     
     - **Validation**: The :py:meth:`_validate_granule_data` method ensures required dimensions and critical variables are present before writing.
     - **Coordinate Preparation**: The :py:meth:`_prepare_coordinates` method extracts dimension data (e.g., time, latitude, longitude).
     - **Variable Extraction**: The :py:meth:`_extract_variable_data` method separates scalar and profile variables for structured storage.
     - **TileDB Writing**: The :py:meth:`_write_to_tiledb` method writes data with a retry mechanism to handle potential failures.

   - **Metadata & Optimization**:

     - Metadata is stored alongside the data to support efficient querying.
     - After all granules are processed, the database undergoes **consolidation** to optimize query performance. The consolidation strategy can be configured in `data_config.yml`.


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

Building and resuming a database
--------------------------------

Use a separate database path (or bucket) for each collection/version and keep
``tiledb.overwrite: false`` when resuming. New arrays store an ingestion manifest
containing collection IDs, selected products, variable definitions and quality
filter selection. Resuming with different data semantics or a different schema
raises an error before metadata is changed. Older databases without this manifest
remain readable, but must be rebuilt to use the new ingestion recovery contract.
Source file URLs are also pinned for each discovered granule; changing a source
file requires a new database. Expiring URL query parameters are not stored.

Run **one ingestion coordinator per database**. Its workers may download and parse
in parallel, while the coordinator serializes writes. Before every write attempt,
including retries after a lost acknowledgment, the writer reads existing shot IDs
in that spatial/time window and writes only missing shots. This adds read I/O but
makes replay of the same source files safe after partial writes. It is not a
transaction across independent writers and does not update changed observations.

Latitude, longitude and daily time are not a unique shot identity. Arrays continue
to allow duplicate coordinates, preserving distinct ``uint64`` shot numbers at the
same location on the same day. Exact observation time is stored in ``timestamp_ns``.

A granule receives a successful checkpoint and ledger entry only after its data
and checkpoint writes finish. Successfully parsed granules with no retained shots
also receive checkpoints. Parsing and final write failures propagate to the caller;
``compute()`` does not consolidate or report success after those failures.

The following optional configuration preserves the default four-product intersection:

.. code-block:: yaml

   required_products: [level2A, level2B, level4A, level4C]
   tiledb:
     overwrite: false
     max_in_flight: 4
     flush_every: 50
     max_buffer_bytes: 268435456
     report_every: 25

``required_products`` can select a smaller product set, but must include ``level2A``
for coordinates and timestamps. Discovery and shot joins require every selected
product. Missing quality-filter datasets now raise errors instead of silently
changing the selection. To choose a subset of the existing named filters, use a
top-level mapping, for example:

.. code-block:: yaml

   quality_filters:
     level2A: [quality_flag, surface_flag]
     level2B: []

Omitting a product from this mapping retains its default filters; an empty list
explicitly disables them. Choose the policy before building, because filtered-out
shots cannot be recovered by the provider. Validate collection IDs, SDS mappings,
profile lengths and filters against representative files of the intended version.

``max_buffer_bytes`` limits buffered DataFrames between flushes; a single granule,
concatenation and workers' parsing/download memory can exceed that amount. Set
``max_in_flight`` alongside the worker count and available RAM. TileDB context
settings can be supplied under ``tiledb.config_overrides``. If ``progress_dir`` is
omitted, progress reports are written under ``data_dir/progress``.
