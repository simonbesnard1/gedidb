.. _database:

#############################
Personalising tileDB Settings
#############################

This section provides an overview of customizing the tileDB settings in the `data_config.yml` to manage and query GEDI data using your own personalized settings.

Overview
--------

The provided `data_config.yml` file allows users to customize tileDB settings to suit their specific requirements. By modifying the configuration file, you can adjust the database connection details, file paths, environment settings, and data extraction settings to optimize data handling and processing.
However, you should be an experienced user with prior knowledge of TileDB and configuration files to ensure proper adjustments and avoid potential issues.

Customizing tileDB settings
---------------------------

The following is an example of the code provided in the `data_config.yml` file:

.. code-block:: yaml

    tiledb:
      storage_type: 'local'                             # either local or s3
      local_path: ''                                    # TileDB URI for storing data
      overwrite: true                                   # Whether to overwrite existing arrays
      temporal_batching: "weekly"                       # either daily or weekly
      chunk_size: 25                                    # chunk siz ein degrees for spatial chunks
      time_range:                                       # Global time range for data
        start_time: "2018-01"                           # Global start time for data
        end_time: "2030-12-31"                          # Global end time for data
      spatial_range:                                    # Global spatial range (bounding box)
        lat_min: -56.0
        lat_max: 56.0
        lon_min: -180.0
        lon_max: 180.0
      dimensions: ['latitude', 'longitude', 'time']     # Dimensions for the TileDB array
      consolidation_settings:
       fragment_size: 200_000_000_000                   # 100GB fragment size
       memory_budget: "150000000000"                    # 150GB total memory budget
       memory_budget_var: "50000000000"                 # 50GB for variable-sized attributes
      cell_order: "hilbert"
      capacity: 100000

**Explanation of Configuration Fields**

1. **storage_type**:
   Specifies where the TileDB arrays will be stored. 
   Options:

   - `local`: Arrays will be stored on the local filesystem.
   - `s3`: Arrays will be stored on an object storage (e.g., AWS S3, Ceph), requiring additional configuration by the user.

2. **local_path**:
   Defines the URI for storing the data when using local storage. Leave it blank or specify a valid directory path.

3. **overwrite**:
   A Boolean flag indicating whether to overwrite existing arrays when creating new ones.

4. **temporal_batching**:
   Specifies the granularity for time-based batching:

   - `daily`: Batches are created for daily intervals.
   - `weekly`: Batches are created for weekly intervals.
   - If not provided all the granules will be processed in one batch

5. **chunk_size**:
   Defines the size of spatial chunks (in degrees) for tiling. Adjusting this can affect query performance and storage efficiency.

6. **time_range**:
   Sets the global time range for the data:

   - `start_time`: Start of the time range (e.g., "2018-01").
   - `end_time`: End of the time range (e.g., "2030-12-31").

7. **spatial_range**:
   Specifies the spatial bounding box for the tileDB:

   - `lat_min` and `lat_max`: Minimum and maximum latitude values.
   - `lon_min` and `lon_max`: Minimum and maximum longitude values.

8. **dimensions**:
   Lists the dimensions for the TileDB array. Common dimensions include `latitude`, `longitude`, and `time`.

9. **consolidation_settings**:
   Configures the settings for consolidating fragments in TileDB arrays:

   - `fragment_size`: Maximum size (in bytes) of a fragment. Example: `200_000_000_000` equals 200GB.
   - `memory_budget`: Total memory budget for consolidation (in bytes). Example: `150000000000` equals 150GB.
   - `memory_budget_var`: Memory allocated for variable-sized attributes (in bytes). Example: `50000000000` equals 50GB.

For detailed information, see the `TileDB Consolidation Documentation <https://docs.tiledb.com/main/background/internal-mechanics/consolidation>`_.

10. **cell_order**:
    Determines the order of cells in the array. The `hilbert` order is commonly used for optimal spatial locality.
    Learn more about cell ordering in the `TileDB Cell Order Documentation <https://documentation.cloud.tiledb.com/academy/structure/arrays/foundation/key-concepts/storage/data-layout/#sparse-arrays>`_.


11. **capacity**:
    Specifies the number of cells to store per tile. Affects performance and storage efficiency.

.. note::

   Familiarity with tileDB is recommended. For more detailed information about specific configuration settings (e.g., consolidation settings), refer to the official TileDB documentation: `TileDB Docs <https://docs.tiledb.com>`_, or `TileDB Academy <https://documentation.cloud.tiledb.com/academy/home/>`_.

