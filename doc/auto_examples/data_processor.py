"""
GEDIProcessor example with different parallel engines
=======================================================

This example demonstrates how to use the `gedidb` library to process GEDI granules 
with different parallel engines, such as `concurrent.futures.ThreadPoolExecutor` and `dask.distributed.Client`.

A default data configuration file (`data_config.yml`) can be downloaded here:

:download:`Download data_config.yml <../_static/test_files/data_config.yml>`

A default geojson file (`test.geojson`) can be downloaded here:

:download:`Download test.geojson <../_static/test_files/test.geojson>`


We will:

1. Set up configuration paths.
2. Initialize different parallel engines (Dask and concurrent futures).
3. Run the GEDIProcessor to process granules and consolidate fragments.

"""

# Import required libraries
import gedidb as gdb
from dask.distributed import Client, LocalCluster
import concurrent.futures

# Configuration file path
config_file = '/path/to/data_config.yml'

# Paths to GeoJSON region and EarthData credentials
geojson_path = '/path/to/test.geojson'
earth_data_dir = '/path/to/EarthData_credentials'

# Define the start and end date for processing
start_date = '2020-01-01'
end_date = '2020-12-31'

# Option 1: Using a ThreadPoolExecutor (concurrent.futures)
# ---------------------------------------------------------
# This option uses Python's standard library for parallel execution.
# Useful for lightweight, multi-threaded tasks on a single machine.
print("Initializing ThreadPoolExecutor...")
concurrent_engine = concurrent.futures.ThreadPoolExecutor(max_workers=5)

# Option 2: Using Dask for Parallel Execution
# -------------------------------------------
# This option is suitable for distributed computing or managing memory more efficiently.
print("Initializing Dask Client...")
n_workers = 1  # Number of Dask workers
cluster = LocalCluster(
    n_workers=5,
    threads_per_worker=1,
    processes=True,
    memory_limit='8GB',
    dashboard_address=None,
)
dask_client = Client(cluster)

# Initialize the GEDIProcessor with the chosen parallel engine
# ------------------------------------------------------------
# Here, we demonstrate usage with `concurrent.futures.ThreadPoolExecutor`. 
# You can replace `parallel_engine=concurrent_engine` with `parallel_engine=dask_client` to use Dask instead.
with gdb.GEDIProcessor(
    config_file=config_file,
    geometry=geojson_path,
    start_date=start_date,
    end_date=end_date,
    earth_data_dir=earth_data_dir,
    parallel_engine=concurrent_engine,  # Change to `parallel_engine=dask_client` for Dask
) as processor:
    # Run the GEDIProcessor to process granules and consolidate fragments
    processor.compute(consolidate=True)
