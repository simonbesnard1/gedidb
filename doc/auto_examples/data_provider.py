"""
Querying Processed GEDI Data from TileDB
========================================

This example demonstrates how to use the `GEDIProvider` class to query and retrieve GEDI data stored in TileDB arrays.
We will:

1. Set up the `GEDIProvider` with a TileDB storage backend.
2. Query data using spatial and temporal filters.
3. Retrieve data in both `xarray.Dataset` and `pandas.DataFrame` formats.
4. Perform nearest-shot queries to retrieve GEDI data closest to a given point.

Before running this example:

- Ensure that the GEDI data has been processed and stored in TileDB arrays.
- Configure the TileDB storage settings (local or S3) based on your data location.

"""

import geopandas as gpd
import gedidb as gdb

# Configure the TileDB storage backend
storage_type = "local"  # Options: "local" or "s3"
local_path = "/path/to/processed/gedi/data"
s3_bucket = None  # Only required if using "s3"

# Initialize the GEDIProvider
provider = gdb.GEDIProvider(
    storage_type=storage_type,
    local_path=local_path,
    s3_bucket=s3_bucket,
)

# Define variables to retrieve
variables = ["latitude", "longitude", "rh100", "beam"]

# Define the spatial region of interest (ROI) as a GeoJSON file or GeoDataFrame
geojson_path = "/path/to/roi.geojson"
geometry = gpd.read_file(geojson_path)

# Define the temporal range
start_time = "2020-01-01"
end_time = "2020-12-31"

##############################################################
# -----------------------------------------------------------------
# Section 1: Bounding Box Query
# -----------------------------------------------------------------
print("=== Bounding Box Query ===")
print("Querying GEDI data within a specified spatial and temporal range.")

# Query data within the bounding box and time range, and retrieve it as an `xarray.Dataset`
data_xarray = provider.get_data(
    variables=variables,
    geometry=geometry,
    start_time=start_time,
    end_time=end_time,
    return_type="xarray",
    query_type="bounding_box",
)

# Print the retrieved `xarray.Dataset`
print("Retrieved data as an xarray.Dataset:")
print(data_xarray)

##############################################################
# -----------------------------------------------------------------
# Section 2: Nearest Shot Query
# -----------------------------------------------------------------
print("\n=== Nearest Shot Query ===")
print("Querying GEDI data for the nearest shots to a specific point.")

# Specify a geographic point (longitude, latitude) and the number of nearest shots
point = (-55.0, -10.0)  # Example longitude and latitude
num_shots = 5  # Retrieve the 5 nearest shots

# Query data for the nearest GEDI shots
nearest_data = provider.get_data(
    variables=variables,
    point=point,
    num_shots=num_shots,
    query_type="nearest",
    return_type="xarray",
)

# Print the nearest GEDI shots
print("Retrieved nearest GEDI shots as an xarray.Dataset:")
print(nearest_data)

##############################################################
# -----------------------------------------------------------------
# Section 3: Data Format Options
# -----------------------------------------------------------------
print("\n=== Data Format Options ===")
print("Retrieving GEDI data as a pandas.DataFrame for easier tabular analysis.")

# Query the data within the bounding box and retrieve it as a `pandas.DataFrame`
data_dataframe = provider.get_data(
    variables=variables,
    geometry=geometry,
    start_time=start_time,
    end_time=end_time,
    return_type="dataframe",
    query_type="bounding_box",
)

# Print the retrieved `pandas.DataFrame`
print("Retrieved data as a pandas.DataFrame:")
print(data_dataframe)
