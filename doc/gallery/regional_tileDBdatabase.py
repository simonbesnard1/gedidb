"""
Creating a Regional TileDB Array for the Amazon
===============================================

This example demonstrates how to create a **regional TileDB array** for the Amazon 
by modifying the `data_config.yml` file to adjust the **spatial range** based on 
a bounding box extracted from a GeoJSON file.

A default data configuration file (`data_config.yml`) can be downloaded here:

:download:`Download data_config.yml <../_static/test_files/data_config.yml>`

A default GeoJSON file for the Amazon region (`amazon.geojson`) can be downloaded here:

:download:`Download amazon.geojson <../_static/test_files/amazon.geojson>`

We will:

1. Modify the configuration file (`data_config.yml`) to define the Amazon spatial extent.
2. Initialize the `GEDIProcessor` with the modified configuration.
3. Process GEDI granules and store them in a **regional TileDB array**.

"""

# Import required libraries
import gedidb as gdb
import yaml
import json

# Define paths for configuration and spatial data
config_file = "/path/to/data_config.yml"
geojson_path = "/path/to/amazon.geojson"

# Step 1: Modify the Configuration File
# -------------------------------------
# Load the existing configuration file
with open(config_file, "r") as file:
    config = yaml.safe_load(file)

# Define the Amazon bounding box from the GeoJSON file
with open(geojson_path, "r") as file:
    amazon_geojson = json.load(file)

# Extract bounding box from the GeoJSON
amazon_bbox = amazon_geojson["features"][0]["geometry"]["coordinates"][0]
lon_min, lat_min = amazon_bbox[0]  # Bottom-left corner
lon_max, lat_max = amazon_bbox[2]  # Top-right corner

# Update the configuration file with the new bounding box
config["tiledb"]["local_path"] = "tiledb/amazon"
config["tiledb"]["spatial_range"]["lat_min"] = lat_min
config["tiledb"]["spatial_range"]["lat_max"] = lat_max
config["tiledb"]["spatial_range"]["lon_min"] = lon_min
config["tiledb"]["spatial_range"]["lon_max"] = lon_max

# Save the modified configuration
new_config_file = "data_config_amazon.yml"
with open(new_config_file, "w") as file:
    yaml.dump(config, file, default_flow_style=False)

print(f"Updated configuration saved as '{new_config_file}'.")

# Step 2: Run the GEDI Processor
# ------------------------------
# Define additional parameters
start_date = "2020-01-01"
end_date = "2023-12-31"
earth_data_dir = "/path/to/EarthData_credentials"

# Initialize and run the GEDIProcessor
with gdb.GEDIProcessor(
    config_file=new_config_file,
    geometry=geojson_path,
    start_date=start_date,
    end_date=end_date,
    earth_data_dir=earth_data_dir,
    parallel_engine=None,  # Modify if using Dask or ThreadPoolExecutor
) as processor:
    # Process the data and consolidate TileDB fragments
    processor.compute(consolidate=True)

print("Regional TileDB array for the Amazon has been successfully created.")
