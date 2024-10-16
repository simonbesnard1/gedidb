import tiledb
import geopandas as gpd
import numpy as np

# Load your GeoParquet file into a GeoDataFrame
gdf = gpd.read_parquet('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/parquet/filtered_granule_O02137_04.parquet')

# Extract latitude and longitude from geometry if they don't already exist as columns
gdf['latitude'] = gdf.geometry.y
gdf['longitude'] = gdf.geometry.x

# Convert 'absolute_time' to UNIX timestamp in microseconds for integer compatibility
gdf['absolute_time'] = gdf['absolute_time'].astype('int64')  # Converts datetime64[us] to integer (microseconds)

# Define the dimensions based on time, latitude, and longitude
domain = tiledb.Domain(
    tiledb.Dim(name="absolute_time", domain=(gdf['absolute_time'].min(), gdf['absolute_time'].max()), tile=100, dtype="int64"),
    tiledb.Dim(name="latitude", domain=(-90.0, 90.0), tile=0.1, dtype="float64"),
    tiledb.Dim(name="longitude", domain=(-180.0, 180.0), tile=0.1, dtype="float64")
)

# Define attributes for each of the other columns in the GeoDataFrame
attributes = []
for column in gdf.columns:
    if column not in ["absolute_time", "latitude", "longitude", "geometry"]:  # Exclude dimensions and geometry
        try:
            dtype = gdf[column].dtype
            attributes.append(tiledb.Attr(name=column, dtype=dtype))
        except:
            pass

# Create the TileDB schema with sparse storage (good for point data)
schema = tiledb.ArraySchema(domain=domain, attrs=attributes, sparse=True)
array_uri = "s3://your-bucket/gedi-array"

# Create the TileDB array
tiledb.Array.create(array_uri, schema)

# Write the data to TileDB
with tiledb.open(array_uri, mode="w") as array:
    array[gdf['absolute_time'].values, gdf['latitude'].values, gdf['longitude'].values] = {
        col: gdf[col].values for col in gdf.columns if col not in ["absolute_time", "latitude", "longitude", "geometry"]
    }
