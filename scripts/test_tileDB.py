import tiledb
import geopandas as gpd
from shapely.geometry import box
from rtree import index

# Initialize the R-tree index
spatial_index = index.Index("gedi_rtree_index")  # Saves to a file named `gedi_rtree_index`

# Example list of granules; in practice, load or iterate over your data chunks
granules = [
    {"id": "granule_001", "data_path": "path/to/granule_001.parquet"},
    {"id": "granule_002", "data_path": "path/to/granule_002.parquet"},
    {"id": "granule_003", "data_path": "path/to/granule_003.parquet"},
    {"id": "granule_004", "data_path": "path/to/granule_004.parquet"},
]

for idx, granule in enumerate(granules):
    # Load each granule's data into a GeoDataFrame
    gdf = gpd.read_parquet(granule["data_path"])
    
    # Extract latitude and longitude if not already separate columns
    gdf['latitude'] = gdf.geometry.y
    gdf['longitude'] = gdf.geometry.x

    # Convert `absolute_time` to UNIX timestamps (microseconds)
    gdf['absolute_time'] = gdf['absolute_time'].astype('int64')

    # Define the TileDB URI for this granule
    array_uri = f"s3://your-bucket/tiledb_data/{granule['id']}/gedi_array"

    # Define the TileDB schema based on latitude, longitude, and time
    domain = tiledb.Domain(
        tiledb.Dim(name="absolute_time", domain=(gdf['absolute_time'].min(), gdf['absolute_time'].max()), tile=100, dtype="int64"),
        tiledb.Dim(name="latitude", domain=(-90.0, 90.0), tile=0.1, dtype="float64"),
        tiledb.Dim(name="longitude", domain=(-180.0, 180.0), tile=0.1, dtype="float64")
    )

    # Define attributes for other columns
    attributes = []
    for column in gdf.columns:
        if column not in ["absolute_time", "latitude", "longitude", "geometry"]:
            dtype = gdf[column].dtype
            attributes.append(tiledb.Attr(name=column, dtype=dtype))

    # Create TileDB schema and array
    schema = tiledb.ArraySchema(domain=domain, attrs=attributes, sparse=True)
    tiledb.Array.create(array_uri, schema)

    # Write data to the TileDB array
    with tiledb.open(array_uri, mode="w") as array:
        array[gdf['absolute_time'].values, gdf['latitude'].values, gdf['longitude'].values] = {
            col: gdf[col].values for col in gdf.columns if col not in ["absolute_time", "latitude", "longitude", "geometry"]
        }

    # Build R-tree entry for this granule
    bbox = box(
        gdf['longitude'].min(), gdf['latitude'].min(),
        gdf['longitude'].max(), gdf['latitude'].max()
    )
    time_range = (gdf['absolute_time'].min(), gdf['absolute_time'].max())

    # Insert bounding box with time range into the R-tree index, linking to the TileDB URI
    spatial_index.insert(idx, bbox.bounds, obj={
        "uri": array_uri,
        "time_range": time_range,
        "granule_id": granule["id"]
    })
