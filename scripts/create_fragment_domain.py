import json
import boto3
import tiledb
import os

# Initialize boto3 session for S3 credentials
session = boto3.Session()
creds = session.get_credentials()
# S3 TileDB context with consolidation settings
tiledb_config =tiledb.Config({
                                    # Consolidation settings
                                    "sm.consolidation.steps": 10,
                                    "sm.consolidation.step_max_frags": 100,  # Adjust based on fragment count
                                    "sm.consolidation.step_min_frags": 10,
                                    "sm.consolidation.buffer_size": 5_000_000_000,  # 5GB buffer size per attribute/dimension
                                    "sm.consolidation.step_size_ratio":0.5, #  allow fragments that differ by up to 50% in size to be consolidated.
                                    "sm.consolidation.amplification": 1.2, #  Allow for 20% amplification
                                    
                                    # Memory budget settings
                                    "sm.memory_budget": "150000000000",  # 150GB total memory budget
                                    "sm.memory_budget_var": "50000000000",  # 50GB for variable-sized attributes
                                    
                                    # S3-specific configurations (if using S3)
                                    "vfs.s3.aws_access_key_id": creds.access_key,
                                    "vfs.s3.aws_secret_access_key": creds.secret_key,
                                    "vfs.s3.endpoint_override": 'https://s3.gfz-potsdam.de',
                                    "vfs.s3.region": 'eu-central-1',
                                })


ctx = tiledb.Ctx(tiledb_config)

bucket = 'dog.gedidb.gedi-l2-l4-v002/test'
scalar_array_uri = os.path.join(f"s3://{bucket}", 'profile_array_uri')


fragment_info = tiledb.FragmentInfoList(scalar_array_uri, ctx=ctx)

i =0
for fragment in fragment_info:

    # Extract the nonempty domain
    nonempty_domain = fragment.nonempty_domain
    latitude_range = nonempty_domain[0]
    longitude_range = nonempty_domain[1]
    
    # Create a GeoJSON Feature for the nonempty domain
    geojson_feature = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [longitude_range[0], latitude_range[0]],
                [longitude_range[1], latitude_range[0]],
                [longitude_range[1], latitude_range[1]],
                [longitude_range[0], latitude_range[1]],
                [longitude_range[0], latitude_range[0]]
            ]]
        },
    }
    
    # Wrap in a GeoJSON FeatureCollection
    geojson = {
        "type": "FeatureCollection",
        "features": [geojson_feature]
    }
    
    # Save the GeoJSON to a file
    geojson_file_path = "/home/simon/Downloads/fragments_/tiledb_fragment_{n_fragment}.geojson".format(n_fragment= str(i))
    with open(geojson_file_path, 'w') as file:
        json.dump(geojson, file, indent=4)
    i +=1

