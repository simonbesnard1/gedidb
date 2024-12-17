import boto3
import tiledb
import os
import numpy as np
import yaml
import pandas as pd


def _load_yaml_file(file_path: str) -> dict:
    """Load a YAML configuration file."""
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)
     
        
def _load_variables_config(config):
    """
    Load and parse the configuration file and consolidate variables from all product levels.

    Returns:
    --------
    dict:
        The dictionary representation of all variables configuration across products.
    """
    # Consolidate all variables from different levels
    variables_config = {}
    for level in ['level_2a', 'level_2b', 'level_4a', 'level_4c']:
        level_vars = config.get(level, {}).get('variables', {})
        for var_name, var_info in level_vars.items():
            variables_config[var_name] = var_info

    return variables_config

# Define function to create bounding boxes
def create_bounding_boxes(lat_min, lat_max, lon_min, lon_max, num_boxes):
    """
    Divide the spatial domain into bounding boxes.
    """
    lat_intervals = np.linspace(lat_min, lat_max, int(np.sqrt(num_boxes)) + 1)
    lon_intervals = np.linspace(lon_min, lon_max, int(np.sqrt(num_boxes)) + 1)
    
    boxes = []
    for i in range(len(lat_intervals) - 1):
        for j in range(len(lon_intervals) - 1):
            boxes.append({
                "lat_min": lat_intervals[i],
                "lat_max": lat_intervals[i + 1],
                "lon_min": lon_intervals[j],
                "lon_max": lon_intervals[j + 1],
            })
    return boxes

# Function to read data from a TileDB array for a bounding box
def read_bounding_box(array_uri, bounding_box, variables_config, ctx):
    with tiledb.SparseArray(array_uri, mode="r", ctx=ctx) as array:
        lat_min, lat_max = bounding_box["lat_min"], bounding_box["lat_max"]
        lon_min, lon_max = bounding_box["lon_min"], bounding_box["lon_max"]
        scalar_vars = list({k.split(".")[0] for k in array.meta if "." in k})
        scalar_vars.append('shot_number')

        data = array.query(attrs=scalar_vars).multi_index[lat_min:lat_max, lon_min:lon_max]
        return data

# Function to write data to a new TileDB array
def write_to_new_array(new_array_uri, granule_data, variables_config, ctx):
   granule_data['latitude'] = granule_data['latitude'] + 1
   granule_data['longitude'] = granule_data['longitude'] + 1
   
   coords = {
       dim_name: (
           granule_data[dim_name]
       )
       for dim_name in ['latitude', 'longitude', 'time', 'profile_point']
   }
   
   # Extract scalar data attributes
   profile_vars = [var_name for var_name, var_info in variables_config.items() if var_info.get('is_profile', False)]
   data = {
       var_name: granule_data[var_name]
       for var_name in profile_vars
   }
   
   data['shot_number'] = granule_data['shot_number']

   # Write to the scalar array
   with tiledb.open(new_array_uri, mode="w", ctx=ctx) as array:
       dim_names = [dim.name for dim in array.schema.domain]
       dims = tuple(coords[dim_name] for dim_name in dim_names)
       array[dims] = data

    
# Main logic
def split_and_reorganize_data(original_array_uri, new_base_uri, num_boxes, variables, ctx):
    # Get the spatial domain from the original array
    with tiledb.SparseArray(original_array_uri, mode="r", ctx=ctx) as array:
        domain = array.nonempty_domain()
        lat_min, lat_max = domain[0]
        lon_min, lon_max = domain[1]
        start_time, end_time = domain[2]

    # Create bounding boxes
    bounding_boxes = create_bounding_boxes(lat_min, lat_max, lon_min, lon_max, num_boxes)

    # Process each bounding box
    for i, bounding_box in enumerate(bounding_boxes):
        print(f"Processing bounding box {i+1}/{len(bounding_boxes)}: {bounding_box}")
        data = read_bounding_box(original_array_uri, bounding_box, variables, ctx)
        write_to_new_array(new_base_uri, data, variables, ctx)


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

bucket = 'dog.gedidb.gedi-l2-l4-v002'
scalar_array_uri = os.path.join(f"s3://{bucket}", 'profile_array_uri')
bucket = 'dog.gedidb.gedi-l2-l4-v002/test'
new_scalar_array_uri = os.path.join(f"s3://{bucket}", 'profile_array_uri')

# Number of bounding boxes
num_boxes = 30

# TileDB context and variables to query
variables_config = _load_variables_config(_load_yaml_file('/home/simon/glm1/person/besnard/gediDB/config_files/data_config.yml'))

# Split and reorganize data
split_and_reorganize_data(scalar_array_uri, new_scalar_array_uri, num_boxes, variables_config, ctx)
