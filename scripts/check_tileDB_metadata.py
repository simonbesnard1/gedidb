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

bucket = 'dog.gedidb.gedi-l2-l4-v002'
scalar_array_uri = os.path.join(f"s3://{bucket}", 'scalar_array_uri')
profile_array_uri = os.path.join(f"s3://{bucket}", 'profile_array_uri')


#%% Check metadata
with tiledb.open(scalar_array_uri, mode="r", ctx=ctx) as scalar_array, \
     tiledb.open(profile_array_uri, mode="r", ctx=ctx) as profile_array:
    
    # Collect metadata for scalar and profile arrays, excluding unwanted keys
    scalar_metadata = {k: scalar_array.meta[k] for k in scalar_array.meta 
                       if not k.startswith("granule_") and "array_type" not in k}
    profile_metadata = {k: profile_array.meta[k] for k in profile_array.meta 
                        if not k.startswith("granule_") and "array_type" not in k}
    
    # Combine metadata from scalar and profile arrays
    combined_metadata = {**scalar_metadata, **profile_metadata}
    organized_metadata = {}
    
    # Organize metadata into nested dictionary structure for DataFrame conversion
    for key, value in combined_metadata.items():
        var_name, attr_type = key.split(".", 1)
        if var_name not in organized_metadata:
            organized_metadata[var_name] = {}
        organized_metadata[var_name][attr_type] = value


#%% Check variables
with tiledb.open(scalar_array_uri, mode="r", ctx=ctx) as scalar_array, \
     tiledb.open(profile_array_uri, mode="r", ctx=ctx) as profile_array:
    
    # Extract unique variable names from metadata keys for scalar and profile arrays
    scalar_vars = list({k.split(".")[0] for k in scalar_array.meta if "." in k})
    profile_vars = list({k.split(".")[0] for k in profile_array.meta if "." in k})