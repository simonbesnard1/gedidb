import boto3
import tiledb
import os

# Initialize boto3 session for S3 credentials
session = boto3.Session()
creds = session.get_credentials()
# S3 TileDB context with consolidation settings
tiledb_config =tiledb.Config({
                                    # S3-specific configurations (if using S3)
                                    "vfs.s3.aws_access_key_id": creds.access_key,
                                    "vfs.s3.aws_secret_access_key": creds.secret_key,
                                    "vfs.s3.endpoint_override": 'https://s3.gfz-potsdam.de',
                                    "vfs.s3.region": 'eu-central-1',
                                })


ctx = tiledb.Ctx(tiledb_config)

bucket = 'dog.gedidb.gedi-l2-l4-v002'
scalar_array_uri = os.path.join(f"s3://{bucket}", 'gedi_array_uri')

# Open the array in read mode
with tiledb.open(scalar_array_uri, 'r', ctx=ctx) as array_uri:
    # Define the desirable fragment size in bytes
    fragment_size = 100_000_000_000  # 200GB
    
    # Generate the consolidation plan
    cons_plan = tiledb.ConsolidationPlan(ctx, array_uri, fragment_size)

    # Output details about the plan
    print("Consolidation Plan Details:")
    print(cons_plan.dump())
    print("\nNumber of nodes: ", cons_plan.num_nodes)

    # Inspect the first node
    if cons_plan.num_nodes > 0:
        print("\nNode #1 Details:")
        print("Number of fragments in node #1: ", cons_plan[0]['num_fragments'])
        print("Fragments URIs in node #1:", cons_plan[0]['fragment_uris'])


# Consolidate using the fragments from the consolidation plan
tiledb.consolidate(
    scalar_array_uri, 
    ctx=ctx,
    config=tiledb_config, 
    fragment_uris=cons_plan[0]['fragment_uris'])

# Vacuum
tiledb.vacuum(
    scalar_array_uri, 
    ctx=ctx,
    config=tiledb_config)
