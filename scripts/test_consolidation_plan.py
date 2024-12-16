import boto3
import tiledb
import os


def spatial_ConsolidationPlan(array_uri, ctx):
    """
    Generate a spatial consolidation plan for a TileDB array, grouping spatially overlapping fragments.

    Parameters:
    ----------
    array_uri : str
        URI of the TileDB array.
    ctx : tiledb.Ctx
        TileDB context.

    Returns:
    -------
    dict
        A dictionary representing the consolidation plan, where each node contains:
        - `num_fragments`: Number of fragments in the group.
        - `fragment_uris`: List of fragment URIs in the group.
    """
    # Retrieve fragment information
    fragment_info = tiledb.FragmentInfoList(array_uri, ctx=ctx)

    # Extract spatial domains for fragments
    fragments = []
    for fragment in fragment_info:
        nonempty_domain = fragment.nonempty_domain
        fragments.append({
            "uri": os.path.basename(fragment.uri),
            "latitude_range": nonempty_domain[0],
            "longitude_range": nonempty_domain[1],
        })

    # Function to check spatial overlap
    def has_spatial_overlap(frag1, frag2):
        """Check if two fragments overlap in spatial domains."""
        lat_overlap = frag1["latitude_range"][0] <= frag2["latitude_range"][1] and \
                      frag1["latitude_range"][1] >= frag2["latitude_range"][0]
        lon_overlap = frag1["longitude_range"][0] <= frag2["longitude_range"][1] and \
                      frag1["longitude_range"][1] >= frag2["longitude_range"][0]
        return lat_overlap and lon_overlap

    # Generate nodes (groups) of spatially overlapping fragments
    visited = set()
    plan = {}
    node_id = 0

    for fragment in fragments:
        if fragment["uri"] in visited:
            continue

        # Create a new node
        current_node = {
            "num_fragments": 0,
            "fragment_uris": []
        }

        # Recursively collect overlapping fragments
        stack = [fragment]
        while stack:
            frag = stack.pop()
            if frag["uri"] in visited:
                continue

            # Mark fragment as visited and add to the current node
            visited.add(frag["uri"])
            current_node["fragment_uris"].append(frag["uri"])
            current_node["num_fragments"] += 1

            # Check for overlapping fragments
            for candidate in fragments:
                if candidate["uri"] not in visited and has_spatial_overlap(frag, candidate):
                    stack.append(candidate)

        # Add the node to the plan
        plan[node_id] = current_node
        node_id += 1

    return plan

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

cons_plan = spatial_ConsolidationPlan(scalar_array_uri, ctx)


