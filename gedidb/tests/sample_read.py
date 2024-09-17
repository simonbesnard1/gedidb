# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import psycopg2
import pandas as pd
import geopandas as gpd

# Connect to the PostGIS database
conn = psycopg2.connect(
    database="postgres",
    user="sherwood",
    password="",
    host="aello.cl.cam.ac.uk",
    port="5432",
)

# Create a cursor object
cur = conn.cursor()

# Define a polygon
# polygon = "POLYGON((-70 -2, -70 -1, -69 -1, -69 -2, -70 -2))"

# Read the shapefile
shape = gpd.read_file("/path/to/shapefile.shp")

# Iterate through each row in the shapefile
for index, row in shape.iterrows():
    # Convert the geometry to WKT format
    polygon = row.geometry.to_wkt()

    # Execute the SQL query
    query = f"""
        SELECT * 
        FROM filtered_l2ab_l4a_shots 
        WHERE 1=1
            AND ST_Within(geometry, ST_GeomFromText('{polygon}', 4326)) 
            AND absolute_time < '2022-04-01' 
            AND absolute_time > '2022-03-01' 
            AND beam_type = 'full' 
        --- Remove the limit to get all the shots
        LIMIT 10
    """
    cur.execute(query)

    # Fetch all the results into a pandas DataFrame
    df = pd.DataFrame(
        cur.fetchall(), columns=[desc[0] for desc in cur.description]
    )

    # Save the DataFrame to a CSV file
    df[
        [
            "shot_number",
            "pai",
            "lat_lowestmode",
            "lon_lowestmode",
            "absolute_time",
            "beam_type",
        ]
    ].to_csv(f"gedi_polygon_{index}.csv", index=False)

# Close the cursor and connection
cur.close()
conn.close()
