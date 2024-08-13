"""Read from the database by joining with a local shapefile."""

import psycopg2
import geopandas as gpd
import pandas as pd

# Connect to the PostGIS database
conn = psycopg2.connect(
    database="postgres",
    user="gedi_readonly",
    password="",
    host="aello.cl.cam.ac.uk",
    port="5432",
)

# Create a cursor object
cur = conn.cursor()

# Read and reformat the geodataframe
roi = gpd.read_file("~/shapefiles/Allometry_plot_locations_4326.gpkg")
roi["wkt"] = roi.geometry.to_wkt()

# Pick only the ID and geometry columns for the insert and join
rows = zip(roi.id_unique, roi.wkt)
cur.execute(
    """
    --sql 
    CREATE TEMP TABLE temporary(id_unique FLOAT, wkt TEXT) ON COMMIT DROP;"""
)
cur.executemany(
    """
    --sql
    INSERT INTO temporary (id_unique, wkt) VALUES (%s, %s);
    """,
    rows,
)

# # Execute the sql query
query = """
    --sql
    SELECT *
    FROM filtered_l2ab_l4a_shots AS s
    JOIN temporary AS t
    ON ST_DWithin(s.geometry::geography, ST_GeomFromText(t.wkt, 4326)::geography, 1000);
"""

cur.execute(query)
res = pd.DataFrame(
    cur.fetchall(), columns=[desc[0] for desc in cur.description]
)
res = gpd.GeoDataFrame(res, geometry=gpd.GeoSeries.from_wkb(res.geometry))
res.rename(columns={"wkt": "plot_wkt"}, inplace=True)

# res.drop(columns=["pai_z", "pavd_z", "cover_z"], inplace=True)
# for c in res.columns:
#     if type(res[c].values[0]) is list:
#         print(c, res[c].values[0])
res.to_parquet("/maps/forecol/data/gedi_allometries_1000m.parquet")

cur.close()
