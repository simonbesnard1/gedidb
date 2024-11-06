# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
import tiledb
import numpy as np
import pandas as pd

# Define synthetic domain and schema with float64 dimensions
lat_min, lat_max = -90.0, 90.0
lon_min, lon_max = -180.0, 180.0
time_min, time_max = 1514764800000000, 1924991999000000  # Microseconds since epoch

domain = tiledb.Domain(
    tiledb.Dim(name="latitude", domain=(lat_min, lat_max), tile=0.1, dtype="float64"),
    tiledb.Dim(name="longitude", domain=(lon_min, lon_max), tile=0.1, dtype="float64"),
    tiledb.Dim(name="time", domain=(time_min, time_max), tile=1000000, dtype="int64")
)

attr = tiledb.Attr(name="intensity", dtype="float32")
schema = tiledb.ArraySchema(domain=domain, attrs=[attr], sparse=True)

array_uri = "test_array_float"
if tiledb.array_exists(array_uri):
    tiledb.remove(array_uri)
tiledb.Array.create(array_uri, schema)

# Write data
with tiledb.open(array_uri, mode="w") as array:
    np.random.seed(0)
    lat_data = np.random.uniform(lat_min, lat_max, size=100)
    lon_data = np.random.uniform(lon_min, lon_max, size=100)
    time_data = np.random.randint(time_min, time_max, size=100)
    intensity_data = np.random.rand(100).astype("float32")

    print("Data Latitude Bounds:", lat_data.min(), lat_data.max())
    print("Data Longitude Bounds:", lon_data.min(), lon_data.max())

    array[lat_data, lon_data, time_data] = {"intensity": intensity_data}

# Query data
with tiledb.open(array_uri, mode="r") as array:
    lat_slice_min, lat_slice_max = -30.0, 30.0
    lon_slice_min, lon_slice_max = -150.0, 150.0
    time_slice_min, time_slice_max = 1514764800000000, 1924991999000000  # Adjust as needed

    # Ensure bounds are sorted
    lat_slice_min, lat_slice_max = sorted([lat_slice_min, lat_slice_max])
    lon_slice_min, lon_slice_max = sorted([lon_slice_min, lon_slice_max])

    # Print slicing bounds
    print("Latitude Bounds:", lat_slice_min, lat_slice_max)
    print("Longitude Bounds:", lon_slice_min, lon_slice_max)

    # Perform the query using multi_index
    query = array.query(attrs=["intensity"])
    data = query.multi_index[
        lat_slice_min:lat_slice_max,
        lon_slice_min:lon_slice_max,
        time_slice_min:time_slice_max
    ]

    print("Queried data:")
    print(data)
