# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import geopandas as gpd
import gedidb as gdb

#%% Instantiate the GEDIProvider
provider = gdb.GEDIProvider(config_file='/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/data_config.yml',
                            table_name="filtered_l2ab_l4ac_shots",
                            metadata_table="variable_metadata")

#%% Load region of interest
region_of_interest = gpd.read_file('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/geojson/test_patches.geojson')

#%% Loop over each polygon in the GeoDataFrame
total_ = 0
for index, patch_geom in region_of_interest.iterrows():
    
    patch_geom = gpd.GeoSeries(patch_geom)

    # Define the columns to query and additional parameters
    vars_selected = ['rh', 'pavd_z', 'pai']
    gedi_data = provider.get_data(variables=vars_selected, geometry=patch_geom, 
                                   start_time="2018-01-01", end_time="2024-12-31", 
                                   limit=None, force=True, order_by=["-shot_number"], 
                                   return_type='xarray')


#%%
import xarray as xr
import numpy as np

# Patch extent in degrees
minx, miny, maxx, maxy = -54.724, -2.448, -54.586, -2.31

# Define 30m resolution in degrees (approx. 30 meters at the equator)
resolution_deg = 30 / 111320  # Convert meters to degrees

# Create a grid of coordinates with the 30-meter resolution
x_coords = np.arange(minx, maxx, resolution_deg)
y_coords = np.arange(miny, maxy, resolution_deg)

# Initialize an empty grid with original time dimension and profile_points as a coordinate
grid = xr.Dataset(
    coords={
        "x": x_coords, 
        "y": y_coords, 
        "time": gedi_data.absolute_time.values
    },
    attrs={"description": "GEDI gridded data for 512x512 patch with 30m pixels"}
)

# Create an array for rh, now only with time as a dimension
# profile_points will be used as a coordinate in each time slice
grid["rh"] = xr.DataArray(
    np.full((len(y_coords), len(x_coords), len(gedi_data.absolute_time), len(gedi_data.profile_points)), np.nan),
    dims=["y", "x", "time"],
    coords={"profile_points": ("time", gedi_data.profile_points.values)}
)

# Populate the grid cells with the GEDI data for each shot
for shot_idx in range(len(gedi_data.shot_number)):
    # Get the GEDI shot's latitude and longitude
    lon, lat = gedi_data.longitude[shot_idx].item(), gedi_data.latitude[shot_idx].item()
    
    # Find the nearest grid cell indices for x and y
    x_idx = (np.abs(x_coords - lon)).argmin()
    y_idx = (np.abs(y_coords - lat)).argmin()
    
    # Assign data to the grid
    grid["rh"][y_idx, x_idx, shot_idx, :] = gedi_data["rh"][shot_idx, :]

# Output the gridded dataset
grid
