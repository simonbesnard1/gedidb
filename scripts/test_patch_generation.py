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
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import contextily as ctx

#%% Instantiate the GEDIProvider
provider = gdb.GEDIProvider(config_file='/home/simon/Documents/science/GFZ/projects/gedi-toolbox/config_files/data_config.yml',
                            table_name="filtered_l2ab_l4ac_shots",
                            metadata_table="variable_metadata")

#%% Load region of interest
region_of_interest = gpd.read_file('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/geojson/test_patches.geojson')

#%% Loop over each polygon in the GeoDataFrame
fig, axes = plt.subplots(3, 3, figsize=(15, 10), constrained_layout=True)
axes = axes.flatten()  # Flatten for easy iteration over 12 subplots

for index, (ax, patch_geom) in enumerate(zip(axes, region_of_interest.iterrows())):
    if index >= 9:
        break  # Only process the first 12 patches
    
    patch_geom = gpd.GeoSeries(patch_geom[1]["geometry"])

    # Define the columns to query and additional parameters
    vars_selected = ['rh', 'agbd']
    gedi_data = provider.get_data(variables=vars_selected, geometry=patch_geom, 
                                  start_time="2018-01-01", end_time="2024-12-31", 
                                  limit=None, force=True, order_by=["-shot_number"], 
                                  return_type='xarray')
    
    # Patch extent in degrees
    minx, miny, maxx, maxy = patch_geom.total_bounds
    
    # Define 30m resolution in degrees (approx. 30 meters at the equator)
    resolution_deg = 30 / 111320  # Convert meters to degrees
    
    # Create a grid of coordinates with the 30-meter resolution
    x_coords = np.arange(minx, maxx, resolution_deg)
    y_coords = np.arange(miny, maxy, resolution_deg)
    
    # Define profile points (e.g., percentages of height: 0%, 10%, ..., 100%)
    profile_points = np.arange(0, 101, 1)
    
    # Initialize an empty grid with profile_points as a coordinate
    grid = xr.Dataset(
        coords={
            "longitude": x_coords, 
            "latitude": y_coords,
            "profile_points": profile_points
        },
        attrs={"description": "GEDI gridded data for 512x512 patch with 30m pixels"}
    )
    
    # Initialize arrays for `rh` and `agbd` in the grid with NaNs
    grid["rh"] = (("latitude", "longitude", "profile_points"), 
                  np.full((len(y_coords), len(x_coords), len(profile_points)), np.nan))
    grid["agbd"] = (("latitude", "longitude"), 
                    np.full((len(y_coords), len(x_coords)), np.nan))
    grid["rh"].attrs = gedi_data.rh.attrs
    grid["agbd"].attrs = gedi_data.agbd.attrs

    # Populate the grid cells with the GEDI data for each shot
    for i in range(len(gedi_data.shot_number)):
        # Find nearest latitude and longitude indices in the grid
        lat_idx = np.abs(grid.latitude - gedi_data.latitude[i]).argmin().item()
        lon_idx = np.abs(grid.longitude - gedi_data.longitude[i]).argmin().item()
        
        # Assign rh and agbd values to the grid at the found indices
        grid["rh"][lat_idx, lon_idx, :] = gedi_data["rh"][i, :]
        grid["agbd"][lat_idx, lon_idx] = gedi_data["agbd"][i]
        
    # Plot AGBD data for this patch
    grid["agbd"].plot(ax=ax, cmap="viridis", add_colorbar=True)
    ax.set_title(f"Patch {index+1} - AGBD (Above Ground Biomass Density)")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    
    # Add basemap to each subplot
    # ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery, crs="EPSG:4326")
    # ax.set_xlim(minx, maxx)
    # ax.set_ylim(miny, maxy)

plt.suptitle("GEDI Data for 9 Patches", fontsize=18)
plt.savefig('/home/simon/Documents/science/GFZ/presentation/3D-ABC/3d-ABC_14_10_24_SB/images/all_samples.png', dpi=300)   
