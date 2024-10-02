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


#%% Load region of interest
region_of_interest = gpd.read_file('./data/geojson/BR-Sa1.geojson')

#%% Instantiate the GEDIProvider
provider = gdb.GEDIProvider(config_file='./config_files/data_config.yml',
                            table_name="filtered_l2ab_l4ac_shots",
                            metadata_table="variable_metadata")

#%% Define the columns to query and additional parameters
vars_selected = 'all'
dataset = provider.get_data(variables=vars_selected, geometry=region_of_interest, 
                               start_time="2018-01-01", end_time="2024-12-31", 
                               limit=None, force=True, order_by=["-shot_number"], 
                               return_type='xarray')
dataset = dataset.drop_vars(['beam_name', 'granule', 'version', 'beam_type'])
dataset.to_netcdf('./data/netcdf/BR-Sa1_gedi.nc', mode= 'w', engine='h5netcdf')
