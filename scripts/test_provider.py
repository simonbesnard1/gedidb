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
region_of_interest = gpd.read_file('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/geojson/BR-Sa3.geojson')

# Define the columns to query and additional parameters
vars_selected = ['rh', 'agbd']
quality_filters = {
    'sensitivity': '>= 0.9 AND <= 1.0',
    'surface_flag': '= 1'
}

gedi_data = provider.get_data(variables=vars_selected, geometry=None, 
                               start_time="2018-01-01", end_time="2024-12-31", 
                               limit=None, force=True, order_by=["-shot_number"], 
                               return_type='pandas')
    

