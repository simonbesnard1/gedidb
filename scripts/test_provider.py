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

import time
start_time_ = time.time()

#%% Instantiate the GEDIProvider
provider = gdb.GEDIProvider(storage_type='s3', s3_bucket="dog.gedidb.gedi-l2-l4-v002",
                            endpoint_override="https://s3.gfz-potsdam.de")

#%% Load region of interest
region_of_interest = gpd.read_file('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/geojson/BR-Sa3.geojson')

# Define the columns to query and additional parameters
vars_selected = ['rh', 'agbd', 'sensitivity', 'energy_total', "rh98"]
quality_filters = {
    'sensitivity': '>= 0.9 and <= 1.0',
    'beam_type': '= full'
}

# Profile the provider's `get_data` function
gedi_data = provider.get_data(
    variables=vars_selected,
    query_type="boundind_box",
    geometry=region_of_interest,
    start_time="2019-07-21",
    end_time="2024-07-25",
    return_type='xarray'
)
print("--- %s seconds ---" % (time.time() - start_time_))


