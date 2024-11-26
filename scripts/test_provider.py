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
import cProfile
import pstats

#%% Instantiate the GEDIProvider
provider = gdb.GEDIProvider(storage_type='s3', s3_bucket="dog.gedidb.gedi-l2-l4-v002",
                            endpoint_override="https://s3.gfz-potsdam.de")

#%% Load region of interest
region_of_interest = gpd.read_file('/home/simon/Documents/science/GFZ/projects/gedi-toolbox/data/geojson/BR-Sa3.geojson')

# Define the columns to query and additional parameters
vars_selected = ['agbd', 'sensitivity', 'energy_total']
quality_filters = {
    'sensitivity': '>= 0.9 and <= 1.0',
    'beam_type': '= full'
}

# Profile the provider's `get_data` function
def profile_get_data():
    gedi_data = provider.get_data(
        variables=vars_selected,
        query_type="boundind_box",
        geometry=region_of_interest,
        start_time="2019-07-21",
        end_time="2024-07-25",
        return_type='xarray'
    )

# Run profiling
profiler = cProfile.Profile()
profiler.enable()
profile_get_data()
profiler.disable()

# Analyze the slowest functions
stats = pstats.Stats(profiler)
stats.strip_dirs()
stats.sort_stats('time')  # Sort by cumulative time
stats.print_stats(5)  # Print the 5 slowest functions
