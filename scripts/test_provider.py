# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#!/usr/bin/env python3

#

# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 12:53:09 2024

@author: simon
"""
from gedidb.providers.gedi_provider import GEDIProvider

#%% Instantiate the GEDIProvider
provider = GEDIProvider(config_file='./config_files/data_config.yml',
                        table_name="filtered_l2ab_l4ac_shots",
                        metadata_table="variable_metadata")

#%% Define the columns to query and additional parameters
vars_selected = ["rh", "pavd_z", "pai"]
dataset = provider.get_dataset(variables=vars_selected, geometry=None, 
                               start_time="2018-01-01", end_time="2023-12-31", 
                               limit=100, force=True, order_by=["-shot_number"], 
                               return_type='xarray')

