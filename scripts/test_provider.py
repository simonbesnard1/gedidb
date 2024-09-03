#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep  3 12:53:09 2024

@author: simon
"""
from gedidb.providers.gedi_provider import GEDIProvider

#%% Instantiate the GEDIProvider
provider = GEDIProvider(config_file='./config_files/data_config.yml',
                        table_name="filtered_l2ab_l4ac_shots")

#%% Define the columns to query and additional parameters
columns = ["shot_number", "beam_name", 'absolute_time', 'geometry', "pavd_z", "pai"]
dataset = provider.get_dataset(columns=columns, geometry=None, 
                               start_time="2019-01-01", end_time="2023-12-31", 
                               limit=100, force=True, order_by=["-shot_number"])

