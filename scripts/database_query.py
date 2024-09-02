#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 17:08:23 2024

@author: simon
"""
from gedidb.providers.db_query import SQLQueryBuilder, GediDatabase

# Instantiate the database connection
config_file = './config_files/data_config.yml' 
db = GediDatabase(data_config_file = config_file)

# Create the query builder
query_builder = SQLQueryBuilder(
                                table_name="filtered_l2ab_l4ac_shots",
                                columns=["shot_number", "beam_name", "rh"],
                                start_time="2023-01-01",
                                end_time="2023-12-31",
                                limit=100,
                                force=True,
                                order_by=["-shot_number"]
                            )

# Execute the query
df = db.query("filtered_l2ab_l4ac_shots", query_builder=query_builder, use_geopandas=True)
