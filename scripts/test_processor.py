# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import gedidb as gdb

if __name__ == '__main__':

    #%% Initiate database builder
    database_builder = gdb.GEDIProcessor(data_config_file = "./config_files/data_config.yml", 
                                         sql_config_file='./config_files/db_scheme.sql')
    
    #%% Process GEDI data
    database_builder.compute(n_workers=4)

