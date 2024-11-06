# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import gedidb as gdb

<<<<<<< HEAD
data_config_file = "./config_files/data_config.yml"
sql_config_file = './config_files/db_scheme.sql'
n_workers = 5
=======
config_file = "./config_files/data_config.yml"
n_workers = 2
>>>>>>> optimise tiledb schema with two arrays

if __name__ == "__main__":

    # Initialize the GEDIProcessor and compute
    with gdb.GEDIProcessor(
        data_config_file=data_config_file,
        sql_config_file=sql_config_file,
        n_workers=n_workers
    ) as processor:
        processor.compute()
    
