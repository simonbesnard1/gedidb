# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import gedidb as gdb

data_config_file = "./config_files/data_config.yml"
sql_config_file = './config_files/db_scheme.sql'

if __name__ == '__main__':

    # Using GEDIProcessor as a context manager
    with gdb.GEDIProcessor(data_config_file, sql_config_file, n_workers=4) as processor:
        processor.compute()
        # The Dask dashboard URL will be printed in the logs
        # You can access it at http://localhost:8787 to monitor resource usage
    
    # The Dask client and cluster are automatically closed when exiting the 'with' block
    
