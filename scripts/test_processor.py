# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import gedidb as gdb
import boto3

#%% Get credentials
session = boto3.Session()
creds = session.get_credentials()
credentials = {
                "AccessKeyId": creds.access_key,
                "SecretAccessKey": creds.secret_key
                }

config_file = "../config_files/data_config.yml"
n_workers = 5

if __name__ == "__main__":

    # Initialize the GEDIProcessor and compute
    with gdb.GEDIProcessor(
        config_file=config_file,
        n_workers=n_workers
    ) as processor:
        processor.compute(consolidate=True)



