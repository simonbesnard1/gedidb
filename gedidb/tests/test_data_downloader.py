# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

"""Test that the provided Granules will be downloaded properly.
"""

import unittest
import os
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.utils.constants import GediProduct

# products with small size for faster testcases:
# structured by [size, url, product]
L2A = [217.657,
       'GEDI02_A_2019110062417_O01994_04_T02062_02_003_01_V002.h5',
       'https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_A.002/GEDI02_A_2019110062417_O01994_04_T02062_02_003_01_V002/GEDI02_A_2019110062417_O01994_04_T02062_02_003_01_V002.h5',
       'level2A']
L2B = [41.368,
       'GEDI02_B_2020195091334_O08976_02_T02866_02_003_01_V002.h5',
       'https://data.lpdaac.earthdatacloud.nasa.gov/lp-prod-protected/GEDI02_B.002/GEDI02_B_2020195091334_O08976_02_T02866_02_003_01_V002/GEDI02_B_2020195091334_O08976_02_T02866_02_003_01_V002.h5',
       'level2B']
L4A = [28.135,
       'GEDI04_A_2019110062417_O01994_04_T02062_02_002_02_V002.h5',
       'https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4A_AGB_Density_V2_1/data/GEDI04_A_2019110062417_O01994_04_T02062_02_002_02_V002.h5',
       'level4A']
L4C = [73.182,
       'GEDI04_C_2019110062417_O01994_04_T02062_02_001_01_V002.h5',
       'https://data.ornldaac.earthdata.nasa.gov/protected/gedi/GEDI_L4C_WSCI/data/GEDI04_C_2019110062417_O01994_04_T02062_02_001_01_V002.h5',
       'level4C']


class TestH5Downloader(unittest.TestCase):

    @classmethod
    def setUp(cls):
        os.chdir(os.path.dirname(__file__))
        if not os.path.exists("data/downloads"):
            os.mkdir("data/downloads")

    def test_l2a(self):
        granule_key, path = H5FileDownloader("data/downloads").download(L2A[1], L2A[2], GediProduct.L2A)
        self.assertTrue(os.path.exists(path[1]), f"File {path[1]} does not exist")
        self.assertAlmostEqual(os.path.getsize(path[1])/(1024 ** 2), L2A[0], 2, f"File {path[1]} does not have the correct size")

    def test_l2b(self):
        granule_key, path = H5FileDownloader("data/downloads").download(L2B[1], L2B[2], GediProduct.L2B)
        self.assertTrue(os.path.exists(path[1]), f"File {path[1]} does not exist")
        self.assertAlmostEqual(os.path.getsize(path[1])/(1024 ** 2), L2B[0], 2, f"File {path[1]} does not have the correct size")

    def test_l4a(self):
        granule_key, path = H5FileDownloader("data/downloads").download(L4A[1], L4A[2], GediProduct.L4A)
        self.assertTrue(os.path.exists(path[1]), f"File {path[1]} does not exist")
        self.assertAlmostEqual(os.path.getsize(path[1])/(1000**2), L4A[0], 2, f"File {path[1]} does not have the correct size")

    def test_l4c(self):
        granule_key, path = H5FileDownloader("data/downloads").download(L4C[1], L4C[2], GediProduct.L4C)
        self.assertTrue(os.path.exists(path[1]), f"File {path[1]} does not exist")
        self.assertAlmostEqual(os.path.getsize(path[1])/(1000**2), L4C[0], 2, f"File {path[1]} does not have the correct size")
