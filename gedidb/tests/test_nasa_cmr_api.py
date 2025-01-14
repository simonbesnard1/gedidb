# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

"""Test that the NASA CMR API has not changed recently.
This test will probably be flaky because it depends on the NASA CMR API.
However, if we don't run this test, they tend to silently disable old API
endpoints, which can break our code.
"""

# === UPDATED ===

import os
import yaml

from gedidb.downloader import cmr_query
from gedidb.utils import constants

import unittest
import geopandas as gpd
import datetime as dt
import geodatasets


class TestNasaCmrApi(unittest.TestCase):

    def _get_test_data(self):
        os.chdir(
            os.path.dirname(__file__)
        )  # this is needed to import local files such as the config

        return (
            gpd.read_file(geodatasets.get_path("naturalearth.land")).iloc[[50]],
            dt.datetime(2020, 10, 1),
            dt.datetime(2020, 11, 1),
            self._get_earthdata_from_config(),
        )

    def _get_earthdata_from_config(self):
        with open("data/data_config.yml", "r") as file:
            return yaml.safe_load(file)["earth_data_info"]

    def test_l2a(self):
        geom, start_date, end_date, earth_data_info = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L2A, geom, start_date, end_date, earth_data_info
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)

    def test_l2b(self):
        geom, start_date, end_date, earth_data_info = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L2B, geom, start_date, end_date, earth_data_info
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)

    def test_l4a(self):
        geom, start_date, end_date, earth_data_info = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L4A, geom, start_date, end_date, earth_data_info
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)

    def test_l4c(self):
        geom, start_date, end_date, earth_data_info = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L4C, geom, start_date, end_date, earth_data_info
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)
