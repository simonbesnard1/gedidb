"""Test that the NASA CMR API has not changed recently.
This test will probably be flaky because it depends on the NASA CMR API.
However, if we don't run this test, they tend to silently disable old API 
endpoints, which can break our code.
"""

from gedidb.downloader import cmr_query
from gedidb.utils import constants

import unittest
import geopandas as gpd
import datetime as dt
import geodatasets


class TestNasaCmrApi(unittest.TestCase):

    def _get_test_data(self):
        return (
            gpd.read_file(geodatasets.get_path("naturalearth.land")).iloc[[50]],
            dt.datetime(2020, 10, 1),
            dt.datetime(2020, 11, 1),
        )

    def test_l2a(self):
        geom, start_date, end_date = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L2A, geom, start_date, end_date
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)
    
    def test_l2b(self):
        geom, start_date, end_date = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L2B, geom, start_date, end_date
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)
    
    def test_l4a(self):
        geom, start_date, end_date = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L4A, geom, start_date, end_date
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)

    def test_l4c(self):
        geom, start_date, end_date = self._get_test_data()
        cmr = cmr_query.GranuleQuery(
            constants.GediProduct.L4C, geom, start_date, end_date
        )
        granules = cmr.query_granules()
        self.assertGreater(len(granules), 0)
