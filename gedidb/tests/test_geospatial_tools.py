# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

# === UPDATED ===

import math
import unittest
from pathlib import Path

import geopandas as gpd
from gedidb.utils import geo_processing as gt

from shapely import MultiPolygon, Polygon

data_dir = Path(__file__).parent / "data"

class TestGeospatialTools(unittest.TestCase):
    def _make_polygon(self, lat: float, lng: float, radius: float) -> Polygon:
        origin_lat = lat + radius
        origin_lng = lng - radius
        far_lat = lat - radius
        far_lng = lng + radius

        return Polygon(
            [
                [origin_lng, origin_lat],
                [far_lng, origin_lat],
                [far_lng, far_lat],
                [origin_lng, far_lat],
                [origin_lng, origin_lat],
            ]
        )

    def test_simple_shape(self):

        polygon = self._make_polygon(10.0, 10.0, 0.3)
        geometry = gpd.GeoSeries(polygon)

        checked = gt.check_and_format_shape(geometry)
        self.assertIsNotNone(checked)

    def test_too_many_shapes(self):

        polygon1 = self._make_polygon(10.0, 10.0, 0.3)
        polygon2 = self._make_polygon(12.0, 12.0, 0.4)
        geometry = gpd.GeoSeries([polygon1, polygon2])

        with self.assertRaises(ValueError):
            _ = gt.check_and_format_shape(geometry)

    def test_multi_polygon_shapes(self):

        polygon1 = self._make_polygon(10.0, 10.0, 0.3)
        polygon2 = self._make_polygon(12.0, 12.0, 0.4)
        multipolygon = MultiPolygon([polygon1, polygon2])
        geometry = gpd.GeoSeries(multipolygon)

        checked = gt.check_and_format_shape(geometry)
        self.assertIsNotNone(checked)

    def test_too_many_points_dont_simplify(self):
        points = []
        for i in range(5001):
            angle = math.pi / (2 * 5001)
            points.append((math.sin(angle), math.cos(angle)))
        polygon = Polygon(points)
        geometry = gpd.GeoSeries(polygon)

        with self.assertRaises(gt.DetailError):
            _ = gt.check_and_format_shape(geometry)

    def test_too_many_points_simplify(self):
        # cheat a little bit and lower the max points threshold
        gt.MAX_CMR_COORDS = 100
        geometry = gpd.read_file(data_dir / "ne_110m_land.zip")
        # This row has 559 coordinates
        geometry = geometry.iloc[[7]]
        checked = gt.check_and_format_shape(geometry, simplify=True)
        checked.reset_index(drop=True, inplace=True)
        geometry.reset_index(drop=True, inplace=True)

        self.assertTrue(checked.contains(geometry).all())

    def test_simplified_still_too_many_points(self):
        points = []
        for i in range(5001):
            angle = (math.pi / (2 * 5001)) * i
            points.append((math.sin(angle), math.cos(angle)))
        polygon = Polygon(points)
        geometry = gpd.GeoSeries(polygon)

        with self.assertRaises(gt.DetailError):
            _ = gt.check_and_format_shape(geometry, simplify=True)
