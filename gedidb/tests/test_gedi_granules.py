# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#
import os
import unittest
from gedidb.granule.granule_parser import parse_h5_file
from gedidb.utils.constants import GediProduct
import pathlib
import warnings
import h5py

# THIS_DIR = pathlib.Path(__name__).parent
THIS_DIR = pathlib.Path.cwd().parent
L4A_NAME = "./data/GEDI04_A_2019117051430_O02102_01_T04603_02_002_02_V002.h5"
L4C_NAME = "./data/GEDI04_C_2019110062417_O01994_04_T02062_02_001_01_V002.h5"
L2B_NAME = "./data/GEDI02_B_2019117051430_O02102_01_T04603_02_003_01_V002.h5"
L2A_NAME = "./data/GEDI02_A_2019162222610_O02812_04_T01244_02_003_01_V002.h5"


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        warnings.simplefilter("ignore", DeprecationWarning)
        os.chdir(os.path.dirname(__file__))

    _data_info = {
        "level_2a": {
            "quality_filter": "None",
            "variables": {
                "shot_number": {
                    "SDS_Name": "shot_number",
                },
                "beam_name": {
                    "SDS_Name": "name",
                },
                "lat_lowestmode": {
                    "SDS_Name": "lat_lowestmode",
                },
                "lon_lowestmode": {
                    "SDS_Name": "lon_lowestmode",
                },
                "rh": {
                    "SDS_Name": "rh",
                },
            },
        },
        "level_2b": {
            "quality_filter": "None",
            "variables": {
                "shot_number": {
                    "SDS_Name": "shot_number",
                },
                "beam_name": {
                    "SDS_Name": "name",
                },
                "lat_lowestmode": {
                    "SDS_Name": "geolocation/lat_lowestmode",
                },
                "lon_lowestmode": {
                    "SDS_Name": "geolocation/lon_lowestmode",
                },
                "pai_z": {
                    "SDS_Name": "pai_z",
                },
            },
        },
        "level_4a": {
            "quality_filter": "None",
            "variables": {
                "shot_number": {
                    "SDS_Name": "shot_number",
                },
                "beam_name": {
                    "SDS_Name": "name",
                },
                "lat_lowestmode": {
                    "SDS_Name": "lat_lowestmode",
                },
                "lon_lowestmode": {
                    "SDS_Name": "lon_lowestmode",
                },
                "agbd": {
                    "SDS_Name": "agbd",
                },
            },
        },
        "level_4c": {
            "quality_filter": "None",
            "variables": {
                "shot_number": {
                    "SDS_Name": "shot_number",
                },
                "beam_name": {
                    "SDS_Name": "name",
                },
                "lat_lowestmode": {
                    "SDS_Name": "lat_lowestmode",
                },
                "lon_lowestmode": {
                    "SDS_Name": "lon_lowestmode",
                },
            },
        },
    }

    def _generic_test_parse_granule(self, file, data):
        # All beams are non-empty
        # (Not true for all files -- but true for the test files)
        beam_data = data.groupby("beam_name").count()
        self.assertEqual(len(beam_data), 8)
        for beam in beam_data.index:
            self.assertNotEqual(beam_data.loc[beam, "shot_number"], 0)

        data_orig = h5py.File(file, "r")
        for beam in beam_data.index:
            hdf_beam_len = len(data_orig[beam]["shot_number"])
            # this test will always return different results, as long as the quality filter gets applied
            # self.assertEqual(beam_data.loc[beam, "shot_number"], hdf_beam_len)

            # right now we check if the quality filter gets applied, i.e. we get less entries with the parsed data
            # than with the original data
            self.assertLessEqual(beam_data.loc[beam, "shot_number"], hdf_beam_len,
                                 "Quality filter returned more data than before")

    def test_parse_granule_l4a(self):
        data = parse_h5_file(
            L4A_NAME,
            GediProduct.L4A.value,
            data_info=self._data_info,
        )

        self._generic_test_parse_granule(L4A_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(L4A_NAME, "r")
        # TODO: idx needs to correspond to a shot_number which won't be initially quality filtered
        idx = 1
        shot_number = data_orig["BEAM1000"]["shot_number"][idx]
        lat = data_orig["BEAM1000"]["lat_lowestmode"][idx]
        lon = data_orig["BEAM1000"]["lon_lowestmode"][idx]
        agbd = data_orig["BEAM1000"]["agbd"][idx]

        row = data.loc[data["shot_number"] == shot_number]
        self.assertEqual(row["lat_lowestmode"].values[0], lat)
        self.assertEqual(row["lon_lowestmode"].values[0], lon)
        self.assertEqual(row["agbd"].values[0], agbd)

    def test_parse_granule_l4c(self):
        data = parse_h5_file(
            L4C_NAME,
            GediProduct.L4C.value,
            data_info=self._data_info,
        )

        self._generic_test_parse_granule(L4C_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(L4C_NAME, "r")
        # TODO: idx needs to correspond to a shot_number which won't be initially quality filtered
        idx = 1
        shot_number = data_orig["BEAM1000"]["shot_number"][idx]
        lat = data_orig["BEAM1000"]["lat_lowestmode"][idx]
        lon = data_orig["BEAM1000"]["lon_lowestmode"][idx]
        # agbd = data_orig["BEAM1000"]["agbd"][idx]

        row = data.loc[data["shot_number"] == shot_number]
        self.assertEqual(row["lat_lowestmode"].values[0], lat)
        self.assertEqual(row["lon_lowestmode"].values[0], lon)
        # self.assertEqual(row["agbd"].values[0], agbd)

    def test_parse_granule_l2b(self):
        data = parse_h5_file(
            L2B_NAME,
            GediProduct.L2B.value,
            data_info=self._data_info,
        )
        data_orig = h5py.File(L2B_NAME, "r")
        # TODO: idx needs to correspond to a shot_number which won't be initially quality filtered
        idx = 1646
        shot_number = data_orig["BEAM1000"]["shot_number"][idx]
        lat = data_orig["BEAM1000"]["geolocation"]["lat_lowestmode"][idx]
        lon = data_orig["BEAM1000"]["geolocation"]["lon_lowestmode"][idx]
        pai_z1 = data_orig["BEAM1000"]["pai_z"][idx][0]

        row = data.loc[data["shot_number"] == shot_number]
        self.assertEqual(row["lat_lowestmode"].values[0], lat)
        self.assertEqual(row["lon_lowestmode"].values[0], lon)
        self.assertEqual(row["pai_z_1"].values[0], pai_z1)

    def test_parse_granule_l2a(self):
        data = parse_h5_file(
            L2A_NAME,
            GediProduct.L2A.value,
            data_info=self._data_info,
        )
        self._generic_test_parse_granule(L2A_NAME, data)
        # Some of the data is correct
        data_orig = h5py.File(L2A_NAME, "r")
        idx = 10000
        shot_number = data_orig["BEAM1000"]["shot_number"][idx]
        lat = data_orig["BEAM1000"]["lat_lowestmode"][idx]
        lon = data_orig["BEAM1000"]["lon_lowestmode"][idx]
        # rh_98 = data_orig["BEAM1000"]["rh"][idx][98]

        row = data.loc[data["shot_number"] == shot_number]
        self.assertEqual(row["lat_lowestmode"].values[0], lat)
        self.assertEqual(row["lon_lowestmode"].values[0], lon)
        # self.assertEqual(row["rh_98"].values[0], rh_98)

    # TODO basic tests of quality filtering


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
