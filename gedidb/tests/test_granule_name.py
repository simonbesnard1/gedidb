import unittest
from gedidb.granule import granule_name

TEST_V2_NAME = "GEDI02_A_2019268053258_O04446_04_T02132_02_003_01_V002.h5"
# GEDI02_A_YYYYDDDHHMMSS_O[orbit_number]_[granule_number]_T[track_number]_[PPDS_type]_ [release_number]_[production_version]_V[version_number].h5


class TestCase(unittest.TestCase):
    def test_parse_name(self):
        parsed = granule_name.parse_granule_filename(TEST_V2_NAME)
        self.assertEqual(parsed.product, "GEDI02_A")
        self.assertEqual(parsed.year, "2019")
        self.assertEqual(parsed.julian_day, "268")
        self.assertEqual(parsed.hour, "05")
        self.assertEqual(parsed.minute, "32")
        self.assertEqual(parsed.second, "58")
        self.assertEqual(parsed.orbit, "O04446")
        self.assertEqual(parsed.sub_orbit_granule, "04")
        self.assertEqual(parsed.ground_track, "T02132")
        self.assertEqual(parsed.positioning, "02")
        self.assertEqual(parsed.release_number, "003")
        self.assertEqual(parsed.granule_production_version, "01")
        self.assertEqual(parsed.major_version_number, "V002")


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
