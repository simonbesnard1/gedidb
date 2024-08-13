import unittest
from gedidb.granule import gedi_l4a, gedi_l2b, gedi_l2a, gedi_l1b
import pathlib
import warnings

THIS_DIR = pathlib.Path(__file__).parent
L4A_NAME = (
    THIS_DIR
    / "data"
    / "GEDI04_A_2019117051430_O02102_01_T04603_02_002_02_V002.h5"
).as_posix()
L2B_NAME = (
    THIS_DIR
    / "data"
    / "GEDI02_B_2019117051430_O02102_01_T04603_02_003_01_V002.h5"
).as_posix()
L2A_NAME = (
    THIS_DIR
    / "data"
    / "GEDI02_A_2019162222610_O02812_04_T01244_02_003_01_V002.h5"
).as_posix()


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        warnings.simplefilter("ignore", DeprecationWarning)

    def _generic_test_parse_granule(self, granule):
        n_filtered = 0
        for beam in granule.iter_beams():
            hdf_beam_len = beam.n_shots
            # All beams are non-empty
            # (Not true for all files -- but true for the test files)
            self.assertNotEqual(hdf_beam_len, 0)
            gdf = beam.main_data
            data_len_orig = len(gdf)
            # The exported shots are equal to the number of shots in the h5 file
            self.assertEqual(data_len_orig, hdf_beam_len)

            beam.sql_format_arrays()
            gdf = beam.main_data
            # SQL formatting doesn't change the number of shots
            self.assertEqual(data_len_orig, len(gdf))

            # Quality filtering doesn't crash
            beam.quality_filter()
            gdf = beam.main_data
            # Quality filtering should drop at least some shots
            self.assertLessEqual(len(gdf), data_len_orig)
            n_filtered += len(gdf)
        # But at least _some_ shots should be valid
        # (Again, maybe not true for all files --
        # but true for the test files)
        self.assertNotEqual(n_filtered, 0)

    def test_parse_granule_l4a(self):
        granule = gedi_l4a.L4AGranule(L4A_NAME)
        self.assertEqual(granule.n_beams, 8)
        self._generic_test_parse_granule(granule)

    def test_parse_granule_l2b(self):
        granule = gedi_l2b.L2BGranule(L2B_NAME)
        self.assertEqual(granule.n_beams, 8)
        self._generic_test_parse_granule(granule)

    def test_parse_granule_l2a(self):
        granule = gedi_l2a.L2AGranule(L2A_NAME)
        self.assertEqual(granule.n_beams, 8)
        self._generic_test_parse_granule(granule)


suite = unittest.TestLoader().loadTestsFromTestCase(TestCase)
