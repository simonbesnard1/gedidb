import unittest
import pathlib
import re
import warnings
from gedidb.granule import gedi_l4a, gedi_l2b, gedi_l2a
from gedidb.database import gedidb_schema
from gedidb.database import column_to_field

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

    def _rh_filter(self, col):
        """Filter out RH columns that are not every 5th value, or RH 98"""
        rh_re = re.compile(r"rh_(\d+)_level2A")
        if not rh_re.match(col):
            return True
        if col == "rh_98_level2A":
            return True
        else:
            return int(rh_re.match(col).group(1)) % 5 == 0

    def test_all_granule_fields_mapped(self):
        """Test that all granule fields are present in the column-to-field map."""
        granule = gedi_l4a.L4AGranule(L4A_NAME)
        for beam in granule.iter_beams():
            gdf = beam.main_data
            l4a_cols = gdf.columns
            break
        granule = gedi_l2a.L2AGranule(L2A_NAME)
        for beam in granule.iter_beams():
            gdf = beam.main_data
            l2a_cols = gdf.columns
            break
        granule = gedi_l2b.L2BGranule(L2B_NAME)
        for beam in granule.iter_beams():
            gdf = beam.main_data
            l2b_cols = gdf.columns
            break

        ctfmap = column_to_field.COLUMN_TO_FIELD

        # Deduplicate: many columns are present in multiple products
        # We choose the lowest level product that contains the field
        # with a few exceptions specified below.
        cl4a = [
            col + "_level4A"
            for col in l4a_cols
            if not (col in l2a_cols or col in l2b_cols)
        ]
        cl2b = [col + "_level2B" for col in l2b_cols if not (col in l2a_cols)]
        cl2a = [col + "_level2A" for col in l2a_cols]

        # Intended exceptions:
        # We pre-filter for shots that pass the L2A quality checks,
        # so we do not explicitly include the flag in the DB.
        cl2a.remove("quality_flag_level2A")
        cl2b.remove("l2a_quality_flag_level2B")
        cl4a.remove("l2_quality_flag_level4A")
        # L2B has less precise sensitivity data than other products
        cl2b.remove("sensitivity_level2B")
        # rh_100 is called rh100 in L2B
        cl2b.remove("rh100_level2B")
        # We use PFT and Region to understand the L4A algorithm choices.
        # Both fields are in the L2B product as well, but to be on the safe side
        # we take the L4A versions of these columns
        cl2b.remove("pft_class_level2B")
        cl2b.remove("region_class_level2B")
        cl4a.append("pft_class_level4A")
        cl4a.append("region_class_level4A")
        # L2B repeats the L2A selected algorithm
        cl2b.remove("selected_l2a_algorithm_level2B")
        # Primary keys and metadata don't get suffixes
        # because they are confirmed to be the same across all products
        # (rather than deriving from a single product as the source of truth)
        cl2a.remove("shot_number_level2A")
        cl2a.remove("granule_name_level2A")
        cl2a.remove("geometry_level2A")
        cl2a.extend(["shot_number", "granule", "geometry"])
        # For unknown reasons, I chose to get these cols from L2B instead of L2A
        cl2a.remove("longitude_bin0_error_level2A")
        cl2a.remove("latitude_bin0_error_level2A")
        cl2a.remove("elevation_bin0_error_level2A")
        cl2b.extend(
            [
                "longitude_bin0_error_level2B",
                "latitude_bin0_error_level2B",
                "elevation_bin0_error_level2B",
            ]
        )
        # We only put every 5th RH value in the DB
        cl2a = [col for col in cl2a if self._rh_filter(col)]

        # Unintended exceptions:
        print(
            """\nDue to unfortunate mistakes, the following columns are not in
              the database. The test enforces the current DB schema,
              but users developing a new database by forking this repo
              should consider including these columns in COLUMN_TO_FIELD
              and the database schema, and updating the test.\n"""
        )
        cl2b.remove("algorithmrun_flag_level2B")
        warnings.warn("algorithmrun_flag_level2B not in Columns", UserWarning)
        cl2b.remove("l2b_quality_flag_level2B")
        warnings.warn("l2b_quality_flag_level2B not in Columns", UserWarning)
        cl2a.remove("surface_flag_level2A")
        warnings.warn("surface_flag_level2A not in Columns", UserWarning)
        # We SHOULD HAVE "selected_algorithm_level4A" in the DB
        # but unfortunately it has the same name as "selected_algorithm_level2A"
        # and we only included the L2A one.
        # A correct test would include:
        # cl4a.append("selected_algorithm_level4A")
        warnings.warn("selected_algorithm_level4A not in Columns", UserWarning)

        # Other exceptions:
        # I don't think I care about these fields? But they're not in the DB.
        # Arguably they should be removed from granule parsing.
        cl2b.remove("waveform_count_level2B")
        cl2b.remove("waveform_start_level2B")
        # Oops, I named this column incorrectly in L2B
        # as "water_persistence" instead of "landsat_water_persistence"
        # so it isn't correctly deduplicated with L4A.
        cl4a.remove("landsat_water_persistence_level4A")

        # Check that all columns are accounted for in the mapping
        for col in cl4a + cl2b + cl2a:
            if col not in ctfmap.values():
                self.fail(f"Column {col} not found in column_to_field map")

    def test_all_columns_in_schema(self):
        """Test that all mapped columns are in the DB schema."""
        ccollection = gedidb_schema.Shots.__table__.columns
        cnames = [c.name for c in ccollection]

        ctfmap = column_to_field.COLUMN_TO_FIELD
        for c in ctfmap.keys():
            if c not in cnames:
                self.fail(f"Column {c} not found in schema")
