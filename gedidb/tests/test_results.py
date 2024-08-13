import unittest
import pandas as pd
from sqlalchemy import text
from gedidb.database import gedidb_common


class TestCase(unittest.TestCase):

    def test_db_read(self):

        wkt = "ST_GeomFromText('POLYGON ((-75.50007586369453 -7.999946762726399, -73.49997688360241 -7.999946762726399, -73.49997688360241 -10.092662048611237, -73.49997688360241 -11.000050317100365, -75.50007586369453 -11.000050317100365, -75.50007586369453 -10.092662048611237, -75.50007586369453 -7.999946762726399))', 4326)"

        engine = gedidb_common.get_engine()
        with engine.connect() as conn:
            gdf = pd.read_sql(
                text(
                    f"SELECT COUNT(*) FROM filtered_l2ab_l4a_shots WHERE ST_Within(geometry, {wkt}) AND absolute_time < '2020-04-01'"
                ),
                conn,
            )
            self.assertTrue(gdf["count"].values[0] > 0)
