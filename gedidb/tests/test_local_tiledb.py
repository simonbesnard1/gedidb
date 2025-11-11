# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import tiledb
import yaml

from gedidb.core.gedidatabase import GEDIDatabase


class TestGEDIDatabase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Dynamically resolve the path to the `data` folder
        cls.data_dir = Path(__file__).parent / "data"
        cls.yaml_file_path = cls.data_dir / "data_config.yml"

        if not cls.yaml_file_path.exists():
            raise FileNotFoundError(f"Config file not found: {cls.yaml_file_path}")

        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

        # Override local TileDB path with a temporary directory
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.config["tiledb"]["local_path"] = cls.temp_dir.name

        # Initialize GEDIDatabase instance
        cls.gedi_db = GEDIDatabase(cls.config)
        cls.gedi_db._create_arrays()  # Create the TileDB array for testing

    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()

    def test_tiledb_dimensions(self):
        """Test that TileDB dimensions are configured correctly."""
        with tiledb.open(
            self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx
        ) as array:
            schema = array.schema
            dims = schema.domain

            # Check dimensions
            lat_dim = dims.dim("latitude")
            lon_dim = dims.dim("longitude")
            time_dim = dims.dim("time")

            self.assertIn(
                "latitude",
                lat_dim.name,
                "The 'latitude' dimension is missing from the TileDB schema.",
            )
            self.assertIn(
                "longitude",
                lon_dim.name,
                "The 'longitude' dimension is missing from the TileDB schema.",
            )
            self.assertIn(
                "time",
                time_dim.name,
                "The 'time' dimension is missing from the TileDB schema.",
            )

            self.assertEqual(lat_dim.domain, (-56.0, 56.0), "Latitude range mismatch")
            self.assertEqual(
                lon_dim.domain, (-180.0, 180.0), "Longitude range mismatch"
            )
            # Check chunk size
            self.assertEqual(lat_dim.tile, 1, "Latitude chunk size mismatch")
            self.assertEqual(lon_dim.tile, 1, "Longitude chunk size mismatch")

    def test_tiledb_attributes(self):
        """Test that TileDB attributes are correctly set."""
        with tiledb.open(
            self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx
        ) as array:
            schema = array.schema

            # Check for expected attributes
            expected_attributes = [
                "shot_number",
                "beam_type",
                "degrade_flag",
            ]  # Example attributes in the array

            for attr in expected_attributes:
                self.assertIn(
                    attr, schema.attr(attr).name, f"Missing attribute: {attr}"
                )

    def test_overwrite_behavior(self):
        """Ensure overwrite behavior works correctly."""
        self.assertTrue(
            self.config["tiledb"]["overwrite"],
            "Overwrite setting should be True",
        )

        # Check if array exists after creation
        self.assertTrue(
            tiledb.array_exists(self.gedi_db.array_uri),
            "TileDB array should exist after creation",
        )

        # Re-create the array and confirm it overwrites
        self.gedi_db._create_arrays()  # Overwrite
        self.assertTrue(
            tiledb.array_exists(self.gedi_db.array_uri),
            "TileDB array should still exist after overwrite",
        )

    def test_write_granule(self):
        """Test the `write_granule` function to write data to TileDB with row batching."""
        granule_file = self.data_dir / "example_data.csv"
        if not granule_file.exists():
            raise FileNotFoundError(f"Granule file not found: {granule_file}")

        granule_data = pd.read_csv(granule_file)
        granule_data["time"] = pd.to_datetime(
            granule_data["time"], utc=True, errors="coerce"
        )

        # Exercise batching explicitly (small batches)
        self.gedi_db.write_granule(granule_data, row_batch=2)

        expected_shot = np.array(
            [
                84480000200057734,
                84480000200057402,
                84480000200057755,
                84480000200057754,
                84480000200057753,
            ],
            dtype=np.int64,
        )

        expected_type = np.array(
            [
                "coverage",
                "coverage",
                "coverage",
                "coverage",
                "coverage",
            ],
            dtype=object,
        )

        expected_name = np.array(
            [
                "/BEAM0000",
                "/BEAM0000",
                "/BEAM0000",
                "/BEAM0000",
                "/BEAM0000",
            ],
            dtype=object,
        )

        with tiledb.open(self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx) as A:
            # Prefer pandas DataFrame API if available; it’s simpler and order-agnostic
            try:
                df = A.query(
                    attrs=("shot_number", "beam_type", "beam_name"), dims=True
                ).df[:]
                # Ensure Python types (TileDB may return object dtype already, but be robust)
                df = df.astype({"shot_number": "int64"})
                df = df.sort_values("shot_number").reset_index(drop=True)

                got_shot = df["shot_number"].to_numpy()
                got_type = df["beam_type"].astype(object).to_numpy()
                got_name = df["beam_name"].astype(object).to_numpy()

            except Exception:
                # Fallback to multi_index for older TileDB-Py
                q = A.query(attrs=("shot_number", "beam_type", "beam_name"))
                res = q.multi_index[:, :, :]  # full domain
                # Build a small DataFrame for deterministic comparison
                df = (
                    pd.DataFrame(
                        {
                            "shot_number": np.asarray(
                                res["shot_number"], dtype=np.int64
                            ),
                            "beam_type": res["beam_type"].astype(object),
                            "beam_name": res["beam_name"].astype(object),
                        }
                    )
                    .sort_values("shot_number")
                    .reset_index(drop=True)
                )

                got_shot = df["shot_number"].to_numpy()
                got_type = df["beam_type"].to_numpy()
                got_name = df["beam_name"].to_numpy()

        # Sort expected the same way for a fair comparison
        order = np.argsort(expected_shot)
        expected_shot_sorted = expected_shot[order]
        expected_type_sorted = expected_type[order]
        expected_name_sorted = expected_name[order]

        # Assertions (clear messages on mismatch)
        self.assertTrue(
            np.array_equal(got_shot, expected_shot_sorted),
            f"Shot number mismatch.\nExpected: {expected_shot_sorted}\nGot: {got_shot}",
        )
        self.assertTrue(
            np.array_equal(got_type, expected_type_sorted),
            f"Beam type mismatch.\nExpected: {expected_type_sorted}\nGot: {got_type}",
        )
        self.assertTrue(
            np.array_equal(got_name, expected_name_sorted),
            f"Beam name mismatch.\nExpected: {expected_name_sorted}\nGot: {got_name}",
        )


suite = unittest.TestLoader().loadTestsFromTestCase(TestGEDIDatabase)
