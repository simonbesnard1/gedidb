import unittest

import numpy as np
import tiledb
import pandas as pd
import os
import yaml
import tempfile
from gedidb.core.gedidatabase import GEDIDatabase


class TestGEDIDatabase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.yaml_file_path = '../../config_files/data_config.yml'
        with open(cls.yaml_file_path, 'r') as file:
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
        with tiledb.open(self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx) as array:
            schema = array.schema
            dims = schema.domain

            # Check dimensions
            lat_dim = dims.dim("latitude")
            lon_dim = dims.dim("longitude")
            time_dim = dims.dim("time")

            self.assertIn("latitude", lat_dim.name, "The 'latitude' dimension is missing from the TileDB schema.")
            self.assertIn("longitude", lon_dim.name, "The 'longitude' dimension is missing from the TileDB schema.")
            self.assertIn("time", time_dim.name, "The 'time' dimension is missing from the TileDB schema.")

            self.assertEqual(lat_dim.domain, (-56.0, 56.0), "Latitude range mismatch")
            self.assertEqual(lon_dim.domain, (-180.0, 180.0), "Longitude range mismatch")
            # Check chunk size
            self.assertEqual(lat_dim.tile, 1.0, "Latitude chunk size mismatch")
            self.assertEqual(lon_dim.tile, 1.0, "Longitude chunk size mismatch")

    def test_tiledb_attributes(self):
        """Test that TileDB attributes are correctly set."""
        with tiledb.open(self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx) as array:
            schema = array.schema

            # Check for expected attributes
            expected_attributes = ["shot_number", "beam_type", "degrade_flag"]  # Example attributes in the array

            for attr in expected_attributes:
                self.assertIn(attr, schema.attr(attr).name, f"Missing attribute: {attr}")

    def test_overwrite_behavior(self):
        """Ensure overwrite behavior works correctly."""
        self.assertTrue(
            self.config["tiledb"]["overwrite"],
            "Overwrite setting should be True"
        )

        # Check if array exists after creation
        self.assertTrue(
            tiledb.array_exists(self.gedi_db.array_uri),
            "TileDB array should exist after creation"
        )

        # Re-create the array and confirm it overwrites
        self.gedi_db._create_arrays()  # Overwrite
        self.assertTrue(
            tiledb.array_exists(self.gedi_db.array_uri),
            "TileDB array should still exist after overwrite"
        )

    def test_write_granule(self):
        """Test the `write_granule` function to write data to TileDB."""

        granule_data = pd.read_csv('data/example_data.csv')
        self.gedi_db.write_granule(granule_data)

        with tiledb.open(self.gedi_db.array_uri, mode="r", ctx=self.gedi_db.ctx) as array:
            shot_number = array.query(attrs=('shot_number',)).multi_index[:, :, :]
            beam_type = array.query(attrs=('beam_type',)).multi_index[:, :, :]
            beam_name = array.query(attrs=('beam_name',)).multi_index[:, :, :]

            self.assertTrue(np.array_equal(shot_number['shot_number'],
                [84480000200057402, 84480000200057734, 84480000200057753, 84480000200057754, 84480000200057755]),
                "Shot number mismatch")
            self.assertTrue(np.array_equal(beam_type['beam_type'],
                ['coverage', 'coverage', 'coverage', 'coverage', 'coverage']),
                "Beam type mismatch")
            self.assertTrue(np.array_equal(beam_name['beam_name'],
                ['/BEAM0000', '/BEAM0000', '/BEAM0000', '/BEAM0000', '/BEAM0000']),
                "Beam name mismatch")


suite = unittest.TestLoader().loadTestsFromTestCase(TestGEDIDatabase)
