import os
import tempfile
import unittest

import pandas as pd
import yaml

from gedidb import GEDIDatabase
from gedidb import GEDIProvider


class TestGEDIProvider(unittest.TestCase):

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
        # write test data
        granule_data = pd.read_csv('data/example_data.csv')
        cls.gedi_db.write_granule(granule_data)

        cls.gedi_provider = GEDIProvider(storage_type='local', local_path=cls.temp_dir.name)


    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()
