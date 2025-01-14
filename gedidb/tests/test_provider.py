import os
import tempfile
import unittest

import pandas as pd
import yaml
from pandas import DataFrame
from xarray import Dataset

from gedidb import GEDIDatabase
from gedidb import GEDIProvider


class TestGEDIProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.yaml_file_path = "data/data_config.yml"
        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.config["tiledb"]["local_path"] = cls.temp_dir.name

        cls.gedi_db = GEDIDatabase(cls.config)
        cls.gedi_db._create_arrays()  # Create the TileDB array for testing
        # write test data
        granule_data = pd.read_csv("data/example_data.csv")
        cls.gedi_db.write_granule(granule_data)

        cls.gedi_provider = GEDIProvider(
            storage_type="local", local_path=cls.temp_dir.name
        )

    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()

    def test_get_data_minimal_input(self):
        """Test get_data with minimal required arguments."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        result = self.gedi_provider.get_data(variables)
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    # def test_get_data_with_geometry(self):
    #     """Test get_data with a geometry argument."""
    #     variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
    #     geometry = gpd.read_file("data/test.geojson")
    #     result = self.gedi_provider.get_data(variables, geometry=geometry)
    #     self.assertIsNotNone(result, "Result should not be None")
    #     self.assertTrue(isinstance(result, Dataset), "Result should be an xarray Dataset")

    def test_get_data_with_time_range(self):
        """Test get_data with start and end time arguments."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        start_time = "2020-06-09"
        end_time = "2020-06-09"
        result = self.gedi_provider.get_data(
            variables, start_time=start_time, end_time=end_time
        )
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    def test_get_data_with_point_query(self):
        """Test get_data with point and radius for query."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        point = (9.43074284703215, 6.33762697689783)
        radius = 1.0
        result = self.gedi_provider.get_data(variables, point=point, radius=radius)
        self.assertIsNotNone(result, "Result should not be None")
        self.assertTrue(
            isinstance(result, Dataset), "Result should be an xarray Dataset"
        )

    def test_get_data_with_quality_filters(self):
        """Test get_data with quality filters."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]

        unfiltered_result = self.gedi_provider.get_data(variables)
        self.assertIsNotNone(unfiltered_result, "Unfiltered result should not be None")
        self.assertTrue(
            isinstance(unfiltered_result, Dataset),
            "Unfiltered result should be an xarray Dataset",
        )

        quality_filters = {"wsci_z_pi_lower": "> 4.0", "wsci_z_pi_upper": "> 6.1"}

        filtered_result = self.gedi_provider.get_data(variables, **quality_filters)
        self.assertIsNotNone(filtered_result, "Filtered result should not be None")
        self.assertTrue(
            isinstance(filtered_result, Dataset),
            "Filtered result should be an xarray Dataset",
        )

        self.assertTrue(
            len(filtered_result[variables[0]]) < len(unfiltered_result[variables[0]]),
            "Filtered data should have fewer entries than unfiltered data",
        )
        self.assertTrue(
            len(filtered_result[variables[1]]) < len(unfiltered_result[variables[1]]),
            "Filtered data should have fewer entries than unfiltered data",
        )

    def test_get_data_with_different_return_types(self):
        """Test get_data with different return types."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]

        # Test xarray return type
        result_xarray = self.gedi_provider.get_data(variables, return_type="xarray")
        self.assertTrue(
            isinstance(result_xarray, Dataset), "Result should be an xarray Dataset"
        )

        # Test pandas DataFrame return type
        result_df = self.gedi_provider.get_data(variables, return_type="dataframe")
        self.assertTrue(
            isinstance(result_df, DataFrame), "Result should be a pandas DataFrame"
        )

    def test_get_data_invalid_query_type(self):
        """Test get_data with an invalid query type."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        query_type = "invalid_query"
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(variables, query_type=query_type)

    def test_get_data_invalid_return_type(self):
        """Test get_data with an invalid query type."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        return_type = "invalid_return"
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(variables, return_type=return_type)
