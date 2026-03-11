# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import os
import tempfile
import unittest

import geopandas as gpd
import pandas as pd
import yaml
from pandas import DataFrame
from xarray import Dataset

from gedidb import GEDIDatabase, GEDIProvider


class TestGEDIProvider(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.chdir(os.path.dirname(__file__))
        cls.yaml_file_path = "data/data_config.yml"
        cls.geometry = gpd.read_file("data/bounding_box.geojson")

        with open(cls.yaml_file_path, "r") as file:
            cls.config = yaml.safe_load(file)

        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.config["tiledb"]["local_path"] = cls.temp_dir.name

        cls.gedi_db = GEDIDatabase(cls.config)
        cls.gedi_db._create_arrays()

        granule_data = pd.read_csv("data/example_data.csv")
        cls.gedi_db.write_granule(granule_data)

        cls.gedi_provider = GEDIProvider(
            storage_type="local", local_path=cls.temp_dir.name
        )

    @classmethod
    def tearDownClass(cls):
        """Cleanup temporary directory."""
        cls.temp_dir.cleanup()

    def test_get_data_with_geometry(self):
        """Test get_data with a geometry argument."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        result = self.gedi_provider.get_data(variables, geometry=self.geometry)
        self.assertIsNotNone(result, "Result should not be None")
        self.assertIsInstance(result, Dataset, "Result should be an xarray Dataset")

    def test_get_data_with_time_range(self):
        """Test get_data with start and end time arguments."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        result = self.gedi_provider.get_data(
            variables,
            geometry=self.geometry,
            start_time="2020-06-09",
            end_time="2020-06-09",
        )
        self.assertIsNotNone(result, "Result should not be None")
        self.assertIsInstance(result, Dataset, "Result should be an xarray Dataset")

    def test_get_data_with_point_query(self):
        """Test get_data with point and radius for query."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]
        result = self.gedi_provider.get_data(
            variables,
            query_type="nearest",
            point=(9.43074284703215, 6.33762697689783),
            radius=1.0,
            num_shots=2,
        )
        self.assertIsNotNone(result, "Result should not be None")
        self.assertIsInstance(result, Dataset, "Result should be an xarray Dataset")

    def test_get_data_with_quality_filters(self):
        """Test get_data with quality filters applied and verify they reduce results."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]

        unfiltered = self.gedi_provider.get_data(variables, geometry=self.geometry)
        self.assertIsNotNone(unfiltered)
        self.assertIsInstance(unfiltered, Dataset)

        filtered = self.gedi_provider.get_data(
            variables,
            geometry=self.geometry,
            **{"wsci_z_pi_lower": "> 4.0", "wsci_z_pi_upper": "> 6.1"},
        )
        self.assertIsNotNone(filtered)
        self.assertIsInstance(filtered, Dataset)

        for var in variables:
            self.assertLess(
                len(filtered[var]),
                len(unfiltered[var]),
                f"Filtered {var} should have fewer entries than unfiltered",
            )

    def test_get_data_with_different_return_types(self):
        """Test get_data returns correct types for xarray and dataframe."""
        variables = ["wsci_z_pi_lower", "wsci_z_pi_upper"]

        result_xarray = self.gedi_provider.get_data(
            variables, geometry=self.geometry, return_type="xarray"
        )
        self.assertIsInstance(result_xarray, Dataset)

        result_df = self.gedi_provider.get_data(
            variables, geometry=self.geometry, return_type="dataframe"
        )
        self.assertIsInstance(result_df, DataFrame)

    def test_get_data_invalid_query_type(self):
        """Test get_data raises ValueError for an unrecognised query type."""
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(
                ["wsci_z_pi_lower", "wsci_z_pi_upper"],
                geometry=self.geometry,
                query_type="invalid_query",
            )

    def test_get_data_invalid_return_type(self):
        """Test get_data raises ValueError for an unrecognised return type."""
        with self.assertRaises(ValueError):
            self.gedi_provider.get_data(
                ["wsci_z_pi_lower", "wsci_z_pi_upper"],
                geometry=self.geometry,
                return_type="invalid_return",
            )
