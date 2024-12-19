# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import pandas as pd
from typing import Optional, Dict
from pathlib import Path

from gedidb.utils.constants import GediProduct
from gedidb.granule.granule.granule import Granule
from gedidb.granule.granule.l2a_granule import L2AGranule
from gedidb.granule.granule.l2b_granule import L2BGranule
from gedidb.granule.granule.l4a_granule import L4AGranule
from gedidb.granule.granule.l4c_granule import L4CGranule


class GranuleParser:
    """
    Base class for parsing GEDI granule data into a GeoDataFrame.
    Provides common parsing logic for different GEDI product types.
    """

    def __init__(self, file: str, data_info: Optional[dict] = None):
        """
        Initialize the GranuleParser.

        Args:
            file (str): Path to the granule file.
            data_info (dict, optional): Dictionary containing relevant data structure information.
        """
        self.file = Path(file)
        if not self.file.exists():
            raise FileNotFoundError(f"Granule file {self.file} not found.")
        self.data_info = data_info if data_info else {}

    @staticmethod
    def parse_granule(granule: Granule) -> pd.DataFrame:
        """
        Parse a single granule and return a GeoDataFrame.

        Args:
            granule (Granule): The granule object to be parsed.

        Returns:
            gpd.GeoDataFrame: A GeoDataFrame containing the parsed granule data.
        """
        granule_data = []
        for beam in granule.iter_beams():
            main_data = beam.main_data
            if main_data is not None:
                granule_data.append(main_data)

        if granule_data:
            try:
                df = pd.concat(granule_data, ignore_index=True)
                return df
            except Exception as e:
                raise ValueError(f"Error parsing granule data: {e}")

        return pd.DataFrame()  # Return empty GeoDataFrame if no data found

    def parse(self) -> pd.DataFrame:
        """
        Abstract method to be implemented by child classes for parsing specific granules.

        Raises:
            NotImplementedError: Child classes must implement this method.
        """
        raise NotImplementedError("This method should be implemented in child classes")

class L2AGranuleParser(GranuleParser):
    """Parser for L2A granules."""

    def parse(self) -> pd.DataFrame:
        granule = L2AGranule(self.file, self.data_info.get('level_2a', {}).get('variables', []))
        return self.parse_granule(granule)


class L2BGranuleParser(GranuleParser):
    """Parser for L2B granules."""

    def parse(self) -> pd.DataFrame:
        granule = L2BGranule(self.file, self.data_info.get('level_2b', {}).get('variables', []))
        return self.parse_granule(granule)


class L4AGranuleParser(GranuleParser):
    """Parser for L4A granules."""

    def parse(self) -> pd.DataFrame:
        granule = L4AGranule(self.file, self.data_info.get('level_4a', {}).get('variables', []))
        return self.parse_granule(granule)


class L4CGranuleParser(GranuleParser):
    """Parser for L4C granules."""

    def parse(self) -> pd.DataFrame:
        granule = L4CGranule(self.file, self.data_info.get('level_4c', {}).get('variables', []))
        return self.parse_granule(granule)

def parse_h5_file(file: str, product: GediProduct, data_info: Optional[Dict]= None) -> pd.DataFrame:
    """
    Parse an HDF5 file based on the product type and return a GeoDataFrame.

    Args:
        file (str): Path to the HDF5 file.
        product (GediProduct): Type of GEDI product (L2A, L2B, L4A, L4C).
        data_info (dict, optional): Information about the data structure.

    Returns:
        gpd.GeoDataFrame: Parsed GeoDataFrame containing the granule data.

    Raises:
        ValueError: If the provided product is not supported.
    """
    parser_classes = {
        GediProduct.L2A.value: L2AGranuleParser,
        GediProduct.L2B.value: L2BGranuleParser,
        GediProduct.L4A.value: L4AGranuleParser,
        GediProduct.L4C.value: L4CGranuleParser,
    }

    parser_class = parser_classes.get(product)
    if parser_class is None:
        raise ValueError(f"Product {product.value} is not supported.")

    parser = parser_class(file, data_info or {})
    return parser.parse()
