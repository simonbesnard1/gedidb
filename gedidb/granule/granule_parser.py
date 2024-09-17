import geopandas as gpd
import pandas as pd
import re
from typing import Optional

from gedidb.utils.constants import WGS84, GediProduct
from gedidb.granule.granule.granule import Granule
from gedidb.granule.granule.l2a_granule import L2AGranule
from gedidb.granule.granule.l2b_granule import L2BGranule
from gedidb.granule.granule.l4a_granule import L4AGranule
from gedidb.granule.granule.l4c_granule import L4CGranule


class GranuleParser:
    """
    Base class for parsing GEDI granule data into a GeoDataFrame.
    This class provides the common logic for different GEDI product types.
    """
    
    def __init__(self, file: str, data_info: Optional[dict] = None, geom: Optional[gpd.GeoSeries] = None):
        """
        Initialize the GranuleParser.

        Args:
            file (str): Path to the granule file.
            data_info (dict, optional): Dictionary containing relevant information about the data structure.
            geom (gpd.GeoSeries, optional): Geometry for spatial data filtering.
        """
        self.file = file
        self.data_info = data_info

    @staticmethod
    def parse_granule(granule: Granule) -> gpd.GeoDataFrame:
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
            df = pd.concat(granule_data, ignore_index=True)
            df['version'] = re.search(r'V\d{3}', granule.version_granule).group()  # Extract version
            gdf = gpd.GeoDataFrame(df, crs=WGS84)
            return gdf
        else:
            return gpd.GeoDataFrame()  # Return empty GeoDataFrame if no data is found

    def parse(self) -> gpd.GeoDataFrame:
        """
        Abstract method to be implemented by child classes for parsing specific granules.

        Raises:
            NotImplementedError: Child classes must implement this method.
        """
        raise NotImplementedError("This method should be implemented in child classes")

class L2AGranuleParser(GranuleParser):
    """Parser for L2A granules."""
    
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2AGranule(self.file, self.data_info['level_2a']['variables'])
        return self.parse_granule(granule)


class L2BGranuleParser(GranuleParser):
    """Parser for L2B granules."""
    
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2BGranule(self.file, self.data_info['level_2b']['variables'])
        return self.parse_granule(granule)


class L4AGranuleParser(GranuleParser):
    """Parser for L4A granules."""
    
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4AGranule(self.file, self.data_info['level_4a']['variables'])
        return self.parse_granule(granule)


class L4CGranuleParser(GranuleParser):
    """Parser for L4C granules."""
    
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4CGranule(self.file, self.data_info['level_4c']['variables'])
        return self.parse_granule(granule)


def parse_h5_file(file: str, product: GediProduct, data_info: Optional[dict] = None) -> gpd.GeoDataFrame:
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
    
    parser = parser_class(file, data_info)
    return parser.parse()
