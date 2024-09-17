import geopandas as gpd
import pandas as pd
import re

from gedidb.utils.constants import WGS84, GediProduct
from gedidb.granule.granule.granule import Granule
from gedidb.granule.granule.l1b_granule import L1BGranule
from gedidb.granule.granule.l2a_granule import L2AGranule
from gedidb.granule.granule.l2b_granule import L2BGranule
from gedidb.granule.granule.l4a_granule import L4AGranule
from gedidb.granule.granule.l4c_granule import L4CGranule

class GranuleParser:
    def __init__(self, file: str, data_info: dict = None,  geom: gpd.GeoSeries=None):
        self.file = file
        self.data_info = data_info
        
    @staticmethod
    def parse_granule(granule: Granule) -> gpd.GeoDataFrame:
        granule_data = []
        for beam in granule.iter_beams():
            main_data = beam.main_data

            if main_data is not None:
                granule_data.append(main_data)
    
        if granule_data:
            df = pd.concat(granule_data, ignore_index=True)

            # We're getting the version of each product from the 'fileName' of the metadata
            # It seems like product L4C somehow uses V001 and is also defined as GEDI_WSCI
            # We do have a consistent version number in our database because when joining the different
            # products together, version L2A has priority. So the product version being used in the database
            # is always L2A, even though it might differ between different products.
            df['version'] = re.search(r'V\d{3}', granule.version_granule).group()
            # print(f"Granule: {granule.product} Version: {granule.version_granule}" )

            gdf = gpd.GeoDataFrame(df, crs=WGS84)
            
            return gdf

    def parse(self) -> gpd.GeoDataFrame:
        raise NotImplementedError("This method should be implemented in child classes")

class L1BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L1BGranule(self.file, self.data_info['level_1b']['variables'])
        return self.parse_granule(granule)

class L2AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2AGranule(self.file, self.data_info['level_2a']['variables'])
        return self.parse_granule(granule)

class L2BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2BGranule(self.file, self.data_info['level_2b']['variables'])
        return self.parse_granule(granule)

class L4AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4AGranule(self.file, self.data_info['level_4a']['variables'])
        return self.parse_granule(granule)

class L4CGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4CGranule(self.file, self.data_info['level_4c']['variables'])
        return self.parse_granule(granule)

def parse_h5_file(file: str, product: GediProduct, data_info: dict = None) -> gpd.GeoDataFrame:
    parser_classes = {
        GediProduct.L2A.value: L2AGranuleParser,
        GediProduct.L2B.value: L2BGranuleParser,
        GediProduct.L4A.value: L4AGranuleParser,
        GediProduct.L4C.value: L4CGranuleParser,
    }

    parser_class = parser_classes.get(product)
    if parser_class is None:
        raise ValueError(f"Product {product} not supported")
    
    parser = parser_class(file, data_info)
    return parser.parse()
