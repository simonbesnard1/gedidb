import geopandas as gpd
import pandas as pd
from gedidb.utils.constants import WGS84, GediProduct
from gedidb.processor.granule.granule import Granule
from gedidb.processor.granule.l1b_granule import L1BGranule
from gedidb.processor.granule.l2a_granule import L2AGranule
from gedidb.processor.granule.l2b_granule import L2BGranule
from gedidb.processor.granule.l4a_granule import L4AGranule
from gedidb.processor.granule.l4c_granule import L4CGranule

class GranuleParser:
    def __init__(self, file: str, data_info: dict = None,  geom: gpd.GeoSeries=None):
        self.file = file
        self.data_info = data_info
        
    def parse_granule(self, granule: Granule) -> gpd.GeoDataFrame:
        granule_data = []
        for beam in granule.iter_beams():
            beam.sql_format_arrays()
            main_data = beam.main_data
            
            if main_data is not None:
                granule_data.append(main_data)
    
        if granule_data:
            df = pd.concat(granule_data, ignore_index=True)
            gdf = gpd.GeoDataFrame(df, crs=WGS84)
            
            return gdf

    def parse(self) -> gpd.GeoDataFrame:
        raise NotImplementedError("This method should be implemented in child classes")

class L1BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L1BGranule(self.file, self.data_info['level_1b']['quality_filter'], 
                             self.data_info['level_1b']['variables'])
        return self.parse_granule(granule)

class L2AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2AGranule(self.file, self.data_info['level_2a']['quality_filter'], 
                             self.data_info['level_2a']['variables'])
        return self.parse_granule(granule)

class L2BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2BGranule(self.file, self.data_info['level_2b']['quality_filter'], 
                             self.data_info['level_2b']['variables'])
        return self.parse_granule(granule)

class L4AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4AGranule(self.file, self.data_info['level_4a']['quality_filter'], 
                             self.data_info['level_4a']['variables'])
        return self.parse_granule(granule)

class L4CGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4CGranule(self.file, self.data_info['level_4c']['quality_filter'], 
                             self.data_info['level_4c']['variables'])
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
