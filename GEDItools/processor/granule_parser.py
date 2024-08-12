import geopandas as gpd
import pandas as pd
from GEDItools.utils.constants import WGS84, GediProduct
from GEDItools.processor.granule.granule import Granule
from GEDItools.processor.granule.l1b_granule import L1BGranule
from GEDItools.processor.granule.l2a_granule import L2AGranule
from GEDItools.processor.granule.l2b_granule import L2BGranule
from GEDItools.processor.granule.l4a_granule import L4AGranule
from GEDItools.processor.granule.l4c_granule import L4CGranule

class GranuleParser:
    def __init__(self, file: str, quality_filter: dict = None, field_mapping: dict = None):
        self.file = file
        self.quality_filter = quality_filter
        self.field_mapping = field_mapping

    def parse_granule(self, granule: Granule) -> gpd.GeoDataFrame:
        granule_data = []
        print(f'Parsing {granule.short_name}')
        for beam in granule.iter_beams():
            print(f'Parsing beam {beam.name}')
            #beam.quality_filter(self.quality_filter)
            beam.sql_format_arrays()
            granule_data.append(beam.main_data)
            print(f'Finished parsing beam {beam.name}')
        df = pd.concat(granule_data, ignore_index=True)
        gdf = gpd.GeoDataFrame(df, crs=WGS84)
        granule.close()
        print(f'Finished parsing {granule.short_name}')
        return gdf

    def parse(self) -> gpd.GeoDataFrame:
        raise NotImplementedError("This method should be implemented in child classes")

class L1BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L1BGranule(self.file, self.quality_filter['level_1b'], self.field_mapping['level_1b'])
        return self.parse_granule(granule)

class L2AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2AGranule(self.file, self.quality_filter['level_2a'], self.field_mapping['level_2a'])
        return self.parse_granule(granule)

class L2BGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L2BGranule(self.file, self.quality_filter['level_2b'], self.field_mapping['level_2b'])
        return self.parse_granule(granule)

class L4AGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4AGranule(self.file, self.quality_filter['level_4a'], self.field_mapping['level_4a'])
        return self.parse_granule(granule)

class L4CGranuleParser(GranuleParser):
    def parse(self) -> gpd.GeoDataFrame:
        granule = L4CGranule(self.file, self.quality_filter['level_4c'], self.field_mapping['level_4c'])
        return self.parse_granule(granule)

def parse_h5_file(file: str, product: GediProduct, quality_filter: dict = None, field_mapping: dict = None) -> gpd.GeoDataFrame:
    parser_classes = {
        GediProduct.L1B.value: L1BGranuleParser,
        GediProduct.L2A.value: L2AGranuleParser,
        GediProduct.L2B.value: L2BGranuleParser,
        GediProduct.L4A.value: L4AGranuleParser,
        GediProduct.L4C.value: L4CGranuleParser,
    }

    parser_class = parser_classes.get(product)
    if parser_class is None:
        raise ValueError(f"Product {product} not supported")
    
    parser = parser_class(file, quality_filter, field_mapping)
    return parser.parse()
