import geopandas as gpd
import pandas as pd

from geditoolbox.utils.constants import WGS84
from geditoolbox.processor.granule.granule import Granule
from geditoolbox.utils.constants import GediProduct
from geditoolbox.processor.granule.l2a_granule import L2AGranule
from geditoolbox.processor.granule.l2b_granule import L2BGranule
from geditoolbox.processor.granule.l4a_granule import L4AGranule
from geditoolbox.processor.granule.l4c_granule import L4CGranule


def parse_h5_file(file: str, product: GediProduct, quality_filter=True):
    if product == GediProduct.L2A.value:
        return parse_file_l2a(file, quality_filter)
    elif product == GediProduct.L2B.value:
        return parse_file_l2b(file, quality_filter)
    elif product == GediProduct.L4A.value:
        return parse_file_l4a(file, quality_filter)
    elif product == GediProduct.L4C.value:
        return parse_file_l4c(file, quality_filter)
    else:
        raise ValueError(f"Product {product} not supported")


def parse_file_l2a(file, quality_filter):
    granule = L2AGranule(file)
    return parse_granule(granule, quality_filter)


def parse_file_l2b(file, quality_filter):
    granule = L2BGranule(file)
    return parse_granule(granule, quality_filter)


def parse_file_l4a(file, quality_filter):
    granule = L4AGranule(file)
    return parse_granule(granule, quality_filter)


def parse_file_l4c(file, quality_filter):
    granule = L4CGranule(file)
    return parse_granule(granule, quality_filter)


def parse_granule(granule: Granule, quality_filter=True) -> gpd.GeoDataFrame:
    granule_data = []
    print(f'Parsing {granule.short_name}')
    for beam in granule.iter_beams():
        print(f'Parsing beam {beam.name}')
        if quality_filter:
            beam.quality_filter()
        beam.sql_format_arrays()
        granule_data.append(beam.main_data)
        print(f'Finished parsing beam {beam.name}')
    df = pd.concat(granule_data, ignore_index=True)
    gdf = gpd.GeoDataFrame(df, crs=WGS84)
    # filter_status = "_unfiltered" if not quality_filter else ""
    # gdf.to_csv(f'./debug/csv/{granule.short_name}{filter_status}.csv')
    granule.close()
    print(f'Finished parsing {granule.short_name}')
    return gdf
