import geopandas as gpd
import pandas as pd

from constants import WGS84
from granule_data.granule.granule import Granule


def _parse_granule(granule: Granule, quality_filter=True) -> gpd.GeoDataFrame:
    granule_data = []
    print(f'Parsing {granule.short_name}')
    for beam in granule.iter_beams():
        if quality_filter:
            beam.quality_filter()
        beam.sql_format_arrays()
        granule_data.append(beam.main_data)
    df = pd.concat(granule_data, ignore_index=True)
    gdf = gpd.GeoDataFrame(df, crs=WGS84)
    # filter_status = "_unfiltered" if not quality_filter else ""
    # gdf.to_csv(f'./debug/csv/{granule.short_name}{filter_status}.csv')
    granule.close()
    print(f'Finished parsing {granule.short_name}')
    return gdf
