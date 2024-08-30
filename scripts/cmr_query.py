import h5py

from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct
from datetime import datetime

import geopandas as gpd

#%% Test run of CMR query

cmr_query = GranuleQuery(product=GediProduct.L2A, geom=gpd.read_file("../testing/data/geojson/test.geojson"), start_date=datetime(2022, 1, 1), end_date=datetime(2022, 1, 31))

print(cmr_query.query_granules()['version'])
