import requests
import geopandas as gpd
from datetime import datetime
import pandas as pd
from GEDItools.utils.constants import GediProduct
from functools import wraps

CMR_URL= "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_PRODUCT_IDS = {
    "GediProduct.L1B": 'C1908344278-LPDAAC_ECS',
    "GediProduct.L2A": 'C1908348134-LPDAAC_ECS',
    "GediProduct.L2B": 'C1908350066-LPDAAC_ECS',
    "GediProduct.L4A": 'C2237824918-ORNL_CLOUD',
    "GediProduct.L4C": 'C3049900163-ORNL_CLOUD',

}

# Decorator for logging
def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Executing {func.__name__}...")
        result = func(*args, **kwargs)
        print(f"Finished {func.__name__}.")
        return result
    return wrapper

# Decorator for handling exceptions
def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"Error occurred in {func.__name__}: {e}")
            # Additional error handling logic can be placed here
    return wrapper

class CMRQuery:
    
    @staticmethod
    #@log_execution
    @handle_exceptions
    def _construct_query_params(product: GediProduct, geom: gpd.GeoSeries, start_date: datetime, end_date: datetime, page_size: int, page_num: int) -> dict:
        
        return {
            "collection_concept_id": CMR_PRODUCT_IDS[str(product)],
            "page_size": page_size,
            "page_num": page_num,
            "bounding_box": CMRQuery._construct_spacial_params(geom),
            "temporal": CMRQuery._construct_temporal_params(start_date, end_date)
        }

    @staticmethod
    #@log_execution
    @handle_exceptions
    def _construct_temporal_params(start_date: datetime, end_date: datetime) -> str:
        if start_date and end_date:
            if start_date > end_date:
                raise ValueError("Start date must be before end date")
            return f'{start_date.strftime("%Y-%m-%dT00:00:00Z")}/{end_date.strftime("%Y-%m-%dT23:59:59Z")}'
        if start_date:
            return f'{start_date.strftime("%Y-%m-%dT00:00:00Z")}/'
        if end_date:
            return f'/{end_date.strftime("%Y-%m-%dT23:59:59Z")}'
        else:
            return ''

    @staticmethod
    #@log_execution
    @handle_exceptions
    def _construct_spacial_params(geom: gpd.GeoSeries) -> str:
        return ','.join([str(x) for x in geom.total_bounds])

    @staticmethod
    def _get_id(item):
        if "LPDAAC" in item["data_center"]:
            _id = item['producer_granule_id'].split('_')
            return f"{_id[3]}_{_id[4]}"
        if "ORNL" in item["data_center"]:
            if item['collection_concept_id'] == 'C2237824918-ORNL_CLOUD':
                _id = item['title'].split('_')
                return f"{_id[8]}_{_id[9]}"
            if item['collection_concept_id'] == 'C3049900163-ORNL_CLOUD':
                _id = item['title'].split('_')
                return f"{_id[5]}_{_id[6]}"
        else:
            raise ValueError("Data center not recognized")

    @staticmethod
    def _get_name(item):
        if "LPDAAC" in item["data_center"]:
            return item["producer_granule_id"]
        if "ORNL" in item["data_center"]:
            return item["title"].split(".", maxsplit=1)[1]
        return None

class GranuleQuery(CMRQuery):
    def __init__(self, product: GediProduct, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None):
        self.product = product
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date

    #@log_execution
    @handle_exceptions
    def query_granules(self, page_size: int = 2000, page_num: int = 1) -> pd.DataFrame:
        cmr_params = self._construct_query_params(self.product, self.geom, self.start_date, self.end_date, page_size, page_num)
        granule_data = []
        
        while True:
            cmr_params["page_num"] = page_num
            response = requests.get(CMR_URL, params=cmr_params)
            response.raise_for_status()
            cmr_response = response.json()["feed"]["entry"]

            if cmr_response:
                granule_data += cmr_response
                page_num += 1
            else:
                break

        granule_data = [{
            'id': self._get_id(item),
            'name': self._get_name(item),
            'url': item['links'][0]['href'],
            'size': float(item['granule_size']),
            'product': self.product.value
        } for item in granule_data]

        df_granule = pd.DataFrame(
            granule_data,
            columns=['id', 'name', 'url', 'size', 'product']
        )

        print(f"Total {self.product.value} granules found:", len(df_granule))
        print("Total file size (MB): ", '{0:,.2f}'.format(df_granule['size'].sum()))

        return df_granule
