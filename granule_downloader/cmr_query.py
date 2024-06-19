import enum
import requests
import geopandas as gpd
from datetime import datetime
import pandas as pd


from requests import HTTPError

from constants import GediProduct

CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_PRODUCT_IDS = {
    GediProduct.L1B: 'C1908344278-LPDAAC_ECS',
    GediProduct.L2A: 'C1908348134-LPDAAC_ECS',
    GediProduct.L2B: 'C1908350066-LPDAAC_ECS',
    # GediProduct.L3: 'C2153683336-ORNL_CLOUD',
    GediProduct.L4A: 'C2237824918-ORNL_CLOUD',
}


def granule_query(
        product: GediProduct,
        geom: str,
        start_date: datetime = None,
        end_date: datetime = None,
        out_dir: str = None,
        page_size: int = 2000,
        page_num: int = 1
) -> pd.DataFrame:

    cmr_params = _construct_query_params(product, geom, (start_date, end_date), page_size, 1)

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
        'id': _get_id(item),
        'url': item['links'][0]['href'],
        'size': float(item['granule_size']),
        'product': product.value
    } for item in granule_data]

    df_granule = pd.DataFrame(
        granule_data
    )

    # print(f"Total {product.value} granules found:", len(df_granule))
    # print("Total file size (MB): ", '{0:,.2f}'.format(df_granule['size'].sum()))

    return df_granule


def _construct_query_params(
        product: GediProduct,
        geom: str,
        date_range: (datetime, datetime),
        page_size: int,
        page_num: int,
) -> dict:

    cmr_params = {
        "collection_concept_id": CMR_PRODUCT_IDS[product],
        "page_size": page_size,
        "page_num": page_num,
        "bounding_box": _construct_spacial_params(geom),
        "temporal": _construct_temporal_params(date_range)
    }

    return cmr_params


def _construct_temporal_params(
        date_range: (datetime, datetime)
) -> str:

    start_date, end_date = date_range
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


def _construct_spacial_params(
        geom: str
) -> str:

    # TODO: different inputs? GeoPandaDataframe / String Filepath
    # TODO: fix this
    try:
        _df = gpd.read_file(geom)
        return ','.join([str(x) for x in _df.total_bounds])
    except Exception as err:
        raise Exception(f'An error occurred: {err}') from err


def _get_id(item):
    # TODO: hacky solution
    if item['data_center'] == 'LPDAAC_ECS':
        return item['producer_granule_id'].split('_')[2]
    elif item['data_center'] == 'ORNL_CLOUD':
        return item['title'].split('_')[7]
    else:
        raise ValueError("Data center not recognized")
