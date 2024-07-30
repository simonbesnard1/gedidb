import requests
import geopandas as gpd
from datetime import datetime
import pandas as pd
from geditoolbox.utils.constants import GediProduct

# Configuration for accessing NASA's Earthdata Search API to query granule data.
# TODO: add to config
CMR_URL = "https://cmr.earthdata.nasa.gov/search/granules.json"
CMR_PRODUCT_IDS = {
    GediProduct.L1B: 'C1908344278-LPDAAC_ECS',
    GediProduct.L2A: 'C1908348134-LPDAAC_ECS',
    GediProduct.L2B: 'C1908350066-LPDAAC_ECS',
    # GediProduct.L3: 'C2153683336-ORNL_CLOUD',
    GediProduct.L4A: 'C2237824918-ORNL_CLOUD',
    GediProduct.L4C: 'C3049900163-ORNL_CLOUD',

    # GediProduct.L1B: 'C2142749196-LPCLOUD',
    # GediProduct.L2A: 'C2142771958-LPCLOUD',
    # GediProduct.L2B: 'C2142776747-LPCLOUD',
}


def granule_query(
        product: GediProduct,
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        page_size: int = 2000,
        page_num: int = 1
) -> pd.DataFrame:
    """
    Queries NASA's Earthdata Search API for granules of a specific GEDI product within a given geometry and time range.

    Parameters:
    - product (GediProduct): The GEDI product type to query.
    - geom (gpd.GeoSeries): The geographical area of interest as a GeoSeries object.
    - start_date (datetime, optional): The start date of the time range for the query.
    - end_date (datetime, optional): The end date of the time range for the query.
    - page_size (int, optional): The number of results to return per page. Default is 2000.
    - page_num (int, optional): The page number to start from. Default is 1.

    Returns:
    - pd.DataFrame: A DataFrame containing the queried granule data.
    """

    cmr_params = _construct_query_params(product, geom, start_date, end_date, page_size, page_num)

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
        'name': _get_name(item),
        'url': item['links'][0]['href'],
        'size': float(item['granule_size']),
        'product': product.value
    } for item in granule_data]

    df_granule = pd.DataFrame(
        granule_data,
        columns=['id', 'name', 'url', 'size', 'product']
    )

    # print(f"Total {product.value} granules found:", len(df_granule))
    # print("Total file size (MB): ", '{0:,.2f}'.format(df_granule['size'].sum()))

    return df_granule


def _construct_query_params(
        product: GediProduct,
        geom: gpd.GeoSeries,
        start_date: datetime,
        end_date: datetime,
        page_size: int,
        page_num: int,
) -> dict:
    """
    Constructs the query parameters for the NASA Earthdata Search API request.

    Parameters:
    - product (GediProduct): The GEDI product type to query.
    - geom (gpd.GeoSeries): The geographical area of interest.
    - start_date (datetime): The start date of the time range for the query.
    - end_date (datetime): The end date of the time range for the query.
    - page_size (int): The number of results to return per page.
    - page_num (int): The page number to start from.

    Returns:
    - dict: A dictionary of query parameters for the API request.
    """

    cmr_params = {
        "collection_concept_id": CMR_PRODUCT_IDS[product],
        "page_size": page_size,
        "page_num": page_num,
        "bounding_box": _construct_spacial_params(geom),
        "temporal": _construct_temporal_params(start_date, end_date)
    }

    return cmr_params


def _construct_temporal_params(
        start_date: datetime,
        end_date: datetime
) -> str:
    """
    Constructs the temporal parameter for the NASA Earthdata Search API request.

    Parameters:
    - start_date (datetime): The start date of the time range for the query.
    - end_date (datetime): The end date of the time range for the query.

    Returns:
    - str: A string representing the temporal range for the query.
    """
    # TODO: h/m/s?

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
        geom: gpd.GeoSeries
) -> str:
    """
    Constructs the spatial parameter for the NASA Earthdata Search API request.

    Parameters:
    - geom (gpd.GeoSeries): The geographical area of interest as a GeoSeries object.

    Returns:
    - str: A string representing the bounding box of the geographical area, suitable for API requests.
    """
    return ','.join([str(x) for x in geom.total_bounds])


def _get_id(item):
    """
    Extracts and formats the granule ID from the API response item.

    This function differentiates between data centers (LPDAAC and ORNL) and formats the granule ID
    based on the specific conventions of each data center. The ID consists of the orbit and surborbit numbers.

    Parameters:
    - item (dict): A single item from the list of entries in the NASA Earthdata Search API response.

    Returns:
    - str: The formatted granule ID.
    """
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


def _get_name(item):
    """
    Extracts the granule name from the API response item.

    This function differentiates between data centers (LPDAAC and ORNL) and extracts the granule name
    based on the specific conventions of each data center.

    Parameters:
    - item (dict): A single item from the list of entries in the NASA Earthdata Search API response.

    Returns:
    - str: The granule name.
    """
    if "LPDAAC" in item["data_center"]:
        return item["producer_granule_id"]
    if "ORNL" in item["data_center"]:
        return item["title"].split(".", maxsplit=1)[1]
    return None
