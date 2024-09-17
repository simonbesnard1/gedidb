# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import requests
import urllib3.exceptions
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import geopandas as gpd
from datetime import datetime
import pandas as pd
import logging
from functools import wraps
from gedidb.granule.granule import granule_name
from gedidb.utils.constants import GediProduct

# Configure logging
logger = logging.getLogger(__name__)

# Decorator for handling exceptions
def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except urllib3.exceptions.MaxRetryError:
            logger.error("Max retry error occurred during the request")
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {e}")
            return None  # Ensure graceful handling of the exception
    return wrapper

class CMRQuery:
    """
    Base class for constructing and handling CMR queries.
    """

    @staticmethod
    @handle_exceptions
    def _construct_query_params(
        product: GediProduct,
        geom: gpd.GeoSeries,
        start_date: datetime,
        end_date: datetime,
        earth_data_info: dict,
        page_size: int,
        page_num: int,
    ) -> dict:
        """
        Construct query parameters for the CMR request.
        """
        return {
            "collection_concept_id": earth_data_info["CMR_PRODUCT_IDS"][str(product)],
            "page_size": page_size,
            "page_num": page_num,
            "bounding_box": CMRQuery._construct_spatial_params(geom),
            "temporal": CMRQuery._construct_temporal_params(start_date, end_date),
        }

    @staticmethod
    @handle_exceptions
    def _construct_temporal_params(
        start_date: datetime, end_date: datetime
    ) -> str:
        """
        Construct the temporal query parameter for the CMR request.
        """
        if start_date and end_date:
            if start_date > end_date:
                raise ValueError("Start date must be before end date")
            return f'{start_date.strftime("%Y-%m-%dT00:00:00Z")}/{end_date.strftime("%Y-%m-%dT23:59:59Z")}'
        if start_date:
            return f'{start_date.strftime("%Y-%m-%dT00:00:00Z")}/'
        if end_date:
            return f'/{end_date.strftime("%Y-%m-%dT23:59:59Z")}'
        return ""

    @staticmethod
    @handle_exceptions
    def _construct_spatial_params(geom: gpd.GeoSeries) -> str:
        """
        Construct the bounding box query parameter from a GeoSeries geometry.
        """
        return ",".join([str(coord) for coord in geom.total_bounds])

    @staticmethod
    def _get_id(name: str) -> str:
        """
        Get the granule ID from the granule name.

        :param name: Granule name.
        :return: Granule ID.
        """
        metadata = granule_name.parse_granule_filename(name)
        return f"{metadata.orbit}_{metadata.sub_orbit_granule}"

    @staticmethod
    def _get_name(item: dict) -> str:
        """
        Extract the name of the granule from the CMR response item.

        :param item: CMR response item.
        :return: Granule name.
        """
        if "LPCLOUD" in item["data_center"]:
            return item["producer_granule_id"]
        if "ORNL" in item["data_center"]:
            return item["title"].split(".", maxsplit=1)[1]
        return None


class GranuleQuery(CMRQuery):
    """
    Class for querying GEDI granules from CMR.
    """

    def __init__(
        self,
        product: GediProduct,
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        earth_data_info: dict = None
    ):
        """
        Initialize the GranuleQuery class.

        :param product: The GEDI product to query.
        :param geom: The geometry for spatial filtering.
        :param start_date: The start date for temporal filtering.
        :param end_date: The end date for temporal filtering.
        :param earth_data_info: Dictionary containing EarthData information.
        """
        self.product = product
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date
        self.earth_data_info = earth_data_info
        
    @handle_exceptions
    def query_granules(
        self, page_size: int = 2000, page_num: int = 1
    ) -> pd.DataFrame:
        """
        Query granules from CMR and return them as a DataFrame.

        :param page_size: Number of results per page.
        :param page_num: Starting page number.
        :return: DataFrame containing queried granules.
        """
        cmr_params = self._construct_query_params(
            self.product,
            self.geom,
            self.start_date,
            self.end_date,
            self.earth_data_info,
            page_size,
            page_num,
        )
        granule_data = []

        # Configure retry strategy for the HTTP session
        adapter = HTTPAdapter(
            max_retries=Retry(
                total=3,
                backoff_factor=0.1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
        )
        session = requests.Session()
        session.mount("https://", adapter)

        while True:
            cmr_params["page_num"] = page_num
            response = session.get(self.earth_data_info["CMR_URL"], params=cmr_params)
            response.raise_for_status()
            cmr_response = response.json()["feed"]["entry"]

            if cmr_response:
                granule_data.extend(cmr_response)
                page_num += 1
            else:
                break

        session.close()

        # Process granule data into a structured DataFrame
        granule_data_processed = [
            {
                "id": self._get_id(self._get_name(item)),
                "name": self._get_name(item),
                "url": item["links"][0]["href"],
                "size": float(item["granule_size"]),
                "product": self.product.value,
            }
            for item in granule_data if self._get_name(item) is not None
        ]

        return pd.DataFrame(
            granule_data_processed, columns=["id", "name", "url", "size", "product"]
        )
