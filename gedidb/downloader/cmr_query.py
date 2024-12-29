# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import geopandas as gpd
from datetime import datetime
import pandas as pd
import logging
from gedidb.granule.granule import granule_name
from gedidb.utils.constants import GediProduct

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CMRQuery:
    """
    Base class for constructing and handling CMR queries.
    """

    @staticmethod
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
    def _construct_spatial_params(geom: gpd.GeoSeries) -> str:

        if geom is None: return
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
        Removes the '.h5' extension if present and strips whitespace.

        :param item: CMR response item.
        :return: Granule name.
        """
        granule_name = None

        # Try to get the granule name from 'producer_granule_id' (preferred)
        if "LPCLOUD" in item["data_center"]:
            granule_name = item["producer_granule_id"]
        # If 'producer_granule_id' is not available, fallback to 'title'
        elif "ORNL" in item["data_center"]:
            title = item["title"]
            # For 'ORNL' data center, 'title' has a prefix we need to remove
            # Split on the first period and take the second part
            if "." in title:
                granule_name = title.split(".", maxsplit=1)[1]
            else:
                granule_name = title
        else:
            logger.warning(f"Unknown data center or missing granule ID in item: {item}")
            return None

        # Remove any leading/trailing whitespace
        granule_name = granule_name.strip()

        # Remove '.h5' extension if present
        if granule_name.endswith('.h5'):
            granule_name = granule_name[:-3]

        return granule_name

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

    def query_granules(self, page_size: int = 2000, page_num: int = 1) -> pd.DataFrame:
        """
        Query granules from CMR and return them as a DataFrame.

        :param page_size: Number of results per page.
        :param page_num: Starting page number.
        :return: DataFrame containing queried granules.
        """
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
            # Reconstruct cmr_params in each iteration with the updated page_num
            cmr_params = self._construct_query_params(
                self.product,
                self.geom,
                self.start_date,
                self.end_date,
                self.earth_data_info,
                page_size,
                page_num,
            )
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
                "id": self._get_id(granule_name),
                "name": granule_name,
                "url": item["links"][0]["href"],
                "size": float(item["granule_size"]),
                "product": self.product.value,
                "start_time": item["time_start"],
            }
            for item in granule_data
            if (granule_name := self._get_name(item)) is not None
        ]

        return pd.DataFrame(
            granule_data_processed, columns=["id", "name", "url", "size", "product", "start_time"]
        )
