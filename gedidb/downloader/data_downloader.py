# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import pathlib
import requests
from datetime import datetime
import geopandas as gpd
from typing import Tuple, Any, Optional
from retry import retry
import logging
from collections import defaultdict
from urllib3.exceptions import NewConnectionError
from requests.exceptions import HTTPError, ConnectionError, ChunkedEncodingError

from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct

# Configure logging
logger = logging.getLogger(__name__)


class GEDIDownloader:
    """
    Base class for GEDI data downloaders.
    """

    def _download(self, *args, **kwargs):
        """
        Abstract method that must be implemented by subclasses.
        """
        raise NotImplementedError("This method should be implemented by subclasses.")

class CMRDataDownloader(GEDIDownloader):
    """
    Downloader for GEDI granules from NASA's CMR service.
    """

    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None, earth_data_info=None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date
        self.earth_data_info = earth_data_info

    @retry((ValueError, TypeError, HTTPError, ConnectionError, NewConnectionError), tries=10, delay=5, backoff=3)
    def download(self) -> dict:
        """
        Download granules across all GEDI products and ensure ID consistency.
        Returns a dictionary organized by granule ID with a list of (url, product) tuples.
        Retry mechanism is applied if the IDs across products are inconsistent.

        :return: A dictionary containing granule data organized by granule ID.
        :raises: ValueError if no granules with all required products are found.
        """
        cmr_dict = defaultdict(list)
        total_granules = 0
        total_size_mb = 0.0

        # Iterate over each required product and collect granule information
        for product in GediProduct:
            try:
                granule_query = GranuleQuery(
                    product, self.geom, self.start_date, self.end_date, self.earth_data_info
                )
                granules = granule_query.query_granules()

                if not granules.empty:
                    total_granules += len(granules)
                    total_size_mb += granules["size"].astype(float).sum()  # Summing the size column

                    # Organize granules by ID and append (url, product) tuples
                    for _, row in granules.iterrows():

                        cmr_dict[row["id"]].append(
                            (
                                row["url"],
                                product.value,
                                row["start_time"],
                            )
                        )
                else:
                    logger.warning(f"No granules found for product {product.value}.")

            except Exception as e:
                logger.error(f"Failed to download granules for {product.name}: {e}")
                continue

        if not cmr_dict:
            raise ValueError("No granules found after retry attempts.")

        # Filter granules to only include those with all required products
        filtered_cmr_dict = self._filter_granules_with_all_products(cmr_dict)

        if not filtered_cmr_dict:
            raise ValueError("No granules with all required products found.")

        # Log the total number of granules and total size of the data
        logger.info(f"NASA's CMR service found {int(total_granules / len(GediProduct))} granules for a total size of {total_size_mb / 1024:.2f} GB ({total_size_mb / 1_048_576:.2f} TB).")

        return filtered_cmr_dict

    def _filter_granules_with_all_products(self, granules: dict) -> dict:
        """
        Filter the granules dictionary to only include granules that have all required products.

        :param granules: Dictionary where keys are granule IDs and values are lists of (url, product) tuples.
        :return: Filtered dictionary containing only granules with all required products.
        """
        required_products = {'level2A', 'level2B', 'level4A', 'level4C'}
        filtered_granules = {}

        for granule_id, product_info in granules.items():
            # Extract the set of products for the current granule
            products_found = {product[1] for product in product_info}

            # Check for missing products
            missing_products = required_products - products_found
            if missing_products:
                # Skip this granule as it's missing required products
                continue
            else:
                # Include this granule as it has all required products
                filtered_granules[granule_id] = product_info

        return filtered_granules

class H5FileDownloader(GEDIDownloader):
    """
    Downloader for HDF5 files from URLs.
    """

    def __init__(self, download_path: str = "."):
        self.download_path = download_path


    @retry((ValueError, TypeError, HTTPError, ConnectionError, ChunkedEncodingError), tries=10, delay=5, backoff=3)
    def download(self, granule_key: str, url: str, product: GediProduct) -> Tuple[str, Tuple[Any, Optional[str]]]:
        """
        Download an HDF5 file for a specific granule and product with resume support.

        Parameters:
        ----------
        granule_key : str
            Granule ID.
        url : str
            URL to download the HDF5 file from.
        product : GediProduct
            GEDI product.

        Returns:
        -------
        Tuple[str, Tuple[Any, Optional[str]]]
            Tuple containing the granule ID and a tuple of product name and file path.
        """
        h5file_path = pathlib.Path(self.download_path) / f"{granule_key}/{product.name}.h5"
        os.makedirs(h5file_path.parent, exist_ok=True)

        downloaded_size = h5file_path.stat().st_size if h5file_path.exists() else 0
        headers = {'Range': f'bytes={downloaded_size}-'} if downloaded_size > 0 else {}
        total_size = None

        try:
            # Fetch total file size from server
            partial_response = requests.get(url, headers={'Range': 'bytes=0-1'}, stream=True, timeout=30)
            partial_response.raise_for_status()

            if 'Content-Range' in partial_response.headers:
                total_size = int(partial_response.headers['Content-Range'].split('/')[-1])

                # File is already fully downloaded
                if downloaded_size == total_size:
                    return granule_key, (product.value, str(h5file_path))

                # Corrupted partial download
                if downloaded_size > total_size:
                    logger.warning(f"Corrupted file detected: {h5file_path}. Deleting and starting fresh.")
                    h5file_path.unlink()
                    headers = {}

            else:
                headers = {}  # Fallback to full download if Range requests are not supported
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch file size for {url}: {e}. Proceeding with full download.")
            h5file_path.unlink(missing_ok=True)
            headers = {}

        # Perform file download (partial or full)
        try:
            with requests.get(url, headers=headers, stream=True, timeout=120) as response:
                response.raise_for_status()

                # Open the file in append or write mode
                mode = 'ab' if downloaded_size > 0 else 'wb'
                with open(h5file_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)

            return granule_key, (product.value, str(h5file_path))

        except requests.RequestException as e:
            logger.error(f"Download failed for {url}: {e}. Removing partial file and retrying.")
            h5file_path.unlink(missing_ok=True)
            raise
        except Exception as e:
            logger.error(f"Unexpected error during download: {e}")
            return granule_key, (product.value, None)
