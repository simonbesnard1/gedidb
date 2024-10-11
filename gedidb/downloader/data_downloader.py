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
import pandas as pd
import geopandas as gpd
from typing import Tuple, Any
from functools import wraps
from retry import retry
import logging
from collections import defaultdict
from urllib3.exceptions import NewConnectionError
from requests.exceptions import RequestException, HTTPError, ConnectionError

from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct

# Configure logging
logger = logging.getLogger(__name__)

# Decorator for handling exceptions
def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error occurred in {func.__name__}: {e}")
            # Additional error handling logic can be placed here
            return None
    return wrapper

class GEDIDownloader:
    """
    Base class for GEDI data downloaders.
    """

    @handle_exceptions
    def _download(self, *args, **kwargs):
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
        
        # Iterate over each required product and collect granule information
        for product in GediProduct:
            try:
                granule_query = GranuleQuery(
                    product, self.geom, self.start_date, self.end_date, self.earth_data_info
                )
                granules = granule_query.query_granules()
                
                if not granules.empty:
                    # Organize granules by ID and append (url, product) tuples
                    for _, row in granules.iterrows():
                        cmr_dict[row["id"]].append((row["url"], product.value))
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
            products_found = {product for _, product in product_info}
                        
            # Check for missing products
            missing_products = required_products - products_found
            if missing_products:
                logger.warning(f"Granule {granule_id} is missing products: {missing_products}")
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

    @retry((ValueError, TypeError, HTTPError, ConnectionError, NewConnectionError), tries=10, delay=5, backoff=3)
    def download(self, granule_key: str, url: str, product: GediProduct, parquet_path) -> Tuple[str, Tuple[Any, None]]:
        """
        Download an HDF5 file for a specific granule and product.
        
        :param granule_key: Granule ID.
        :param url: URL to download the HDF5 file from.
        :param product: GEDI product.
        :return: Tuple containing the granule ID and a tuple of product name and file path.
        """
        h5file_path = pathlib.Path(self.download_path) / f"{granule_key}/{product.name}.h5"
        parquetfile_path = os.path.join(parquet_path, f"filtered_granule_{granule_key}.parquet")

        # If file already exists, skip download
        if os.path.exists(parquetfile_path):
            logger.info(f"File already exists for {granule_key}, skipping download.")
            return granule_key, (product.value, str(h5file_path)) 
        
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                os.makedirs(h5file_path.parent, exist_ok=True)
                
                # Write the downloaded content to the file
                with open(h5file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)

                return granule_key, (product.value, str(h5file_path))

        except (RequestException, ConnectionError, NewConnectionError) as e:
            # Log each error encountered during download
            logger.error(f"Error downloading {url} on attempt: {e}")
            raise  # Propagate the error to activate retry

        # Final fallback if retries are exhausted
        except Exception as e:
            logger.error(f"Download failed after all retries for {url}: {e}")
            return granule_key, (product.value, None)  # Explicitly handle max retry failure case