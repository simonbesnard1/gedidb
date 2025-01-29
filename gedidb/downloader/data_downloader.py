# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import pathlib
import requests
from datetime import datetime
import geopandas as gpd
from typing import Tuple, Optional
from retry import retry
import logging
from collections import defaultdict
from urllib3.exceptions import NewConnectionError
from requests.exceptions import (
    HTTPError, ConnectionError, ChunkedEncodingError, Timeout, RequestException
)
from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct

# Configure logging
logger = logging.getLogger(__name__)

# Create a filter to suppress WARNING messages
class WarningFilter(logging.Filter):
    def filter(self, record):
        return record.levelno != logging.WARNING  # Exclude only WARNING logs


# Apply the filter
logger.addFilter(WarningFilter())

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

    def __init__(
        self,
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        earth_data_info=None,
    ):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date
        self.earth_data_info = earth_data_info

    @retry(
        (ValueError, TypeError, HTTPError, ConnectionError, NewConnectionError),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger
    )
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
                    product,
                    self.geom,
                    self.start_date,
                    self.end_date,
                    self.earth_data_info,
                )
                granules = granule_query.query_granules()

                if not granules.empty:
                    total_granules += len(granules)
                    total_size_mb += (
                        granules["size"].astype(float).sum()
                    )  # Summing the size column

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
        logger.info(
            f"NASA's CMR service found {int(total_granules / len(GediProduct))} granules for a total size of {total_size_mb / 1024:.2f} GB ({total_size_mb / 1_048_576:.2f} TB)."
        )
        
        return filtered_cmr_dict

    def _filter_granules_with_all_products(self, granules: dict) -> dict:
        """
        Filter the granules dictionary to only include granules that have all required products.

        :param granules: Dictionary where keys are granule IDs and values are lists of (url, product) tuples.
        :return: Filtered dictionary containing only granules with all required products.
        """
        required_products = {"level2A", "level2B", "level4A", "level4C"}
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

class H5FileDownloader:
    """
    Downloader for HDF5 files from URLs, with resume and retry support,
    plus a temporary ".part" approach to ensure data integrity.
    """

    def __init__(self, download_path: str = "."):
        self.download_path = pathlib.Path(download_path)

    @retry(
        (ValueError, TypeError, HTTPError, ConnectionError, ChunkedEncodingError, Timeout, RequestException),
        tries=10,
        delay=5,
        backoff=3,
        logger=logger
    )
    def download(self, granule_key: str, url: str, product: GediProduct) -> Tuple[str, Tuple[str, Optional[str]]]:
        """
        Download an HDF5 file for a specific granule and product with resume support
        using a temporary ".part" file. Renames to ".h5" only upon successful download.

        :param granule_key: Granule ID.
        :param url: URL to download the HDF5 file from.
        :param product: GEDI product.
        :return: (granule_key, (product_value, local_filepath or None if download failed))
        """
        # Paths
        granule_dir = self.download_path / granule_key
        final_path = granule_dir / f"{product.name}.h5"       # The final HDF5 file
        temp_path = granule_dir / f"{product.name}.h5.part"   # Temporary file for partial download
        os.makedirs(granule_dir, exist_ok=True)

        # Check if we already have a fully downloaded file:
        if final_path.exists():
            return granule_key, (product.value, str(final_path))

        # If we have a partial file from a previous attempt, get its size:
        downloaded_size = temp_path.stat().st_size if temp_path.exists() else 0

        # Attempt to retrieve total file size with a small GET request (Range=0-1)
        headers = {"Range": "bytes=0-1"}
        total_size: Optional[int] = None
            
        try:
            partial_response = requests.get(url, headers=headers, stream=True, timeout=30)
            if "Content-Range" in partial_response.headers:
                total_size = int(partial_response.headers["Content-Range"].split("/")[-1])
                # If partial file already matches the total, rename and skip
                if downloaded_size == total_size:
                    temp_path.rename(final_path)
                    return granule_key, (product.value, str(final_path))

                # Otherwise, resume download from downloaded_size
                headers["Range"] = f"bytes={downloaded_size}-"
            else:
                # Server doesn't support partial range requests
                headers = {}
        except requests.RequestException as e:
            logger.warning(f"Failed to get Content-Range for {granule_key}: {e}. Will download from scratch.")
            headers = {}  # Fallback to full download if size check fails

        # Download (or resume) the file to the .part path
        try:
            with requests.get(url, headers=headers, stream=True, timeout=30) as r:
                r.raise_for_status()

                # Open in append mode if we are resuming, otherwise write mode
                mode = "ab" if headers.get("Range") else "wb"
                with open(temp_path, mode) as f:
                    bytes_downloaded_this_session = 0
                    chunk_size = 8 * 1024 * 1024  # 8 MB

                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            bytes_downloaded_this_session += len(chunk)

            # After writing, validate final size if we know total_size
            final_downloaded_size = temp_path.stat().st_size
            if total_size is not None:
                if final_downloaded_size != total_size:
                    # The file is not fully downloaded or there's a mismatch
                    raise ValueError(
                        f"Final size mismatch for {granule_key}. "
                        f"Expected {total_size}, got {final_downloaded_size}."
                    )

            # Rename from .part to .h5, indicating a complete file
            temp_path.rename(final_path)
            return granule_key, (product.value, str(final_path))

        except (HTTPError, ConnectionError, ChunkedEncodingError) as e:
            # Will be retried up to 'tries' times by the @retry decorator
            logger.warning(f"Encountered network error for {granule_key}: {e}. Retrying...")
            raise

        except Exception as e:
            logger.error(f"Download failed after all retries for {granule_key}: {e}")
            return granule_key, (product.value, None)  # Return None if all retries fail
