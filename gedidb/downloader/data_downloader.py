import os
import pathlib
import requests
from datetime import datetime
import pandas as pd
import geopandas as gpd
from requests.exceptions import RequestException
from typing import Tuple, Any
from functools import wraps
from retry import retry
import logging

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

    @retry((ValueError, TypeError), tries=10, delay=5, backoff=3)
    def download(self) -> pd.DataFrame:
        """
        Download granules across all GEDI products and ensure ID consistency.
        Retry mechanism is applied if the IDs across products are inconsistent.
        
        :return: A DataFrame containing granule data for all products.
        :raises: ValueError if no granules found or ID consistency fails after retries.
        """
        cmr_df = pd.DataFrame()
        products_checked = []

        for product in GediProduct:
            try:
                granule_query = GranuleQuery(product, self.geom, self.start_date, self.end_date, self.earth_data_info)
                granules = granule_query.query_granules()

                if not granules.empty:
                    granules['product'] = product.value
                    products_checked.append(product.value)
                    cmr_df = pd.concat([cmr_df, granules], ignore_index=True)

            except Exception as e:
                logger.error(f"Failed to download granules for {product.name}: {e}")
                continue

        if cmr_df.empty:
            raise ValueError("No granules found after retry attempts.")
        
        if not self._check_all_products_present(products_checked):
            logger.warning("Not all products have granules. Retrying...")
            raise ValueError("Missing granules for some products.")
        
        if not self._check_id_consistency(cmr_df):
            logger.warning("ID inconsistency detected. Retrying...")
            raise ValueError("Inconsistent IDs found across products.")
        
        return cmr_df
    
    @staticmethod
    def _check_all_products_present(products_checked: list) -> bool:
        """
        Ensure that granules for all products are present in the download.
        
        :param products_checked: List of products for which granules were successfully retrieved.
        :return: True if granules for all required products are present, False otherwise.
        """
        required_products = {'level2A', 'level2B', 'level4A', 'level4C'}
        missing_products = required_products - set(products_checked)
        if missing_products:
            logger.warning(f"Missing products: {missing_products}")
            return False
        return True
    
    @staticmethod
    def _check_id_consistency(df: pd.DataFrame) -> bool:
        """
        Check if IDs are consistent across all products in the DataFrame.
        
        :param df: The DataFrame containing granule data with 'id' and 'product' columns.
        :return: True if IDs are consistent, False otherwise.
        """
        ids_by_product = df.groupby('product')['id'].apply(set)
        first_product_ids = ids_by_product.iloc[0]
        for product_ids in ids_by_product:
            if product_ids != first_product_ids:
                return False
        return True

    @staticmethod
    @handle_exceptions
    def clean_up_cmr_data(cmr_df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean up the CMR data by nesting the 'url' and 'size' attributes within the 'details' column.
        
        :param cmr_df: DataFrame containing CMR data.
        :return: Cleaned DataFrame with nested details for each granule ID.
        """
        def _create_nested_dict(group):
            return {row['product']: {'url': row['url'], 'size': row['size']} for _, row in group.iterrows()}

        final_df = cmr_df.groupby('id').apply(_create_nested_dict).reset_index()
        final_df.columns = ['id', 'details']
        return final_df.sort_values(by='id')

class H5FileDownloader(GEDIDownloader):
    """
    Downloader for HDF5 files from URLs.
    """

    def __init__(self, download_path: str = "."):
        self.download_path = download_path

    @retry((ValueError, TypeError), tries=10, delay=5, backoff=3)
    def download(self, _id: str, url: str, product: GediProduct) -> Tuple[str, Tuple[Any, None]]:
        """
        Download an HDF5 file for a specific granule and product.
        
        :param _id: Granule ID.
        :param url: URL to download the HDF5 file from.
        :param product: GEDI product.
        :return: Tuple containing the granule ID and a tuple of product name and file path.
        """
        file_path = pathlib.Path(self.download_path) / f"{_id}/{product.name}.h5"

        if file_path.exists():
            return _id, (product.value, str(file_path))

        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                os.makedirs(file_path.parent, exist_ok=True)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                return _id, (product.value, str(file_path))

        except RequestException as e:
            logger.error(f"Error downloading {url}: {e}")
            return _id, (product.value, None)