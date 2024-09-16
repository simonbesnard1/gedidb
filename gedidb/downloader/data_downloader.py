import os
import pathlib
import requests
from datetime import datetime
import pandas as pd
import geopandas as gpd
from requests.exceptions import RequestException
from typing import Tuple, Any


from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct
from functools import wraps
from retry import retry

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

class GEDIDownloader:
    @handle_exceptions
    def _download(self, *args, **kwargs):
        raise NotImplementedError("This method should be implemented by subclasses.")

class CMRDataDownloader(GEDIDownloader):
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None, earth_data_info = None):
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
    
        # Iterate over all GEDI products and fetch granules
        for product in GediProduct:
            try:
                granule_query = GranuleQuery(product, self.geom, self.start_date, self.end_date, self.earth_data_info)
                granules = granule_query.query_granules()
    
                if not granules.empty:
                    # Add the product column to the granules DataFrame
                    granules['product'] = product.value
                    # Keep track of the products that have data
                    products_checked.append(product.value)
    
                    # Concatenate to the cumulative DataFrame
                    cmr_df = pd.concat([cmr_df, granules], ignore_index=True)
    
            except Exception as e:
                print(f"Failed to download granules for {product.name}: {e}")
                continue
    
        if len(cmr_df) == 0:
            raise ValueError("No granules found after retry attempts.")
    
        # Ensure that all products have granules
        if not self._check_all_products_present(products_checked):
            print("Not all products have granules. Retrying...")
            raise ValueError("Missing granules for some products.")
    
        # Ensure IDs are consistent across products
        if not self._check_id_consistency(cmr_df):
            print("ID inconsistency detected. Retrying...")
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
            return False
    
        return True
    
    @staticmethod
    def _check_id_consistency(df: pd.DataFrame) -> bool:
        """
        Check if IDs are consistent across all products in the DataFrame.
        
        :param df: The DataFrame containing granule data with 'id' and 'product' columns.
        :return: True if IDs are consistent, False otherwise.
        """
        # Get the set of IDs for each product
        ids_by_product = df.groupby('product')['id'].apply(set)
    
        # Ensure that each product has the same set of IDs
        first_product_ids = ids_by_product.iloc[0]
        for product_ids in ids_by_product:
            if product_ids != first_product_ids:
                return False
    
        return True

    @staticmethod
    @handle_exceptions
    def clean_up_cmr_data(cmr_df: pd.DataFrame) -> pd.DataFrame:
        def _create_nested_dict(group):
            return {row['product']: {'url': row['url'], 'size': row['size']} for _, row in group.iterrows()}

        final_df = cmr_df.groupby('id').apply(_create_nested_dict).reset_index()
        final_df.columns = ['id', 'details']
        return final_df.sort_values(by='id')

class H5FileDownloader(GEDIDownloader):
    def __init__(self, download_path: str = "."):
        self.download_path = download_path

    @retry((ValueError, TypeError), tries=10, delay=5, backoff=3)
    def download(self, _id: str, url: str, product: GediProduct) -> Tuple[str, Tuple[Any, None]]:
        file_path = pathlib.Path(self.download_path) / f"{_id}/{product.name}.h5"
        
        if file_path.exists():
            return _id, (product.value, str(file_path))

        try:
            with requests.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                # Create directory with _id as the name if it does not exist
                os.makedirs(file_path.parent, exist_ok=True)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                return _id, (product.value, str(file_path))

        except RequestException as e:
            print(f"Error downloading {url}: {e}")
            return _id, (product.value, None)