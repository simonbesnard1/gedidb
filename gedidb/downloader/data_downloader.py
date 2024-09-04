import os
import pathlib
import time
import requests
from datetime import datetime
import pandas as pd
import geopandas as gpd
from requests.exceptions import RequestException
from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct
from functools import wraps

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

# Retry decorator for extended backoff (up to 24 hours)
def retry_with_extended_backoff(retries=10, backoff_in_seconds=60, max_wait_time_hours=24):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            total_wait_time = 0
            
            while attempts < retries and total_wait_time < max_wait_time_hours * 3600:
                try:
                    return func(*args, **kwargs)
                except RequestException as e:
                    attempts += 1
                    wait_time = backoff_in_seconds * (2 ** (attempts - 1))
                    total_wait_time += wait_time
                    print(f"Attempt {attempts} failed: {e}. Retrying in {wait_time // 60} minutes...")
                    
                    # If the wait time exceeds 1 hour, cap it at 1 hour per retry
                    if wait_time > 3600:
                        wait_time = 3600
                    time.sleep(wait_time)
            print(f"All {retries} attempts or {max_wait_time_hours} hours of waiting have failed.")
            return None
        return wrapper
    return decorator

class GEDIDownloader:
    @handle_exceptions
    def _download(self, *args, **kwargs):
        raise NotImplementedError("This method should be implemented by subclasses.")

class CMRDataDownloader(GEDIDownloader):
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date

    @retry_with_extended_backoff(retries=20, backoff_in_seconds=60, max_wait_time_hours=24)
    @handle_exceptions
    def download(self) -> pd.DataFrame:
        cmr_df = pd.DataFrame()

        for product in GediProduct:
            try:
                granule_query = GranuleQuery(product, self.geom, self.start_date, self.end_date)
                granules = granule_query.query_granules()
                cmr_df = pd.concat([cmr_df, granules])
            except Exception as e:
                print(f"Failed to download granules for {product.name}: {e}")

        if len(cmr_df) == 0:
            raise ValueError("No granules found after retry attempts.")
        
        return cmr_df

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

    @retry_with_extended_backoff(retries=20, backoff_in_seconds=60, max_wait_time_hours=24)
    @handle_exceptions
    def download(self, _id: str, url: str, product: GediProduct) -> tuple[str, tuple[GediProduct, str]]:
        file_path = pathlib.Path(self.download_path) / f"{_id}/{product.name}.h5"
        
        if file_path.exists():
            print(f"{file_path} already exists.")
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