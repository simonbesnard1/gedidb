import os
import pathlib
from datetime import datetime
import pandas as pd
import requests
from GEDItools.downloader.cmr_query import GranuleQuery
from GEDItools.utils.constants import GediProduct
import geopandas as gpd
from functools import wraps

# Decorator for logging
def log_execution(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        print(f"Executing {func.__name__}...")
        result = func(*args, **kwargs)
        print(f"Finished {func.__name__}.")
        return result
    return wrapper

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
    #@log_execution
    @handle_exceptions
    def _download(self, *args, **kwargs):
        raise NotImplementedError("This method should be implemented by subclasses.")

class CMRDataDownloader(GEDIDownloader):
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date

    #@log_execution
    @handle_exceptions
    def download(self) -> pd.DataFrame:
        cmr_df = pd.DataFrame()

        for product in GediProduct:
            cmr_df = pd.concat([cmr_df, GranuleQuery(product, self.geom, self.start_date, self.end_date).query_granules()])

        if len(cmr_df) == 0:
            raise ValueError("No granules found")

        return cmr_df

    @staticmethod
    #@log_execution
    @handle_exceptions
    def clean_up_cmr_data(cmr_df: pd.DataFrame) -> pd.DataFrame:
        def _create_nested_dict(group):
            return {row['product']: {'url': row['url'], 'size': row['size']} for _, row in group.iterrows()}

        final_df = cmr_df.groupby('id').apply(_create_nested_dict).reset_index()
        final_df.columns = ['id', 'details']

        # Sort the dataframe by 'id'
        return final_df.sort_values(by='id')

class H5FileDownloader(GEDIDownloader):
    def __init__(self, download_path: str = "."):
        self.download_path = download_path

    #@log_execution
    @handle_exceptions
    def download(self, _id: str, url: str, product: GediProduct) -> tuple[str, tuple[GediProduct, str]]:
        file_path = pathlib.Path(self.download_path) / f"{_id}/{product}.h5"
        
        if file_path.exists():
            print(f"{file_path} Already exists")
            return _id, (product.value, str(file_path))

        try:
            print(f"{product}: Downloading")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                # make dir with _id as name if not existing:
                os.makedirs(file_path.parent, exist_ok=True)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                print(f"{product}: Done")
                return _id, (product.value, str(file_path))

        except Exception as e:
            print(f"Error downloading {url}: {e}")
            return _id, (product.value, None)
