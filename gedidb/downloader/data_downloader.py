import os
import pathlib
from datetime import datetime
import pandas as pd
import requests
from gedidb.downloader.cmr_query import GranuleQuery
from gedidb.utils.constants import GediProduct
import geopandas as gpd
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

class GEDIDownloader:
    @handle_exceptions
    def _download(self, *args, **kwargs):
        raise NotImplementedError("This method should be implemented by subclasses.")

class CMRDataDownloader(GEDIDownloader):
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date

    @handle_exceptions
    def download(self) -> pd.DataFrame:
        cmr_df = pd.DataFrame()

        for product in GediProduct:
            cmr_df = pd.concat([cmr_df, GranuleQuery(product, self.geom, self.start_date, self.end_date).query_granules()])

        if len(cmr_df) == 0:
            raise ValueError("No granules found")

        return cmr_df

    @staticmethod
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

    @handle_exceptions
    def download(self, _id: str, url: str, product: GediProduct) -> tuple[str, tuple[GediProduct, str]]:

        if datetime.now().weekday() == 2:  # 2 corresponds to Wednesday
            print("Wednesdays there is a scheduled maintenance. Download might be affected.")
            return _id, (product.value, None)

        file_path = pathlib.Path(self.download_path) / f"{_id}/{product.name}.h5"
        
        if file_path.exists():
            print(f"{file_path} Already exists")
            return _id, (product.value, str(file_path))

        dl_try = 1

        def attempt_download():
            with requests.get(url, stream=True, timeout=10) as r:
                r.raise_for_status()
                os.makedirs(file_path.parent, exist_ok=True)
                with open(file_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                if file_path.stat().st_size == 0 and dl_try == 2:
                    self._write_debug_info(_id, url, product.value, ValueError("Downloaded file size is 0"))
                    return _id, (product.value, None)
                return _id, (product.value, str(file_path))


        try:
            return attempt_download()
        except Exception as e:
            print(f"TRY {dl_try} Error downloading {url}: {e}")
            dl_try += 1
            try:
                return attempt_download()
                self._write_debug_info(_id, url, product, e)
            except Exception as e:
                print(f"TRY {dl_try} Error downloading {url}: {e}")
                self._write_debug_info(_id, url, product, e)
                return _id, (product.value, None)

    def _write_debug_info(self, _id: str, url: str, product: GediProduct, error: Exception):#
        # TODO: debug path?
        # debug_file = pathlib.Path(self.debug_path) / f"{_id}_debug.log"
        debug_file = pathlib.Path(self.download_path) / f"{_id}_debug.log"
        with open(debug_file, 'a') as f:
            f.write(f"Timestamp: {datetime.now()}\n")
            f.write(f"Product: {product.value}\n")
            f.write(f"URL: {url}\n")
            f.write(f"Error: {error}\n")
            f.write("\n")

