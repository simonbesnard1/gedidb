import geopandas as gpd
from datetime import datetime
from functools import wraps

from gedidb.utils.spark_session import create_spark
from gedidb.downloader.data_downloader import CMRDataDownloader


def log_execution(start_message=None, end_message=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            log_message = start_message or f"Executing {func.__name__}..."
            print(log_message)
            result = func(*args, **kwargs)
            log_message = end_message or f"Finished {func.__name__}..."
            print(log_message)
            return result
        return wrapper
    return decorator

class GEDIDatabase:
    def __init__(self, geom: gpd.GeoSeries, start_date: datetime = None, end_date: datetime = None, earth_data_info =None):
        self.geom = geom
        self.start_date = start_date
        self.end_date = end_date
        self.earth_data_info = earth_data_info

    @log_execution(start_message = "Retrieving CMR data...", end_message="CMR data succesfully retrieved")
    def download_cmr_data(self):
        return CMRDataDownloader(self.geom, start_date=self.start_date, 
                                 end_date=self.end_date, earth_data_info= self.earth_data_info).download()
    
    @log_execution(start_message = "Creating spark session...", end_message="Spark session created")
    def create_spark_session(self):
        return create_spark()
