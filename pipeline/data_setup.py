import geopandas as gpd
from datetime import datetime
from sqlalchemy import Engine
from spark.spark_session import create_spark
from granule_downloader.data_handler import download_cmr_data, download_h5_file


def main_parser(
    geom: gpd.GeoSeries,
    start_date: datetime = None,
    end_date: datetime = None,
    sql_connector: Engine = None,
    save_cmr_data: bool = False,
    delete_h5_files: bool = True,
):

    # Download data from CMR

    cmr_data = download_cmr_data(geom, start_date=start_date, end_date=end_date)

    spark = create_spark()
    # print(cmr_data)

    name_url = cmr_data[
        [
            "id",
            "name",
            "url",
            "product",
        ]
    ].to_records(index=False)

    urls = spark.sparkContext.parallelize(name_url)
    urls.map(lambda x: download_h5_file(x[0], x[2], x[3])).collect()



if __name__ == "__main__":

    geom = gpd.read_file("../testing/data/test.geojson")

    main_parser(geom, start_date=datetime(2019, 1, 1), end_date=datetime(2021, 1, 31))