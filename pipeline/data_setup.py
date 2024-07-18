import os

import geopandas as gpd
from datetime import datetime
from sqlalchemy import Engine

from constants import GediProduct
from granule_data import granule_parser
from spark.spark_session import create_spark
from granule_downloader.data_downloader import download_cmr_data, download_h5_file


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
    cmr_data = cmr_data.sort_values(by="size", ascending=True)

    spark = create_spark()

    name_url = cmr_data[
        [
            "id",
            "name",
            "url",
            "product",
        ]
    ].to_records(index=False)

    urls = spark.sparkContext.parallelize(name_url[:2])
    mapped_urls = urls.map(lambda x: download_h5_file(x[0], x[2], x[3])).groupByKey()

    processed_granules = mapped_urls.map(_process_granule)

    for row in processed_granules.collect():
        print(row[0], list(row[1]))


def _process_granule(
    row
):

    granule_key, granules = row
    included_files = sorted([fname[1].name for fname in granules])
    outfile_path = f"filtered_l2ab_l4a_{granule_key}.parquet"
    return_value = (granule_key, outfile_path, included_files)
    if os.path.exists(outfile_path):
        return return_value

    gdfs = {}
    # 1. Parse each file and run per-product filtering.
    for product, file in granules:
        gdfs[product] = (
            granule_parser.parse_granule(product)
            .rename(lambda x: f"{x}_{product.value}", axis=1)
            .rename({f"shot_number_{product.value}": "shot_number"}, axis=1)
        )

    gdf = (
        gdfs[GediProduct.L2B]
        # .join(
        #    gdfs[GediProduct.L2B].set_index("shot_number"),
        #    on="shot_number",
        #    how="inner",
        #)
        .join(
            gdfs[GediProduct.L4A].set_index("shot_number"),
            on="shot_number",
            how="inner",
        )
        .drop(["geometry_level2B", "geometry_level4A"], axis=1)
        .set_geometry("geometry_level2A")
        .rename_geometry("geometry")
    )

    gdf["granule"] = granule_key
    gdf.to_parquet(
        outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us"
    )
    return return_value


if __name__ == "__main__":

    geom = gpd.read_file("../testing/data/test.geojson")

    main_parser(geom, start_date=datetime(2019, 1, 1), end_date=datetime(2021, 1, 31))