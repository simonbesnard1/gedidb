import os

import geopandas as gpd
import pandas as pd
from datetime import datetime
from sqlalchemy import Engine

from constants import GediProduct
from geditoolbox.db import db
from geditoolbox.db.column_to_field import FIELD_TO_COLUMN
from geditoolbox.granule_data import granule_parser
from geditoolbox.spark.spark_session import create_spark
from geditoolbox.granule_downloader.data_downloader import download_cmr_data, download_h5_file


def main_parser(
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        sql_connector: Engine = None,
        save_cmr_data: bool = False,
        delete_h5_files: bool = True,
        quality_filter: bool = True,
):
    # Download data from CMR

    # cmr_data = download_cmr_data(geom, start_date=start_date, end_date=end_date, save_to_cmr=True)
    cmr_data = pd.read_csv("granule_data2.csv")

    spark = create_spark()

    name_url = cmr_data[
        [
            "id",
            "name",
            "url",
            "product",
        ]
    ].to_records(index=False)

    urls = spark.sparkContext.parallelize(name_url)
    mapped_urls = urls.map(lambda x: download_h5_file(x[0], x[2], x[3])).groupByKey()

    processed_granules = mapped_urls.map(_process_granule)

    for row in processed_granules.collect():
        print(row[0], row[1])

    granule_entries = processed_granules.coalesce(8).map(_write_db)
    granule_entries.count()
    spark.stop()
    print("done")


def _process_granule(
        row: tuple[str, tuple[GediProduct, str]]
):
    granule_key, granules = row
    included_files = sorted([fname[0] for fname in granules])
    outfile_path = f"filtered_l2ab_l4a_{granule_key}.parquet"
    return_value = (granule_key, outfile_path, included_files)
    if os.path.exists(outfile_path):
        return return_value

    gdfs = {}
    # 1. Parse each file and run per-product filtering.
    for product, file in granules:
        gdfs[product] = (
            granule_parser.parse_h5_file(file, product, quality_filter=True)
            .rename(lambda x: f"{x}_{product}", axis=1)
            .rename({f"shot_number_{product}": "shot_number"}, axis=1)
        )

        print(list(gdfs[product].columns))

    # TODO: L1B, L4C and are not used in this pipeline
    gdf = (
        gdfs[GediProduct.L2A.value]
        .join(
            gdfs[GediProduct.L2B.value].set_index("shot_number"),
            on="shot_number",
            how="inner",
        )
        .join(
            gdfs[GediProduct.L4A.value].set_index("shot_number"),
            on="shot_number",
            how="inner",
        )
        .drop(["geometry_level2B", "geometry_level4A"], axis=1)
        .set_geometry("geometry_level2A")
        .rename_geometry("geometry")
    )

    print("parquet step")

    gdf["granule"] = granule_key
    gdf.to_parquet(
        outfile_path, allow_truncated_timestamps=True, coerce_timestamps="us"
    )

    return return_value


def _write_db(input):

    granule_key, outfile_path, included_files = input
    gedi_data = gpd.read_parquet(outfile_path)

    if gedi_data.empty:
        print("empty")
        return
    gedi_data = gedi_data[list(FIELD_TO_COLUMN.keys())]
    gedi_data = gedi_data.rename(columns=FIELD_TO_COLUMN)
    gedi_data = gedi_data.astype({"shot_number": "int64"})
    print(f"Writing granule: {granule_key}")

    with db.get_db_conn().begin() as conn:
        granule_entry = pd.DataFrame(
            data={
                "granule_name": [granule_key],
                # "granule_hash": [hash_string_list(included_files)],
                "granule_file": [outfile_path.name],
                "l2a_file": [included_files[0]],
                "l2b_file": [included_files[1]],
                "l4a_file": [included_files[2]],
                "created_date": [pd.Timestamp.utcnow()],
            }
        )
        granule_entry.to_sql(
            name="gedi_granules",
            con=conn,
            index=False,
            if_exists="append",
        )
        """
        gedi_data.to_postgis(
            name="filtered_l2ab_l4a_shots",
            con=conn,
            index=False,
            if_exists="append",
        )
        """
        conn.commit()
        del gedi_data
    return granule_entry
