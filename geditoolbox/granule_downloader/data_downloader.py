import os
import traceback
from datetime import datetime

import pandas as pd
import requests

from geditoolbox.granule_downloader.cmr_query import granule_query
from constants import GediProduct
import geopandas as gpd


def download_cmr_data(
        geom: gpd.GeoSeries,
        start_date: datetime = None,
        end_date: datetime = None,
        save_to_cmr: bool = False
) -> pd.DataFrame:
    cmr_df = pd.DataFrame()

    for product in GediProduct:
        cmr_df = pd.concat([cmr_df, granule_query(product, geom, start_date, end_date)])

    if len(cmr_df) == 0:
        raise ValueError("No granules found")

    if save_to_cmr:
        cmr_df.to_csv("granule_data.csv")

    return cmr_df
    # return _clean_up_cmr_data(cmr_df)


def download_h5_file(
        _id: str,
        url: str,
        product: GediProduct
):
    try:
        print(f"Downloading")
        with requests.get(url) as r:
            r.raise_for_status()
            # make dir with _id as name if not existing:
            if not os.path.exists(_id):
                os.makedirs(_id)
            with open(f"./{_id}/{product}.h5", 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            print("here we are")
            return _id, f"./{_id}/{product}.h5"

    except Exception as e:
        print(e)
        # traceback.print_exc()


def _clean_up_cmr_data(
        cmr_df: pd.DataFrame
) -> pd.DataFrame:
    def _create_nested_dict(group):
        return {row['product']: {'url': row['url'], 'size': row['size']} for _, row in group.iterrows()}

    final_df = cmr_df.groupby('id').apply(_create_nested_dict).reset_index()
    final_df.columns = ['id', 'details']

    # Sort the dataframe by 'id'
    return final_df.sort_values(by='id')
