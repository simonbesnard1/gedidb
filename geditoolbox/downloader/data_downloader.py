import os
import pathlib
from datetime import datetime

import pandas as pd
import requests

from geditoolbox.downloader.cmr_query import granule_query
from geditoolbox.utils.constants import GediProduct
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

    # TODO: needed?
    if save_to_cmr:
        cmr_df.to_csv("granule_data2.csv")
        print("saved")

    return cmr_df
    # return _clean_up_cmr_data(cmr_df)


def download_h5_file(
        _id: str,
        url: str,
        product: GediProduct
) -> tuple[str, tuple[GediProduct, str]]:

    if pathlib.Path(f"./{_id}/{product}.h5").exists():
        print(f"./{_id}/{product}.h5 Already exists")
        return _id, (product, f"./{_id}/{product}.h5")

    try:
        print(f"{product}: Downloading")
        with requests.get(url) as r:
            r.raise_for_status()
            # make dir with _id as name if not existing:
            if not os.path.exists(_id):
                os.makedirs(_id)
            with open(f"./{_id}/{product}.h5", 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
            print(f"{product}: Done")
            return _id, (product, f"./{_id}/{product}.h5")

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
