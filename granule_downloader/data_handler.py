import traceback
from datetime import datetime

import pandas as pd
import requests

from granule_downloader.cmr_query import granule_query
from constants import GediProduct
import geopandas as gpd


def download_cmr_data(
        geom: str
) -> pd.DataFrame:
    cmr_df = pd.DataFrame()

    for product in GediProduct:
        cmr_df = pd.concat([cmr_df, granule_query(product, geom)])

    # Display the final dataframe
    # cmr_df.to_json("granule_data.csv", orient='records')
    cmr_df.to_csv("granule_data.csv")

    return _clean_up_cmr_data(cmr_df)


def download_h5_file(
        _id: str,
        url: str,
        product: GediProduct
):
    print(f"Trying to download product {product.value} from {url}")

    start_time = datetime.now()
    try:
        print(f"Downloading")
        with requests.get(url) as r:
            r.raise_for_status()
            # print(f'header size (MB): {round(int(r.headers.get('content-length')) / (1024 * 1024), 2)}')
            print(f"Download complete, total time taken:", datetime.now() - start_time)
            # TODO: name by id
            with open(f"./downloads/{product.name}_{_id}.h5", 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

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
