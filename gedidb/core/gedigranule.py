# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import logging
import shutil
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Dict

from gedidb.utils.constants import GediProduct
from gedidb.granule import granule_parser
from gedidb.core.gedidatabase import GEDIDatabase

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GEDIGranule:
    """
    GEDIGranule handles the processing and management of GEDI granules, including parsing, joining,
    and saving the data to S3 object storage, as well as querying processed granules from a database.

    Attributes:
    ----------
    download_path : str
        Path where granules are downloaded.
    data_info : dict
        Dictionary containing relevant information about data, such as table names.
    """

    def __init__(self, download_path: str, data_info: dict):
        """
        Initialize the GEDIGranule class.

        Parameters:
        ----------
        download_path : str
            Path where granules are downloaded.
        data_info : dict
            Dictionary containing relevant information about data.
        """
        self.download_path = download_path
        self.data_info = data_info
        self.data_writer =  GEDIDatabase(data_info)

    def process_granule(self, row: Tuple):
        """
        Process a granule by parsing, joining, and saving it to TileDB.

        Parameters:
        ----------
        row : tuple
            Tuple containing the granule key and product data.

        Returns:
        -------
        tuple or None
            Tuple containing the granule key, output path (TileDB array URI), and list of processed files, or None if processing fails.
        """
        granule_key = row[0][0]
        granules = [item[1] for item in row]

        gdf_dict = self.parse_granules(granules, granule_key)
        if not gdf_dict:
            self.data_writer.mark_granule_as_processed(granule_key)
            return None

        gdf = self._join_dfs(gdf_dict, granule_key)
        if gdf is None:
            self.data_writer.mark_granule_as_processed(granule_key)
            return None

        return granule_key, gdf

        # .data_writer.write_granule(gdf)
        # self.data_writer.mark_granule_as_processed(granule_key)

    def parse_granules(self, granules: List[Tuple[str, str]], granule_key: str) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Parse granules and return a dictionary of dictionaries of NumPy arrays.

        Returns:
        -------
        dict
            Dictionary of dictionaries, each containing NumPy arrays for each product.
        """
        data_dict = {}
        granule_dir = os.path.join(self.download_path, granule_key)

        for product, file in granules:
            if file is None:
                continue

            data = granule_parser.parse_h5_file(file, product, data_info=self.data_info)

            if data is not None:
                data_dict[product] = data
            else:
                logger.warning(f"Skipping product {product} for granule {granule_key} due to parsing failure.")

        shutil.rmtree(granule_dir)

        return {k: v for k, v in data_dict.items() if "shot_number" in v}

    @staticmethod
    def _join_dfs(df_dict: Dict[str, pd.DataFrame], granule_key: str) -> Optional[pd.DataFrame]:
        """
        Join multiple DataFrames based on shot number. Ensure required products are available.

        Returns:
        -------
        pd.DataFrame or None
            Joined DataFrame or None if the required data is missing or if the join fails.
        """
        required_products = [GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]

        # Check if required products are available and non-empty
        for product in required_products:
            if product.value not in df_dict or df_dict[product.value].empty:
                return None

        # Start with the L2A product DataFrame and reset the index
        df = df_dict[GediProduct.L2A.value].reset_index(drop=True)

        # Perform the join for each required product based on the 'shot_number' column
        for product in [GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]:
            product_df = df_dict[product.value].set_index("shot_number")
            df = df.set_index("shot_number").join(
                product_df,
                how="inner",
                rsuffix=f'_{product.value}'
            ).reset_index(drop=False)

        # Drop duplicate columns (those with suffixes from the join)
        suffixes = [f'_{GediProduct.L2B.value}', f'_{GediProduct.L4A.value}', f'_{GediProduct.L4C.value}']
        columns_to_drop = [col for col in df.columns if col.endswith(tuple(suffixes))]
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)

        return df if not df.empty else None
