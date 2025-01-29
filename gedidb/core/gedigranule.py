# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import logging
import shutil
import pandas as pd
import numpy as np
from typing import Optional, Tuple, List, Dict

from gedidb.utils.constants import GediProduct
from gedidb.granule import granule_parser

# Configure the logger
logger = logging.getLogger(__name__)


class GEDIGranule:
    """
    GEDIGranule handles the processing and management of GEDI granules, including parsing, joining,
    and saving the data to TileDB, as well as querying processed granules from a database.

    Attributes:
    -----------
    download_path : str
        Path where granules are downloaded.
    data_info : dict
        Dictionary containing relevant information about data, such as table names.
    """

    def __init__(self, download_path: str, data_info: dict):
        """
        Initialize the GEDIGranule class.

        Parameters:
        -----------
        download_path : str
            Path where granules are downloaded.
        data_info : dict
            Dictionary containing relevant information about data.
        """
        self.download_path = download_path
        self.data_info = data_info

    def process_granule(
        self, row: Tuple[Tuple[str, str], List[Tuple[str, str]]]
    ) -> Tuple[str, Optional[pd.DataFrame]]:
        """
        Process a granule by parsing, joining, and saving it to TileDB.

        Parameters:
        -----------
        row : Tuple
            Tuple containing the granule key and product data.

        Returns:
        -------
        Tuple[str, Optional[pd.DataFrame]]
            Tuple containing the granule key and the joined DataFrame, or None if processing fails.
        """
        granule_key = row[0][0]
        granules = [item[1] for item in row]

        try:
            gdf_dict = self.parse_granules(granules, granule_key)
            if not gdf_dict:
                logger.warning(
                    f"Granule {granule_key}: Parsing returned no valid data."
                )
                return granule_key, None

            gdf = self._join_dfs(gdf_dict, granule_key)
            if gdf is None:
                return granule_key, None

            return granule_key, gdf
        except Exception as e:
            logger.error(f"Granule {granule_key}: Processing failed with error: {e}")
            return granule_key, None

    def parse_granules(
        self, granules: List[Tuple[str, str]], granule_key: str
    ) -> Dict[str, Dict[str, np.ndarray]]:
        """
        Parse granules and return a dictionary of dictionaries of NumPy arrays.

        Returns:
        --------
        dict
            Dictionary of dictionaries, each containing NumPy arrays for each product.
        """
        data_dict = {}
        granule_dir = os.path.join(self.download_path, granule_key)

        try:
            for product, file in filter(lambda x: x[1] is not None, granules):
                data = granule_parser.parse_h5_file(
                    file, product, data_info=self.data_info
                )

                if data is not None:
                    data_dict[product] = data
                else:
                    logger.warning(
                        f"Granule {granule_key}: Failed to parse product {product}."
                    )

            # Clean up the directory after parsing
            if os.path.exists(granule_dir):
                shutil.rmtree(granule_dir, ignore_errors=True)
        except Exception as e:
            logger.error(f"Granule {granule_key}: Error while parsing: {e}")
            return {}

        return {k: v for k, v in data_dict.items() if "shot_number" in v}

    @staticmethod
    def _join_dfs(
        df_dict: Dict[str, pd.DataFrame], granule_key: str
    ) -> Optional[pd.DataFrame]:
        """
        Join multiple DataFrames based on shot number. Ensure required products are available.

        Returns:
        --------
        pd.DataFrame or None
            Joined DataFrame or None if the required data is missing or if the join fails.
        """
        required_products = [
            GediProduct.L2A,
            GediProduct.L2B,
            GediProduct.L4A,
            GediProduct.L4C,
        ]

        try:
            # Validate required products
            for product in required_products:
                if product.value not in df_dict or df_dict[product.value].empty:
                    return None

            # Start joining with the L2A product
            df = df_dict[GediProduct.L2A.value].reset_index(drop=True)

            for product in required_products[1:]:
                product_df = df_dict[product.value].set_index("shot_number")
                df = (
                    df.set_index("shot_number")
                    .join(product_df, how="inner", rsuffix=f"_{product.value}")
                    .reset_index()
                )

            # Drop duplicate columns with suffixes
            suffixes = [f"_{product.value}" for product in required_products[1:]]
            df = df.loc[:, ~df.columns.str.endswith(tuple(suffixes))]

            return df if not df.empty else None
        except Exception as e:
            logger.error(f"Granule {granule_key}: Error while joining DataFrames: {e}")
            return None
