# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import os
import shutil
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from gedidb.granule import granule_parser
from gedidb.utils.constants import GediProduct, required_products

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
        if not row:
            raise ValueError("No product files supplied.")
        granule_key = row[0][0]
        if any(item[0] != granule_key for item in row):
            raise ValueError("Product files belong to different granules.")
        granules = [item[1] for item in row]
        expected = required_products(self.data_info)
        available = {product for product, path in granules if path is not None}
        if any(product.value not in available for product in expected):
            raise ValueError(
                f"Granule {granule_key}: Missing required product file(s)."
            )
        frames = self.parse_granules(granules, granule_key)
        joined = self._join_dfs(frames, granule_key, expected)
        # Only successful parsing and joining may discard local downloads.
        granule_dir = os.path.join(self.download_path, granule_key)
        if os.path.exists(granule_dir):
            shutil.rmtree(granule_dir, ignore_errors=True)
        return granule_key, joined

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
        for product, file in granules:
            data = granule_parser.parse_h5_file(file, product, data_info=self.data_info)
            if data is None:
                raise ValueError(f"Granule {granule_key}: Failed to parse {product}.")
            if not data.empty and "shot_number" not in data:
                raise ValueError(
                    f"Granule {granule_key}: {product} has no shot_number."
                )
            data_dict[product] = data
        return data_dict

    @staticmethod
    def _join_dfs(
        df_dict: Dict[str, pd.DataFrame],
        granule_key: str,
        products: Optional[List[GediProduct]] = None,
    ) -> pd.DataFrame:
        """Inner join required products, distinguishing empty data from errors."""
        products = products if products is not None else list(GediProduct)
        for product in products:
            if product.value not in df_dict:
                raise ValueError(
                    f"Granule {granule_key}: Missing parsed product {product.value}."
                )
            frame = df_dict[product.value]
            if not frame.empty:
                if "shot_number" not in frame or frame.shot_number.isna().any():
                    raise ValueError(
                        f"Granule {granule_key}: Invalid shot_number in {product.value}."
                    )
                if frame.shot_number.dtype.kind not in "iu":
                    raise ValueError(
                        f"Granule {granule_key}: shot_number must be an integer."
                    )
                if frame.shot_number.duplicated().any():
                    raise ValueError(
                        f"Granule {granule_key}: Duplicate shot_number in {product.value}."
                    )
        if any(df_dict[p.value].empty for p in products):
            return pd.DataFrame()
        df = df_dict[products[0].value]
        for product in products[1:]:
            other = df_dict[product.value]
            columns = [c for c in other if c == "shot_number" or c not in df]
            df = df.merge(
                other[columns], on="shot_number", how="inner", validate="one_to_one"
            )
        return df
