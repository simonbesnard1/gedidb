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
from gedidb.utils.constants import GediProduct

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
        missing_product = [level for level, data in granules if data is None]

        if missing_product:
            logger.warning(
                f"Granule {granule_key} was not processed: Missing HDF5 file(s) for levels: {missing_product}"
            )
            return None, None

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
            logger.error(
                f"Granule {granule_key} was not processed: Processing failed with error: {e}"
            )
            return None, None

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
            for product, file in granules:
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
        Join GEDI product DataFrames on 'shot_number' with this policy:
          - Required products: L2A, L2B, L4A, L4C must exist and be non-empty.
          - Base = inner(L2A, L2B) on 'shot_number' (one-to-one after de-dup).
          - Then left-join L4A and L4C to the base, so L4 data is added where available.

        Additional behavior:
          - Enforces consistent 'string' dtype for 'shot_number'.
          - Drops duplicates per product on 'shot_number' (keeps first).
          - Handles column collisions with suffixes.
          - Returns None if any required product missing/empty or if base ends empty.

        """
        key = "shot_number"
        req = [GediProduct.L2A, GediProduct.L2B, GediProduct.L4A, GediProduct.L4C]

        missing = []
        for p in req:
            code = p.value
            if code not in df_dict or df_dict[code] is None or df_dict[code].empty:
                missing.append(code)

        if missing:
            return None

        def _prep(df: pd.DataFrame, product_name: str) -> pd.DataFrame:
            """Prepare DataFrame with validation and deduplication."""
            if key not in df.columns:
                raise KeyError(
                    f"[{granule_key}] Product {product_name} missing join key '{key}'"
                )

            # Single copy, then modify
            out = df.copy()
            out[key] = out[key].astype("string")

            out = out.drop_duplicates(subset=[key], keep="first")

            return out

        # Prepare all DataFrames
        try:
            l2a = _prep(df_dict[GediProduct.L2A.value], "L2A")
            l2b = _prep(df_dict[GediProduct.L2B.value], "L2B")
            l4a = _prep(df_dict[GediProduct.L4A.value], "L4A")
            l4c = _prep(df_dict[GediProduct.L4C.value], "L4C")
        except Exception as e:
            logger.error(f"[{granule_key}] Preparation failed: {e}")
            return None

        # STEP 1: Build base via inner join L2A ⨝ L2B
        overlap_l2 = l2a.columns.intersection(l2b.columns).difference([key])
        if len(overlap_l2) > 0:

            # Use dictionary comprehension (slightly faster)
            l2b = l2b.rename(columns={c: f"{c}_L2B" for c in overlap_l2})

        try:
            base = l2a.merge(
                l2b, on=key, how="inner", validate="one_to_one", copy=False
            )
        except Exception as e:
            logger.error(f"[{granule_key}] L2A⨝L2B merge failed: {e}")
            return None

        if base.empty:
            return None

        # STEP 2: LEFT-join L4A
        overlap = base.columns.intersection(l4a.columns).difference([key])
        if len(overlap) > 0:

            l4a = l4a.rename(columns={c: f"{c}_L4A" for c in overlap})

        try:
            base = base.merge(
                l4a, on=key, how="left", validate="one_to_one", copy=False
            )

        except Exception as e:
            logger.error(f"[{granule_key}] L4A left-join failed: {e}")
            return None

        # STEP 3: LEFT-join L4C
        overlap = base.columns.intersection(l4c.columns).difference([key])
        if len(overlap) > 0:

            l4c = l4c.rename(columns={c: f"{c}_L4C" for c in overlap})

        try:
            base = base.merge(
                l4c, on=key, how="left", validate="one_to_one", copy=False
            )

        except Exception as e:
            logger.error(f"[{granule_key}] L4C left-join failed: {e}")
            return None

        # Build column list without repeated list operations
        l2a_cols = [c for c in l2a.columns if c != key]
        other_cols = [c for c in base.columns if c not in ([key] + l2a_cols)]
        base = base[[key] + l2a_cols + other_cols]

        return base if not base.empty else None
