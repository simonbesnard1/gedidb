# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import logging
import tiledb
import numpy as np
import pandas as pd
from typing import Optional, List, Dict, Tuple

# Configure the logger
logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]


class TileDBProvider:
    """
    A base provider class for managing low-level interactions with TileDB arrays for GEDI data.
    """

    def __init__(
        self,
        storage_type: str = "local",
        s3_bucket: Optional[str] = None,
        local_path: Optional[str] = "./",
        url: Optional[str] = None,
        region: str = "eu-central-1",
        credentials: Optional[dict] = None,
        n_workers: int = 5,
    ):
        self.n_workers = n_workers

        # Validate storage_type
        if not storage_type or not isinstance(storage_type, str):
            raise ValueError("The 'storage_type' argument must be a non-empty string.")

        storage_type = storage_type.lower()

        if storage_type == "s3":
            # Validate s3_bucket for S3 storage type
            if not s3_bucket:
                raise ValueError("The 's3_bucket' must be provided when 'storage_type' is set to 's3'.")
            self.scalar_array_uri = f"s3://{s3_bucket}/array_uri"
            self.ctx = self._initialize_s3_context(credentials, url, region)

        elif storage_type == "local":
            # Validate local_path for local storage type
            if not local_path:
                raise ValueError("The 'local_path' must be provided when 'storage_type' is set to 'local'.")
            self.scalar_array_uri = os.path.join(local_path, "array_uri")
            self.ctx = self._initialize_local_context()

        else:
            # Raise an error for invalid storage_type
            raise ValueError(f"Invalid 'storage_type': {storage_type}. Must be 'local' or 's3'.")

    def _initialize_s3_context(
        self, credentials: Optional[dict], url: str, region: str
    ) -> tiledb.Ctx:
        config = {
            "vfs.s3.endpoint_override": url,
            "vfs.s3.region": region,
            "py.init_buffer_bytes": "512000000",  # Increase buffer size
        }
        
        # Add credentials if provided
        if credentials:
            config.update({
                "vfs.s3.aws_access_key_id": credentials.get("AccessKeyId", ""),
                "vfs.s3.aws_secret_access_key": credentials.get("SecretAccessKey", "")
            })
        
        return tiledb.Ctx(config)


    def _initialize_local_context(self) -> tiledb.Ctx:
        return tiledb.Ctx(
            {
                "py.init_buffer_bytes": "512000000",  # Increase buffer size
            }
        )

    def get_available_variables(self) -> pd.DataFrame:
        """
        Retrieve metadata for available variables in the scalar TileDB array.
        """
        try:
            with tiledb.open(
                self.scalar_array_uri, mode="r", ctx=self.ctx
            ) as scalar_array:
                metadata = {
                    k: scalar_array.meta[k]
                    for k in scalar_array.meta
                    if not k.startswith("granule_") and "array_type" not in k
                }

                organized_metadata = {}
                for key, value in metadata.items():
                    var_name, attr_type = key.split(".", 1)
                    if var_name not in organized_metadata:
                        organized_metadata[var_name] = {}
                    organized_metadata[var_name][attr_type] = value

                return pd.DataFrame.from_dict(organized_metadata, orient="index")
        except Exception as e:
            logger.error(f"Failed to retrieve variables from TileDB: {e}")
            raise

    def _query_array(
        self,
        array_uri: str,
        variables: List[str],
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_time: Optional[np.datetime64],
        end_time: Optional[np.datetime64],
        **filters: Dict[str, str],
    ) -> Tuple[Optional[Dict[str, np.ndarray]], Dict[str, List[str]]]:
        """
        Execute a query on a TileDB array with spatial, temporal, and additional filters.
        """
        try:
            with tiledb.open(array_uri, mode="r", ctx=self.ctx) as array:
                attr_list = []
                profile_vars = {}

                for var in variables:
                    if f"{var}.profile_length" in array.meta:
                        profile_length = array.meta[f"{var}.profile_length"]
                        profile_attrs = [
                            f"{var}_{i}" for i in range(1, profile_length + 1)
                        ]
                        attr_list.extend(profile_attrs)
                        profile_vars[var] = profile_attrs
                    else:
                        attr_list.append(var)

                # Construct the quality filter condition
                cond_list = []
                for key, condition in filters.items():
                    # Handle range conditions like ">= 0.9 and <= 1.0"
                    if "and" in condition:
                        parts = condition.split("and")
                        for part in parts:
                            cond_list.append(f"{key} {part.strip()}")
                    else:
                        cond_list.append(f"{key} {condition.strip()}")
                cond_string = " and ".join(cond_list) if cond_list else None
                query = array.query(attrs=attr_list, cond=cond_string)
                data = query.multi_index[
                    lat_min:lat_max, lon_min:lon_max, start_time:end_time
                ]

                if len(data["shot_number"]) == 0:
                    return None, profile_vars

                return data, profile_vars
        except Exception as e:
            logger.error(f"Error querying TileDB array '{array_uri}': {e}")
            raise
