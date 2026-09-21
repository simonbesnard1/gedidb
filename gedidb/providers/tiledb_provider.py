# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import logging
import os
import re
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from shapely import intersects_xy
import tiledb

logger = logging.getLogger(__name__)

DEFAULT_DIMS = ["shot_number"]

# Pre-sorted once: longest operators first so "==" is checked before "="
_SORTED_OPS: Tuple[str, ...] = tuple(
    sorted({">=", "<=", "==", "!=", ">", "<", "="}, key=len, reverse=True)
)


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
        s3_config_overrides: Optional[Dict[str, str]] = None,
        config_overrides: Optional[Dict[str, str]] = None,
    ):
        if not storage_type or not isinstance(storage_type, str):
            raise ValueError("The 'storage_type' argument must be a non-empty string.")

        self.storage_type = storage_type.lower()
        self.s3_config_overrides = s3_config_overrides or {}
        self.config_overrides = config_overrides or {}

        if self.storage_type == "s3":
            if not s3_bucket:
                raise ValueError(
                    "The 's3_bucket' must be provided when 'storage_type' is set to 's3'."
                )
            if not url:
                raise ValueError(
                    "The 'url' (S3 endpoint) must be provided when 'storage_type' is 's3'."
                )

            self.scalar_array_uri = f"s3://{s3_bucket}/array_uri"
            self.ctx = self._initialize_s3_context(credentials, url, region)

        elif self.storage_type == "local":
            if not local_path:
                raise ValueError(
                    "The 'local_path' must be provided when 'storage_type' is set to 'local'."
                )
            self.scalar_array_uri = os.path.join(local_path, "array_uri")
            self.ctx = self._initialize_local_context()

        else:
            raise ValueError(
                f"Invalid 'storage_type': {storage_type}. Must be 'local' or 's3'."
            )

        self._schema_cache = None
        self._metadata_cache = None
        self._array_handle = None

    def _initialize_s3_context(
        self, credentials: Optional[dict], url: str, region: str
    ) -> tiledb.Ctx:

        cores = os.cpu_count() or 8
        max_reader_threads = min(cores * 4, 64)
        max_s3_ops = min(cores * 8, 256)

        base_config = {
            # Endpoint / region
            "vfs.s3.endpoint_override": url,
            "vfs.s3.region": region,
            "vfs.s3.scheme": "https",
            "vfs.s3.use_virtual_addressing": "false",
            # Parallel S3 I/O
            "vfs.s3.max_parallel_ops": str(max_s3_ops),
            # Larger part size reduces S3 GET count for big spatial tiles
            "vfs.s3.multipart_part_size": str(128 * 1024**2),  # 128 MB
            # Timeouts
            "vfs.s3.connect_timeout_ms": "60000",  # 60 s
            "vfs.s3.request_timeout_ms": "600000",  # 10 min
            # Threading
            "sm.compute_concurrency_level": str(max_reader_threads),
            "sm.io_concurrency_level": str(max_reader_threads),
            "sm.num_reader_threads": str(max_reader_threads),
            "sm.num_tiledb_threads": str(max_reader_threads),
            # Memory budgets — larger values allow TileDB to plan bigger
            # single-pass reads instead of breaking them into smaller chunks.
            "sm.memory_budget": str(256 * 1024**2),
            "sm.memory_budget_var": str(128 * 1024**2),
            "sm.mem.total_budget": str(1024**3),
            # Caches
            "py.init_buffer_bytes": str(1024**2),  # Per attribute buffer
            "sm.tile_cache_size": str(256 * 1024**2),
            # Misc
            "sm.enable_signal_handlers": "false",
        }

        if credentials:
            base_config.update(
                {
                    "vfs.s3.aws_access_key_id": credentials.get("AccessKeyId", ""),
                    "vfs.s3.aws_secret_access_key": credentials.get(
                        "SecretAccessKey", ""
                    ),
                    "vfs.s3.aws_session_token": credentials.get("SessionToken", ""),
                    "vfs.s3.no_sign_request": "false",
                }
            )
        else:
            base_config["vfs.s3.no_sign_request"] = "true"

        # Allow targeted overrides (for experiments)
        base_config.update(self.s3_config_overrides)
        base_config.update(self.config_overrides)

        return tiledb.Ctx(base_config)

    def _initialize_local_context(self) -> tiledb.Ctx:
        cores = os.cpu_count() or 8
        threads = str(min(cores * 4, 64))
        return tiledb.Ctx(
            {
                "py.init_buffer_bytes": str(1024**2),  # Per attribute buffer
                "sm.tile_cache_size": str(256 * 1024**2),
                "sm.num_reader_threads": threads,
                "sm.num_tiledb_threads": threads,
                "sm.compute_concurrency_level": threads,
                "sm.io_concurrency_level": threads,
                **self.config_overrides,
            }
        )

    def _get_array(self) -> tiledb.Array:
        """
        Return a persistently open read handle to the scalar array.
        Opening a TileDB array on S3 requires multiple metadata round-trips;
        reusing the same handle across calls eliminates that overhead.
        """
        if self._array_handle is None or not self._array_handle.isopen:
            self._array_handle = tiledb.open(
                self.scalar_array_uri, mode="r", ctx=self.ctx
            )
        return self._array_handle

    def get_available_variables(self) -> pd.DataFrame:
        """
        Retrieve metadata for available variables in the scalar TileDB array.
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        try:
            # Reuse the persistent handle — avoids a second S3 open/close.
            scalar_array = self._get_array()
            metadata = {
                k: scalar_array.meta[k]
                for k in scalar_array.meta
                if not k.startswith(("granule_", "gedidb.")) and "array_type" not in k
            }

            organized_metadata = defaultdict(dict)
            for key, value in metadata.items():
                if "." in key:
                    var_name, attr_type = key.split(".", 1)
                    organized_metadata[var_name][attr_type] = value

            result = pd.DataFrame.from_dict(dict(organized_metadata), orient="index")
            self._metadata_cache = result
            return result

        except Exception as e:
            logger.error(f"Failed to retrieve variables from TileDB: {e}")
            raise

    def _build_profile_attrs(
        self,
        variables: List[str],
        array_meta,
    ) -> Tuple[List[str], Dict[str, List[str]], Dict[str, str]]:
        """
        Build attribute list and profile variable mapping.

        Supports single-label selection via ``"var:label"`` syntax
        (e.g. ``"rh:98"``). Only the matching TileDB attribute is fetched;
        the result column is renamed to ``{var}_p{label}``
        (e.g. ``rh_p98``).

        Returns
        -------
        attr_list : List[str]
            Flat list of TileDB attribute names to request.
        profile_vars : Dict[str, List[str]]
            Mapping of variable name → expanded column names for full profiles.
        scalar_renames : Dict[str, str]
            Mapping of raw TileDB attr name → user-facing name for
            single-label selections (e.g. ``rh_99`` → ``rh_p98``).
        """
        attr_list: List[str] = []
        profile_vars: Dict[str, List[str]] = {}
        scalar_renames: Dict[str, str] = {}

        for var in variables:
            # ── Single-label selection: "rh:98" ──────────────────────────────
            if ":" in var:
                base_var, label_str = var.split(":", 1)
                label_val = int(label_str)
                labels_raw = array_meta.get(f"{base_var}.profile_labels")
                if labels_raw is None:
                    raise ValueError(
                        f"Variable '{base_var}' has no label metadata. "
                        f"Cannot select by label '{label_val}'."
                    )
                labels = [int(v) for v in labels_raw.split(",")]
                if len(labels) != array_meta.get(
                    f"{base_var}.profile_length", len(labels)
                ) or len(set(labels)) != len(labels):
                    raise ValueError(
                        f"Invalid profile label metadata for '{base_var}'."
                    )
                if label_val not in labels:
                    raise ValueError(
                        f"Label '{label_val}' not found in '{base_var}'. "
                        f"Available labels: {labels}."
                    )
                idx = labels.index(label_val) + 1  # TileDB attrs are 1-indexed
                tiledb_attr = f"{base_var}_{idx}"
                attr_list.append(tiledb_attr)
                scalar_renames[tiledb_attr] = f"{base_var}_p{label_val}"
                continue

            profile_key = f"{var}.profile_length"
            if profile_key in array_meta:
                profile_attrs = [
                    f"{var}_{i}" for i in range(1, array_meta[profile_key] + 1)
                ]
                attr_list.extend(profile_attrs)
                profile_vars[var] = profile_attrs
            else:
                # Guard against "rh_98"-style direct attribute access for profile
                # variables. TileDB attrs are 1-indexed, so rh_98 = label 97, not
                # label 98. Force users through the "var:label" syntax instead.
                m = re.fullmatch(r"([a-zA-Z][a-zA-Z0-9_]*)_(\d+)", var)
                if m:
                    base_var = m.group(1)
                    if array_meta.get(f"{base_var}.profile_labels") is not None:
                        labels_raw = array_meta[f"{base_var}.profile_labels"]
                        labels = [int(v) for v in labels_raw.split(",")]
                        index = int(m.group(2)) - 1
                        if not 0 <= index < len(labels):
                            raise ValueError(f"Profile index out of range: {var}")
                        raise ValueError(
                            f"'{var}' directly accesses a 1-indexed TileDB attribute "
                            f"and will return the wrong profile point. "
                            f"Use the 'var:label' syntax instead, e.g. "
                            f"'{base_var}:{labels[int(m.group(2)) - 1]}' for the "
                            f"equivalent label. Available labels: {labels}."
                        )
                attr_list.append(var)

        return list(dict.fromkeys(attr_list)), profile_vars, scalar_renames

    @staticmethod
    def _apply_scalar_renames(data, profile_vars, renames):
        data = dict(data)
        components = {col for columns in profile_vars.values() for col in columns}
        for source, target in renames.items():
            data[target] = data[source]
            if source not in components:
                del data[source]
        return data

    def _build_condition_string(self, filters: Dict[str, str]) -> Optional[str]:
        """Validate comparison expressions; malformed filters must never be ignored."""
        groups = []
        literal = (
            r"(?:[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?|'[^']*'|\"[^\"]*\")"
        )
        comparison = re.compile(rf"^\s*(>=|<=|==|!=|>|<|=)\s*({literal})\s*$")
        for key, expression in filters.items():
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) or not isinstance(
                expression, str
            ):
                raise ValueError(f"Invalid quality filter: {key}={expression!r}")
            # Quoted values remain intact, including case and the words 'and'/'or'.
            tokens = re.split(
                r"\s+(and|or)\s+(?=[<>=!])", expression, flags=re.IGNORECASE
            )
            clauses = []
            for i, token in enumerate(tokens):
                if i % 2:
                    clauses.append(token.lower())
                    continue
                match = comparison.fullmatch(token)
                if match is None:
                    raise ValueError(f"Invalid quality filter: {key}={expression!r}")
                op, value = match.groups()
                clauses.append(f"{key} {'==' if op == '=' else op} {value}")
            groups.append("(" + " ".join(clauses) + ")")
        return " and ".join(groups) or None

    def _filter_by_polygon(
        self,
        data: Dict[str, np.ndarray],
        geometry,
    ) -> Dict[str, np.ndarray]:
        """
        Filter query results by irregular polygon using vectorized operations.

        Parameters
        ----------
        data : Dict[str, np.ndarray]
            Query results from TileDB.
        geometry : gpd.GeoDataFrame or shapely geometry
            Polygon(s) to filter by. Accepts either a GeoDataFrame or a
            pre-computed shapely geometry (e.g. passed from ``query_data``
            to avoid computing the union twice).

        Returns
        -------
        Dict[str, np.ndarray]
            Filtered data containing only points within the polygon.
        """
        if data is None or len(data.get("shot_number", [])) == 0:
            return data

        lons = data["longitude"]
        lats = data["latitude"]

        # Resolve to a single shapely geometry (caller may pass a precomputed union)
        if hasattr(geometry, "union_all"):
            geom = (
                geometry.union_all() if len(geometry) > 1 else geometry.geometry.iloc[0]
            )
        else:
            geom = geometry  # already a shapely geometry

        # Vectorized point-in-polygon test
        mask = intersects_xy(geom, lons, lats)

        filtered_data = {key: value[mask] for key, value in data.items()}

        logger.info(
            f"Polygon filter: {mask.sum()}/{len(mask)} points retained "
            f"({100 * mask.sum() / len(mask):.1f}%)"
        )

        return filtered_data

    def _query_array(
        self,
        variables: List[str],
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_time: Optional[np.datetime64] = None,
        end_time: Optional[np.datetime64] = None,
        geometry=None,
        return_coords: bool = True,
        use_polygon_filter: bool = False,
        quantization_factor: float = 1e6,
        **filters: Dict[str, str],
    ) -> Tuple[Optional[Dict[str, np.ndarray]], Dict[str, List[str]], Dict[str, str]]:
        """
        Execute a query on a TileDB array with spatial, temporal, and additional filters.

        ``geometry`` may be a GeoDataFrame or a pre-computed shapely geometry;
        both are accepted by ``_filter_by_polygon``.
        """
        try:
            array = self._get_array()

            attr_list, profile_vars, scalar_renames = self._build_profile_attrs(
                list(dict.fromkeys(variables + DEFAULT_DIMS)), array.meta
            )
            cond_string = self._build_condition_string(filters)

            query = array.query(
                attrs=attr_list,
                cond=cond_string,
                coords=return_coords,
                return_incomplete=False,
            )

            data = query.multi_index[
                lat_min:lat_max, lon_min:lon_max, start_time:end_time
            ]

            if not data or len(data.get("shot_number", [])) == 0:
                return None, profile_vars, scalar_renames

            if use_polygon_filter and geometry is not None:
                data = self._filter_by_polygon(data, geometry)
                if len(data.get("shot_number", [])) == 0:
                    return None, profile_vars, scalar_renames

            return data, profile_vars, scalar_renames

        except Exception as e:
            error_str = str(e)
            if "dimension 'time'" in error_str and "out of domain bounds" in error_str:
                match = re.search(r"domain bounds \[(\d+), (\d+)\]", error_str)
                if match:
                    epoch = np.datetime64("1970-01-01", "D")
                    t_min = str(epoch + np.timedelta64(int(match.group(1)), "D"))[:10]
                    t_max = str(epoch + np.timedelta64(int(match.group(2)), "D"))[:10]
                    raise ValueError(
                        f"Requested time range is outside the database bounds. "
                        f"Please provide a start_time and end_time within "
                        f"[{t_min}, {t_max}]."
                    ) from e
            logger.error(f"Error querying TileDB array '{self.scalar_array_uri}': {e}")
            raise

    def _get_tiledb_spatial_domain(self) -> Tuple[float, float, float, float]:
        """
        Retrieve the spatial domain (bounding box) from the TileDB array schema.
        """
        if self._schema_cache is not None:
            return self._schema_cache

        array = self._get_array()
        domain = array.schema.domain
        min_lon, max_lon = domain.dim(1).domain
        min_lat, max_lat = domain.dim(0).domain

        result = (min_lon, max_lon, min_lat, max_lat)
        self._schema_cache = result
        return result

    def query_dataframe(
        self,
        variables: List[str],
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        start_time: Optional[np.datetime64] = None,
        end_time: Optional[np.datetime64] = None,
        geometry=None,
        use_polygon_filter: bool = False,
        **filters: Dict[str, str],
    ) -> Optional[pd.DataFrame]:
        """
        Query TileDB and return results as a pandas DataFrame.
        """
        data, profile_vars, scalar_renames = self._query_array(
            variables,
            lat_min,
            lat_max,
            lon_min,
            lon_max,
            start_time,
            end_time,
            geometry=geometry,
            use_polygon_filter=use_polygon_filter,
            **filters,
        )

        if data is None:
            return None

        data = self._apply_scalar_renames(data, profile_vars, scalar_renames)
        df = pd.DataFrame(data)

        for var_name, profile_cols in profile_vars.items():
            if all(col in df.columns for col in profile_cols):
                df[var_name] = df[profile_cols].values.tolist()
                df = df.drop(columns=profile_cols)

        return df

    def iter_query_dataframe(
        self,
        variables,
        lat_min,
        lat_max,
        lon_min,
        lon_max,
        start_time=None,
        end_time=None,
        geometry=None,
        use_polygon_filter=False,
        **filters,
    ):
        """Yield DataFrame chunks without materializing a complete query.

        Bounds use WGS84 degrees and integer days since 1970-01-01, as in
        query_dataframe. Chunk sizes are controlled by TileDB buffer settings.
        """
        array = self._get_array()
        attrs, profiles, renames = self._build_profile_attrs(
            list(dict.fromkeys(variables + DEFAULT_DIMS)), array.meta
        )
        query = array.query(
            attrs=attrs,
            cond=self._build_condition_string(filters),
            coords=True,
            return_incomplete=True,
        )
        for data in query.multi_index[
            lat_min:lat_max, lon_min:lon_max, start_time:end_time
        ]:
            if not len(data["shot_number"]):
                continue
            if use_polygon_filter:
                if geometry is None:
                    raise ValueError("geometry is required for polygon filtering.")
                data = self._filter_by_polygon(data, geometry)
            if not len(data["shot_number"]):
                continue
            data = self._apply_scalar_renames(data, profiles, renames)
            frame = pd.DataFrame(data)
            for name, columns in profiles.items():
                frame[name] = frame[columns].values.tolist()
                frame = frame.drop(columns=columns)
            yield frame

    def close(self) -> None:
        """Close the persistent array handle and clear caches."""
        self._schema_cache = None
        self._metadata_cache = None
        if self._array_handle is not None:
            if self._array_handle.isopen:
                self._array_handle.close()
            self._array_handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
