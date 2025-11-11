# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import time
import json
from copy import deepcopy
import logging

import numpy as np
import pandas as pd
import geopandas as gpd

from gedidb.providers.tiledb_provider import TileDBProvider
from gedidb.utils.geo_processing import (
    _datetime_to_timestamp_days,
)


logger = logging.getLogger(__name__)


def _parse_time(t):
    """
    Parse time input (string, datetime, pandas Timestamp, etc.)
    into TileDB-compatible timestamp in days (float).
    """
    if t is None:
        return None
    return _datetime_to_timestamp_days(np.datetime64(t))


def load_geometry_and_bbox(geojson_path: str):
    gdf = gpd.read_file(geojson_path)
    if gdf.empty:
        raise ValueError(f"No geometry found in {geojson_path}")

    # Dissolve all to a single geometry, then use bounds as query bbox
    geom = gdf.unary_union
    minx, miny, maxx, maxy = geom.bounds

    # Note: array expects [lat_min:lat_max, lon_min:lon_max]
    bbox = {
        "lon_min": float(minx),
        "lon_max": float(maxx),
        "lat_min": float(miny),
        "lat_max": float(maxy),
    }

    return gdf, bbox


def estimate_bytes(df: pd.DataFrame) -> int:
    """
    Rough but consistent: sum column-wise memory usage.
    """
    if df is None or df.empty:
        return 0
    return int(df.memory_usage(deep=True).sum())


def run_s3_benchmarks(
    geojson_path: str,
    start_time,
    end_time,
    variables,
    s3_bucket: str,
    url: str,
    region: str,
    credentials: dict = None,
    configs: dict = None,
    use_polygon_filter: bool = True,
) -> pd.DataFrame:
    """
    Run read-performance benchmarks for different S3 config variants.

    Parameters
    ----------
    geojson_path : str
        Path to GeoJSON defining AOI.
    start_time, end_time :
        Time range (anything pandas can parse).
    variables : list of str
        TileDB attribute names to query.
    s3_bucket, url, region : str
        S3 location & endpoint.
    credentials : dict, optional
        AWS-style credentials if needed.
    configs : dict, optional
        Mapping: name -> dict of TileDB S3 config overrides.
        If None, a default set of sensible variants is used.
    use_polygon_filter : bool
        If True, apply polygon filtering inside bbox.

    Returns
    -------
    pd.DataFrame
        One row per config, including timing and throughput stats.
    """

    geometry, bbox = load_geometry_and_bbox(geojson_path)
    start_time = _parse_time(start_time)
    end_time = _parse_time(end_time)

    if configs is None:
        # A small search space of meaningful variants (not random chaos)
        configs = {
            "baseline": {},
            "more_threads": {
                "sm.compute_concurrency_level": "32",
                "sm.io_concurrency_level": "32",
                "sm.num_reader_threads": "32",
                "sm.num_tiledb_threads": "32",
                "vfs.s3.max_parallel_ops": "32",
            },
            "bigger_parts": {
                "vfs.s3.multipart_part_size": str(64 * 1024**2),  # 64 MB
            },
            "smaller_parts": {
                "vfs.s3.multipart_part_size": str(8 * 1024**2),  # 8 MB
            },
            "larger_cache": {
                "sm.tile_cache_size": str(8 * 1024**3),  # 8 GB
            },
        }

    results = []

    for name, overrides in configs.items():
        cfg = deepcopy(overrides)

        # Init provider with this config
        provider = TileDBProvider(
            storage_type="s3",
            s3_bucket=s3_bucket,
            url=url,
            region=region,
            credentials=credentials,
            s3_config_overrides=cfg,
        )

        # --- Warmup run (populate caches, stabilize connection)
        try:
            _ = provider.query_dataframe(
                variables=variables,
                lat_min=bbox["lat_min"],
                lat_max=bbox["lat_max"],
                lon_min=bbox["lon_min"],
                lon_max=bbox["lon_max"],
                start_time=start_time,
                end_time=end_time,
                geometry=geometry if use_polygon_filter else None,
                use_polygon_filter=use_polygon_filter,
            )
        except Exception as e:
            logger.error(f"[{name}] Warmup failed: {e}")
            provider.close()
            results.append(
                {
                    "config": name,
                    "ok": False,
                    "error": str(e),
                    "rows": 0,
                    "bytes": 0,
                    "time_s": np.nan,
                    "rows_per_s": np.nan,
                    "mb_per_s": np.nan,
                    "config_overrides": json.dumps(cfg),
                }
            )
            continue

        # --- Timed run
        t0 = time.perf_counter()
        try:
            df = provider.query_dataframe(
                variables=variables,
                lat_min=bbox["lat_min"],
                lat_max=bbox["lat_max"],
                lon_min=bbox["lon_min"],
                lon_max=bbox["lon_max"],
                start_time=start_time,
                end_time=end_time,
                geometry=geometry if use_polygon_filter else None,
                use_polygon_filter=use_polygon_filter,
            )
        except Exception as e:
            t1 = time.perf_counter()
            logger.error(f"[{name}] Timed run failed: {e}")
            provider.close()
            results.append(
                {
                    "config": name,
                    "ok": False,
                    "error": str(e),
                    "rows": 0,
                    "bytes": 0,
                    "time_s": t1 - t0,
                    "rows_per_s": np.nan,
                    "mb_per_s": np.nan,
                    "config_overrides": json.dumps(cfg),
                }
            )
            continue

        t1 = time.perf_counter()
        provider.close()

        if df is None or df.empty:
            rows = 0
            total_bytes = 0
        else:
            rows = int(len(df))
            total_bytes = estimate_bytes(df)

        dt = max(t1 - t0, 1e-9)
        mb = total_bytes / (1024**2) if total_bytes > 0 else 0.0

        results.append(
            {
                "config": name,
                "ok": True,
                "error": "",
                "rows": rows,
                "bytes": total_bytes,
                "time_s": dt,
                "rows_per_s": rows / dt if rows > 0 else 0.0,
                "mb_per_s": mb / dt if mb > 0 else 0.0,
                "config_overrides": json.dumps(cfg),
            }
        )

    return pd.DataFrame(results).sort_values("mb_per_s", ascending=False)
