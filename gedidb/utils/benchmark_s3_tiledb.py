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
    check_and_format_shape,
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

    return gdf


def estimate_bytes(df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    return int(df.memory_usage(deep=True).sum())


def default_s3_benchmark_configs():
    """
    Curated set of S3/TileDB configs to probe key performance dimensions:
    - concurrency
    - part size
    - cache size
    - addressing mode
    """
    return {
        # Control
        "baseline": {},
        # 1) Concurrency variants
        "more_threads_16": {
            "sm.compute_concurrency_level": "16",
            "sm.io_concurrency_level": "16",
            "sm.num_reader_threads": "16",
            "sm.num_tiledb_threads": "16",
            "vfs.s3.max_parallel_ops": "16",
        },
        "more_threads_32": {
            "sm.compute_concurrency_level": "32",
            "sm.io_concurrency_level": "32",
            "sm.num_reader_threads": "32",
            "sm.num_tiledb_threads": "32",
            "vfs.s3.max_parallel_ops": "32",
        },
        "single_thread": {
            "sm.compute_concurrency_level": "1",
            "sm.io_concurrency_level": "1",
            "sm.num_reader_threads": "1",
            "sm.num_tiledb_threads": "1",
            "vfs.s3.max_parallel_ops": "4",
        },
        # 2) Multipart size variants
        "part_8mb": {
            "vfs.s3.multipart_part_size": str(8 * 1024**2),
        },
        "part_64mb": {
            "vfs.s3.multipart_part_size": str(64 * 1024**2),
        },
        "part_128mb": {
            "vfs.s3.multipart_part_size": str(128 * 1024**2),
        },
        # 3) Cache size variants
        "cache_4gb": {
            "sm.tile_cache_size": str(4 * 1024**3),
        },
        "cache_8gb": {
            "sm.tile_cache_size": str(8 * 1024**3),
        },
        # 4) Combined "aggressive" profile
        "high_parallel_high_cache": {
            "sm.compute_concurrency_level": "32",
            "sm.io_concurrency_level": "32",
            "sm.num_reader_threads": "32",
            "sm.num_tiledb_threads": "32",
            "vfs.s3.max_parallel_ops": "32",
            "sm.tile_cache_size": str(8 * 1024**3),
            "vfs.s3.multipart_part_size": str(64 * 1024**2),
        },
        # 5) Path-style addressing toggle (just to check endpoint quirks)
        "path_style_addressing": {
            "vfs.s3.use_virtual_addressing": "false",
        },
    }


def run_s3_benchmarks(
    geometry,
    start_time,
    end_time,
    variables,
    s3_bucket: str,
    url: str,
    region: str,
    credentials: dict = None,
    configs: dict = None,
    use_polygon_filter: bool = False,
) -> pd.DataFrame:
    """
    Run read-performance benchmarks for different S3 config variants.
    """

    geometry = check_and_format_shape(geometry, simplify=True)
    lon_min, lat_min, lon_max, lat_max = geometry.total_bounds

    start_ts = _parse_time(start_time)
    end_ts = _parse_time(end_time)

    if configs is None:
        configs = default_s3_benchmark_configs()

    results = []

    for name, overrides in configs.items():
        cfg = deepcopy(overrides)
        logger.info(f"[{name}] Testing config: {cfg}")

        provider = TileDBProvider(
            storage_type="s3",
            s3_bucket=s3_bucket,
            url=url,
            region=region,
            credentials=credentials,
            s3_config_overrides=cfg,
        )

        # ---- Timed run
        t0 = time.perf_counter()
        df = provider.query_dataframe(
            variables=variables,
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            start_time=start_ts,
            end_time=end_ts,
            geometry=geometry,
        )
        t1 = time.perf_counter()
        provider.close()

        rows = int(len(df)) if df is not None else 0
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

    df_res = pd.DataFrame(results)
    df_res = df_res.sort_values(["ok", "mb_per_s"], ascending=[False, False])
    return df_res
