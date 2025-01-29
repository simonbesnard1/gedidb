# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import geopandas as gpd
from shapely.geometry import MultiPolygon
from shapely.geometry.polygon import orient
from shapely.geometry.base import BaseGeometry  # for typing only
import dateutil.parser
import numpy as np
import pandas as pd
from typing import Union
from collections import defaultdict
from datetime import datetime, timedelta

# Maximum number of coordinates allowed for NASA CMR API
MAX_CMR_COORDS = 4999


class DetailError(Exception):
    """
    Exception raised when the shape exceeds the maximum number of points allowed by NASA's API.

    Attributes:
        n_coords (int): Number of coordinates in the shape that caused the error.
    """

    def __init__(self, n_coords: int):
        self.n_coords = n_coords
        super().__init__(
            f"Shape contains {n_coords} coordinates, exceeding the limit of {MAX_CMR_COORDS}."
        )


def _count_coordinates(geom: BaseGeometry) -> int:
    """
    Count the total number of coordinates in a given geometry object.

    Args:
        geom (BaseGeometry): A shapely geometry object (Polygon or MultiPolygon).

    Returns:
        int: Total number of exterior coordinates in the geometry.
    """
    if isinstance(geom, MultiPolygon):
        return sum([len(part.exterior.coords) for part in geom.geoms])
    return len(geom.exterior.coords)


def check_and_format_shape(
    shp: gpd.GeoDataFrame, simplify: bool = False
) -> gpd.GeoSeries:
    """
    Validate and format a GeoDataFrame's geometry for NASA's CMR API.

    This function checks if the shape has more than the allowed number of points,
    simplifies the shape if necessary, and ensures proper polygon orientation.

    Args:
        shp (gpd.GeoDataFrame): The input GeoDataFrame containing a single polygon or multipolygon.
        simplify (bool): Whether to simplify the shape to a convex hull if it exceeds the allowed point limit.

    Raises:
        ValueError: If more than one polygon is passed in the GeoDataFrame.
        DetailError: If the shape has too many points and cannot be simplified.

    Returns:
        gpd.GeoSeries: A formatted GeoSeries with a valid geometry for the CMR API.
    """
    if len(shp) > 1:
        raise ValueError("Only one polygon at a time is supported.")

    geom = shp.geometry.values[0]
    n_coords = _count_coordinates(geom)

    # Check if the shape exceeds the maximum coordinate limit
    if n_coords > MAX_CMR_COORDS:
        if not simplify:
            raise DetailError(n_coords)

        logging.info(f"Simplifying shape with {n_coords} coordinates to convex hull.")
        geom = geom.convex_hull
        n_coords = _count_coordinates(geom)
        logging.info(f"Simplified shape to {n_coords} coordinates.")

        if n_coords > MAX_CMR_COORDS:
            raise DetailError(n_coords)

    # Ensure proper orientation for polygons and multipolygons
    if isinstance(geom, MultiPolygon):
        return gpd.GeoSeries([orient(p) for p in geom.geoms], crs=shp.crs)
    else:
        return gpd.GeoSeries(orient(geom), crs=shp.crs)


def _datetime_to_timestamp_days(dt: Union[str, np.datetime64]) -> int:
    """
    Convert an ISO8601 datetime string (e.g., "2018-01-01T00:00:00Z") or numpy.datetime64
    to a timestamp in days since epoch (UTC).

    Parameters:
    ----------
    dt : Union[str, np.datetime64]
        The ISO8601 datetime string in UTC (e.g., "2018-01-01T00:00:00Z") or numpy.datetime64.

    Returns:
    --------
    int
        The timestamp in days since epoch.
    """
    if isinstance(dt, np.datetime64):
        # Convert datetime64 to days since epoch
        timestamp = int(
            (dt - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "D")
        )
    else:
        # Parse ISO string and convert to days since epoch
        dt = dateutil.parser.isoparse(dt).replace(tzinfo=dateutil.tz.UTC)  # Ensure UTC
        timestamp = int(
            dt.timestamp() // (86400)
        )  # Convert to days (86400 seconds/day)
    return timestamp


def _timestamp_to_datetime(days: np.ndarray) -> np.ndarray:
    """
    Convert an array of timestamps in days since epoch to np.datetime64 with daily precision.

    Parameters:
    ----------
    days : np.ndarray
        Array of timestamps in days since the epoch.

    Returns:
    --------
    np.ndarray
        Array of datetime64[D] values in UTC.
    """
    # Convert days since epoch to datetime64 with daily precision
    return (np.datetime64("1970-01-01", "D") + days).astype("datetime64[ns]")


def convert_to_days_since_epoch(
    timestamps: Union[pd.DatetimeIndex, pd.Series, list]
) -> pd.Series:
    """
    Convert nanosecond-precision timestamps to daily timestamps in days since the Unix epoch (1970-01-01).

    Parameters:
    ----------
    timestamps : Union[pd.DatetimeIndex, pd.Series, list]
        A sequence of timestamps (e.g., DatetimeIndex, pandas Series, or list of ISO8601 strings).

    Returns:
    --------
    pd.Series
        A pandas Series representing the number of days since the Unix epoch.
    """
    # Ensure timestamps are converted to pandas DatetimeIndex
    timestamps = pd.to_datetime(timestamps, utc=True)

    # Define the Unix epoch
    epoch = pd.Timestamp("1970-01-01", tz="UTC")

    # Truncate timestamps to daily resolution and calculate days since epoch
    days_since_epoch = (timestamps.floor("D") - epoch).days

    return days_since_epoch


def _temporal_tiling(
    unprocessed_cmr_data: dict, time_granularity: str = "weekly"
) -> dict:
    """
    Separate the granules into temporal tiles by either daily or weekly.

    Parameters:
    ----------
    unprocessed_cmr_data : dict
        Dictionary of unprocessed granules from the CMR API.
    time_granularity : str
        A string that defines the granularity of temporal tiling. Can be 'daily' or 'weekly'.
        Default is 'weekly'.

    Returns:
    --------
    dict
        Nested dictionary where the outer keys represent temporal tiles, and inner keys are granule IDs.
    """

    grouped_data = defaultdict(lambda: defaultdict(list))

    def get_week_start(date: datetime) -> datetime:
        """Get the start of the week (Monday)."""
        return date - timedelta(days=date.weekday())

    def get_day_start(date: datetime) -> datetime:
        """Get the start of the day (midnight)."""
        return date.replace(hour=0, minute=0, second=0, microsecond=0)

    for granule_id, entries in unprocessed_cmr_data.items():
        for entry in entries:
            url, product, date_str = entry
            start_date = datetime.fromisoformat(date_str.replace("Z", ""))

            if time_granularity == "weekly":
                week_start = get_week_start(start_date)
                year_week = f"{week_start.year}-W{week_start.strftime('%U')}"
                grouped_data[year_week][granule_id].append((url, product, date_str))

            elif time_granularity == "daily":
                day_start = get_day_start(start_date)
                day_key = day_start.date().isoformat()
                grouped_data[day_key][granule_id].append((url, product, date_str))

    return grouped_data
