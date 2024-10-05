# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.geometry.polygon import orient
from shapely.geometry.base import BaseGeometry  # for typing only

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
        super().__init__(f"Shape contains {n_coords} coordinates, exceeding the limit of {MAX_CMR_COORDS}.")

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

def calculate_zone(row):
    """
    Determine the geographical zone based on longitude and latitude bins.

    Parameters
    ----------
    row : pandas.Series or dict
        A row from a DataFrame or GeoDataFrame containing at least the following keys:
        - 'longitude_bin0': The longitude value (or bin), expected to be in the range [-180, 180].
        - 'latitude_bin0' : The latitude value (or bin), expected to be in the range [-90, 90].

    Returns
    -------
    str
        A string representing the geographical zone, which can be one of the following:
        - 'wh_north_polar'
        - 'wh_north_temperate'
        - 'wh_tropical'
        - 'wh_south_temperate'
        - 'wh_south_polar'
        - 'eh_north_polar'
        - 'eh_north_temperate'
        - 'eh_tropical'
        - 'eh_south_temperate'
        - 'eh_south_polar'

    Raises
    ------
    ValueError
        If the longitude or latitude values are outside the expected ranges or do not correspond to a defined zone.

    Notes
    -----
    - The Earth is divided into zones based on hemispheres and latitude bands.
    - Longitude ranges:
        - Western Hemisphere: -180 ≤ longitude < 0
        - Eastern Hemisphere: 0 ≤ longitude ≤ 180
    - Latitude bands:
        - North Polar:       60 ≤ latitude ≤ 90
        - North Temperate:   30 ≤ latitude < 60
        - Tropical:           0 ≤ latitude < 30
        - South Temperate:  -30 ≤ latitude < 0
        - South Polar:      -90 ≤ latitude < -30
    - The function uses inclusive lower bounds and exclusive upper bounds, except for the maximum values.

    Examples
    --------
    >>> row = {'longitude_bin0': -75, 'latitude_bin0': 45}
    >>> calculate_zone(row)
    'wh_north_temperate'

    >>> row = {'longitude_bin0': 120, 'latitude_bin0': -15}
    >>> calculate_zone(row)
    'eh_south_temperate'

    >>> row = {'longitude_bin0': -45, 'latitude_bin0': 70}
    >>> calculate_zone(row)
    'wh_north_polar'
    """
    longitude_bin0 = row['longitude_bin0']
    latitude_bin0 = row['latitude_bin0']
    
    # Check if longitude is in the Western Hemisphere
    if -180 <= longitude_bin0 < 0:
        # Western Hemisphere zones
        if 60 <= latitude_bin0 <= 90:
            return 'wh_north_polar'
        elif 30 <= latitude_bin0 < 60:
            return 'wh_north_temperate'
        elif 0 <= latitude_bin0 < 30:
            return 'wh_tropical'
        elif -30 <= latitude_bin0 < 0:
            return 'wh_south_temperate'
        elif -90 <= latitude_bin0 < -30:
            return 'wh_south_polar'
        else:
            raise ValueError(f"Invalid latitude_bin0 for Western Hemisphere: {latitude_bin0}")
    # Check if longitude is in the Eastern Hemisphere
    elif 0 <= longitude_bin0 <= 180:
        # Eastern Hemisphere zones
        if 60 <= latitude_bin0 <= 90:
            return 'eh_north_polar'
        elif 30 <= latitude_bin0 < 60:
            return 'eh_north_temperate'
        elif 0 <= latitude_bin0 < 30:
            return 'eh_tropical'
        elif -30 <= latitude_bin0 < 0:
            return 'eh_south_temperate'
        elif -90 <= latitude_bin0 < -30:
            return 'eh_south_polar'
        else:
            raise ValueError(f"Invalid latitude_bin0 for Eastern Hemisphere: {latitude_bin0}")
    else:
        # Longitude value is outside the valid range
        raise ValueError(f"Invalid longitude_bin0: {longitude_bin0}")
