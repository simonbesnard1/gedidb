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
