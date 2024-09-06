import logging
import geopandas as gpd
from shapely.geometry import polygon, multipolygon
from shapely import Geometry  # for typing only

MAX_CMR_COORDS = 4999


class DetailError(Exception):
    """Raised when too many points in a shape for NASA's API"""

    def __init__(self, n_coords: int):
        self.n_coords = n_coords
        super().__init__(
            f"Shape contains {n_coords} coordinates, exceeding the limit."
        )


def _count_coordinates(geom: Geometry) -> int:
    if isinstance(geom, multipolygon.MultiPolygon):
        return sum([len(part.exterior.coords) for part in geom.geoms])
    return len(geom.exterior.coords)


def check_and_format_shape(
    shp: gpd.GeoDataFrame, simplify: bool = False
) -> gpd.GeoSeries:
    """Check if shapefile is valid for the NASA CMR API and format if needed.

    The CMR API cannot accept a shapefile with more than 5000 points,
    so we offer to simplify the shape to just a convex hull around the region.
    The CMR API also requires that the polygon is oriented correctly.
    Raises:
        ValueError: If more than one polygon is passed.
        DetailError: If the shapefile has too many points and simplify is False,
            or if the simplified shape still has too many points.
    """
    if len(shp) > 1:
        raise ValueError("Only one polygon at a time supported.")
    row = shp.geometry.values[0]

    n_coords = _count_coordinates(row)
    if n_coords > MAX_CMR_COORDS:
        if not simplify:
            raise DetailError(n_coords)
        logging.info(
            f"Simplifying shape with {n_coords} coordinates to convex hull."
        )
        row = row.convex_hull
        n_coords = _count_coordinates(row)
        logging.info(f"Simplified shape to {n_coords} coordinates.")
        if n_coords > MAX_CMR_COORDS:
            raise DetailError(n_coords)

    if isinstance(row, multipolygon.MultiPolygon):
        return gpd.GeoSeries(
            [polygon.orient(s) for s in row.geoms], crs=shp.crs
        )
    else:
        return gpd.GeoSeries(polygon.orient(row), crs=shp.crs)
