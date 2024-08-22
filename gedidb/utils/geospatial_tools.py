import logging
from geopandas import GeoDataFrame, GeoSeries
from shapely.geometry.polygon import orient

class DetailError(Exception):
    """Raised when too many points in a shape for NASA's API"""

    def __init__(self, n_coords: int):
        self.n_coords = n_coords
        super().__init__(f"Shape contains {n_coords} coordinates, exceeding the limit.")


class ShapeProcessor:
    def __init__(self, shape: GeoDataFrame, max_coords: int = 4999):
        if len(shape) > 1:
            raise ValueError("Only one polygon at a time is supported.")
        if max_coords >= 5000:
            raise ValueError("NASA's API can only handle shapes with less than 5000 points.")
        
        self.shape = shape.geometry.values[0]
        self.max_coords = max_coords
        self.oriented_geom = None

    def count_coordinates(self) -> int:
        """Counts the total number of coordinates in the shape."""
        if self.shape.geom_type.startswith("Multi"):
            return sum(len(part.exterior.coords) for part in self.shape.geoms)
        return len(self.shape.exterior.coords)

    def simplify_shape(self):
        """Simplifies the shape to its convex hull if it exceeds the max_coords."""
        n_coords = self.count_coordinates()
        if n_coords > self.max_coords:
            logging.info(f"Simplifying shape with {n_coords} coordinates to convex hull.")
            self.shape = self.shape.convex_hull
            logging.info(f"Simplified shape to {len(self.shape.exterior.coords)} coordinates.")
    
    def orient_geometry(self):
        """Orients the shape's geometry, handling both single and multi-polygons."""
        if self.shape.geom_type.startswith("Multi"):
            self.oriented_geom = GeoSeries([orient(part) for part in self.shape.geoms])
        else:
            self.oriented_geom = GeoSeries(orient(self.shape))
    
    def check_and_format(self, simplify: bool = False) -> GeoSeries:
        """
        Checks the shape for compatibility with NASA's API and formats it accordingly.
        
        Args:
            simplify (bool): Whether to simplify the shape if it exceeds max_coords.

        Raises:
            DetailError: If the shape exceeds max_coords and simplify is False.
        
        Returns:
            GeoSeries: The formatted (and possibly simplified) shape.
        """
        if simplify:
            self.simplify_shape()
        elif self.count_coordinates() > self.max_coords:
            raise DetailError(self.count_coordinates())
        
        self.orient_geometry()
        return self.oriented_geom