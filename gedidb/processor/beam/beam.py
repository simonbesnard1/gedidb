import geopandas as gpd
import h5py
import numpy as np

from gedidb.utils.constants import WGS84
from typing import Union, List


class Beam(h5py.Group):

    def __init__(self, granule, beam: str, quality_flag:dict, field_mapping:dict, geom: gpd.GeoSeries):
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self._cached_data = None
        self._shot_geolocations = None
        self.quality_flag = quality_flag
        self.field_mapping = field_mapping
        self.geom = geom
        
    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["geolocation/longitude_lastbin"],
                y=self["geolocation/latitude_lastbin"],
                crs=WGS84,
            )
        return self._shot_geolocations

    @property
    def quality_filter(self):
        return self.quality_flag
    
    @property
    def field_mapper(self):
        return self.field_mapping
    
    @property
    def main_data(self) -> gpd.GeoDataFrame:
        if self._cached_data is None:
            data = self._get_main_data_dict()
            geometry = self.shot_geolocations
            
            # Filter geometry using the filtered index from apply_filter
            if hasattr(self, '_filtered_index'):
                geometry = geometry[self._filtered_index]
            
            self._cached_data = gpd.GeoDataFrame(
                data, geometry=geometry, crs=WGS84
            )
    
        return self._cached_data


    def sql_format_arrays(self) -> None:
        """Forces array-type fields to be sql-formatted (text strings).

        Until this function is called, array-type fields will be np.array() objects. This formatting can be undone by resetting the cache.
        """
    
        array_cols = [c for c in self.main_data.columns if c.endswith("_z")]
        
        for c in array_cols:
            self._cached_data[c] = self.main_data[c].map(self._arr_to_str)

    def _arr_to_str(self, arr: Union[List[float], np.array, float]) -> str:
        """Converts array type data or single float values to SQL-friendly string."""
        if isinstance(arr, (list, np.ndarray)):
            return "{" + ", ".join(map(str, arr)) + "}"
        elif isinstance(arr, float):
            return str(arr)  # Handle single float values
        else:
            return "{}"  # Handle unexpected cases, or raise an error if necessary

