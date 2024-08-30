import geopandas as gpd
import h5py
import numpy as np

from gedidb.utils.constants import WGS84
from typing import Union, List


class Beam(h5py.Group):

    def __init__(self, granule, beam: str, quality_flag:dict, field_mapping:dict):
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self.quality_flag = quality_flag
        self.field_mapping = field_mapping
        
    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def quality_filter(self):
        return self.quality_flag
    
    @property
    def field_mapper(self):
        return self.field_mapping
    
    @property
    def main_data(self) -> gpd.GeoDataFrame:

        data = self._get_main_data()
        
        if data is not None:
            geometry = self.shot_geolocations
            
            # Filter geometry using the filtered index from apply_filter
            if hasattr(self, '_filtered_index'):
                geometry = geometry[self._filtered_index]
              
            self._cached_data = gpd.GeoDataFrame(
                data, geometry=geometry, crs=WGS84
            )


            return self._cached_data