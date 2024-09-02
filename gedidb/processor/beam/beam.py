import geopandas as gpd
import h5py
import numpy as np

from gedidb.utils.constants import WGS84
from typing import Union, List

class Beam(h5py.Group):

    def __init__(self, granule, beam: str, field_mapping: dict):
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self.field_mapping = field_mapping
        self._cached_data = None

    @staticmethod
    def apply_filter(data, filters=None):
        if filters is not None:
            mask = np.ones(len(data['beam_name']), dtype=bool)
            for filter_name, filter_func in filters.items():
                filter_mask = filter_func(data)
                mask &= filter_mask
            return mask
        return np.ones(len(data['beam_name']), dtype=bool)

    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def field_mapper(self):
        return self.field_mapping

    @property
    def main_data(self) -> gpd.GeoDataFrame:
        if self._cached_data is None:
            data = self._get_main_data()
            
            if data is not None:
                geometry = self.shot_geolocations
                
                if hasattr(self, '_filtered_index'):
                    geometry = geometry[self._filtered_index]
                
                for key, value in data.items():
                    if isinstance(value, np.ndarray) and value.ndim > 1:
                        data[key] = value.tolist()
                
                self._cached_data = gpd.GeoDataFrame(
                    data, geometry=geometry, crs=WGS84
                )
                
                # Apply SQL formatting to array-type fields
                self.sql_format_arrays()

        return self._cached_data

    def sql_format_arrays(self):
        array_cols = [c for c in self._cached_data.columns if c.endswith("_z") or c == "rh"]

        for c in array_cols:
            self._cached_data[c] = self._cached_data[c].map(self._arr_to_str)

    def _arr_to_str(self, arr: Union[List[float], np.array, float]) -> str:
        return "{" + ", ".join(map(str, arr)) + "}"
