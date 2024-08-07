import geopandas as gpd
import h5py
import numpy as np

from GEDItools.utils.constants import WGS84
from typing import Union, List
import yaml


QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


class Beam(h5py.Group):

    def __init__(self, granule, beam: str, config_file:str):
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self._cached_data = None
        self._shot_geolocations = None
        
        with open(config_file) as f:
            self.quality_filter_config = yaml.safe_load(f)


    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    def _get_main_data_dict(self):
        raise NotImplementedError

    @property
    def shot_geolocations(self):
        raise NotImplementedError

    def quality_filter(self):
        raise NotImplementedError

    @property
    def main_data(self) -> gpd.GeoDataFrame:

        if self._cached_data is None:
            data = self._get_main_data_dict()
            geometry = self.shot_geolocations
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

    def _arr_to_str(self, arr: Union[List[float], np.array]) -> str:
        """Converts array type data to SQL-friendly string."""
        return "{" + ", ".join(map(str, arr)) + "}"
