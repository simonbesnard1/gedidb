from __future__ import annotations

import geopandas as gpd
import h5py

from constants import WGS84

QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


class Granule(h5py.File):

    def __init__(self, file_path):

        super().__init__(file_path, "r")
        self.short_name = self['METADATA']['DatasetIdentification'].attrs['shortName']
        self.beam_names = [name for name in self.keys() if name.startswith("BEAM")]

        print(self.short_name)

        # self.debug_file()

    def debug_file(self):

        layer = 0
        with open(f"./debug/{self.short_name}_debug.txt", "w") as f:
            def print_group(group, layer):
                for x in group:
                    if isinstance(group[x], h5py.Group):
                        f.write(f"{'\t' * layer}{x}:\n")
                        print_group(group[x], layer + 1)
                    else:
                        f.write(f"{'\t' * layer}{x}\n")

            print_group(self, layer)


class Beam(h5py.Group):

    def __init__(self, granule: Granule, beam: str):
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self._cached_data = None
        self._shot_geolocations = None

    @property
    def n_shots(self) -> int:
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def main_data(self) -> gpd.GeoDataFrame:
        """
        Return the main data for all shots in beam as geopandas DataFrame.

        Returns:
            gpd.GeoDataFrame: A geopandas DataFrame containing the main data for the given beam object.
        """
        if self._cached_data is None:
            data = self._get_main_data_dict()
            geometry = self.shot_geolocations
            self._cached_data = gpd.GeoDataFrame(
                data, geometry=geometry, crs=WGS84
            )

        return self._cached_data
