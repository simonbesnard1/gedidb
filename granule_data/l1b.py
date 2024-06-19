import pandas as pd
import geopandas as gpd

from granule_data.granule import Granule, Beam, QDEGRADE
from constants import WGS84


class L1BGranule(Granule):

    def __init__(self, file_path):
        super().__init__(file_path)


class L1BBeam(Beam):

    def __init__(self, granule: Granule, beam: str):
        super().__init__(granule, beam)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["lon_lowestmode"],
                y=self["lat_lowestmode"],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["delta_time"][:],
            # Quality data
            "degrade": self["geolocation/degrade"][:],
            "stale_return_flag": self["stale_return_flag"][:],
            "solar_elevation": self["geolocation/solar_elevation"][:],
            "solar_azimuth": self["geolocation/solar_elevation"][:],
            "rx_energy": self["rx_energy"][:],
            # DEM
            "dem_tandemx": self["geolocation/digital_elevation_model"][:],
            "dem_srtm": self["geolocation/digital_elevation_model_srtm"][:],
            # geolocation bin0
            "latitude_bin0": self["geolocation/latitude_bin0"][:],
            "latitude_bin0_error": self["geolocation/latitude_bin0_error"][:],
            "longitude_bin0": self["geolocation/longitude_bin0"][:],
            "longitude_bin0_error": self["geolocation/longitude_bin0_error"][:],
            "elevation_bin0": self["geolocation/elevation_bin0"][:],
            "elevation_bin0_error": self["geolocation/elevation_bin0_error"][:],
            # geolocation lastbin
            "latitude_lastbin": self["geolocation/latitude_lastbin"][:],
            "latitude_lastbin_error": self["geolocation/latitude_lastbin_error"][:],
            "longitude_lastbin": self["geolocation/longitude_lastbin"][:],
            "longitude_lastbin_error": self["geolocation/longitude_lastbin_error"][:],
            "elevation_lastbin": self["geolocation/elevation_lastbin"][:],
            "elevation_lastbin_error": self["geolocation/elevation_lastbin_error"][:],
            # relative waveform position info in beam and ssub-granule
            "waveform_start": self["rx_sample_start_index"][:] - 1,
            "waveform_count": self["rx_sample_count"][:],
        }

        return data
