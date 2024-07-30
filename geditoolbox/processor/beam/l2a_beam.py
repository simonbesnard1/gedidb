import pandas as pd
import geopandas as gpd
import yaml

from geditoolbox.processor.granule.granule import Granule
from geditoolbox.processor.beam.beam import Beam
from geditoolbox.utils.constants import WGS84


class L2ABeam(Beam):

    def __init__(self, granule: Granule, beam: str):
        super().__init__(granule, beam)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['lon_lowestmode'],
                y=self['lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def quality_filter(self):

        filtered = self.main_data

        # how to deal with this in config file?
        filtered["elevation_difference_tdx"] = (filtered["elev_lowestmode"] - filtered["digital_elevation_model"])

        # TODO: dont load for every beam
        with open('../config.yml') as f:
            config = yaml.safe_load(f)

        quality_filters = config["quality_filters"]["level_2a"]

        for key, value in quality_filters.items():
            if key == 'drop':
                return
            if isinstance(value, list):
                for v in value:
                    filtered = filtered.query(f"{key} {v}")
            else:
                filtered = filtered.query(f"{key} {value}")

        filtered = filtered.drop(quality_filters['drop'], axis=1)

        """
        filtered = filtered[
            # initial filtering
            (filtered["quality_flag"] == 1)
            & (filtered["sensitivity_a0"] >= 0.9)
            & (filtered["sensitivity_a0"] <= 1.0)
            # other values than https://docs.google.com/document/d/1XmcoV8-k-8C_Tmh-CJ4sYvlvOqkbiXP1Kah_KrCkMqU/edit
            & (filtered["sensitivity_a2"] > 0.95)
            & (filtered["sensitivity_a2"] <= 1.0)
            & (filtered["degrade_flag"].isin(QDEGRADE))

            # secondary filtering
            # missing tropical_evergreen_broadleaf
            & (filtered["rh_100"] >= 0)
            & (filtered["rh_100"] < 120)
            & (filtered["surface_flag"] == 1)
            & (filtered["elevation_difference_tdx"] > -150)
            & (filtered["elevation_difference_tdx"] < 150)
            # missing water_persistence & urban_proportion
            ]

        filtered = filtered.drop(
            [
                "quality_flag",
                "surface_flag"
            ],
            axis=1
        )
        """

        self._cached_data = filtered

    def _get_main_data_dict(self) -> dict:
        gedi_l2a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
                   # General identifiable data
                   "granule_name": [self.parent_granule.filename] * self.n_shots,
                   "shot_number": self["shot_number"][:],
                   "beam_type": [self.beam_type] * self.n_shots,
                   "beam_name": [self.name] * self.n_shots,
                   # Temporal data
                   "delta_time": self["delta_time"][:],
                   "absolute_time": (gedi_l2a_count_start + pd.to_timedelta(self["delta_time"], unit="seconds")),
                   # Quality data
                   "sensitivity_a0": self["sensitivity"][:],
                   "sensitivity_a1": self["geolocation/sensitivity_a1"][:],
                   "sensitivity_a2": self["geolocation/sensitivity_a2"][:],
                   "sensitivity_a3": self["geolocation/sensitivity_a3"][:],
                   "sensitivity_a4": self["geolocation/sensitivity_a4"][:],
                   "sensitivity_a5": self["geolocation/sensitivity_a5"][:],
                   "sensitivity_a6": self["geolocation/sensitivity_a6"][:],
                   "quality_flag": self["quality_flag"][:],
                   "degrade_flag": self["degrade_flag"][:],
                   "solar_elevation": self["solar_elevation"][:],
                   "solar_azimuth": self["solar_elevation"][:],
                   "energy_total": self["energy_total"][:],
                   "surface_flag": self["surface_flag"][:],
                   # DEM
                   "digital_elevation_model": self["digital_elevation_model"][:],
                   "digital_elevation_model_srtm": self["digital_elevation_model_srtm"][:],
                   # Processing data
                   "selected_algorithm": self["selected_algorithm"][:],
                   "selected_mode": self["selected_mode"][:],
                   # Geolocation data
                   "lon_lowestmode": self["lon_lowestmode"][:],
                   "longitude_bin0_error": self["longitude_bin0_error"][:],
                   "lat_lowestmode": self["lat_lowestmode"][:],
                   "latitude_bin0_error": self["latitude_bin0_error"][:],
                   "elev_lowestmode": self["elev_lowestmode"][:],
                   "elevation_bin0_error": self["elevation_bin0_error"][:],
                   "lon_highestreturn": self["lon_highestreturn"][:],
                   "lat_highestreturn": self["lat_highestreturn"][:],
                   "elev_highestreturn": self["elev_highestreturn"][:],
               } | {
                   f"rh_{i}": self["rh"][:, i] for i in range(101)
               }

        return data
