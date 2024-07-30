import pandas as pd
import geopandas as gpd
import numpy as np
import yaml

from geditoolbox.processor.granule.granule import Granule
from geditoolbox.processor.beam.beam import Beam
from geditoolbox.utils.constants import WGS84


class L2BBeam(Beam):

    def __init__(self, granule: Granule, beam: str):
        super().__init__(granule, beam)

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['geolocation/lon_lowestmode'],
                y=self['geolocation/lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def quality_filter(self):
        filtered = self.main_data

        # how to deal with this in config file?
        # also QEDEGRADE is not defined
        filtered["elevation_difference_tdx"] = (
                filtered["elev_lowestmode"] - filtered["digital_elevation_model"]
        )

        with open('../config.yml') as f:
            config = yaml.safe_load(f)

        quality_filters = config["quality_filters"]["level_2b"]

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
            (filtered["l2a_quality_flag"] == 1)
            & (filtered["l2b_quality_flag"] == 1)
            # what is this?
            # & (filtered["algorithmrun_flag"] == 1)
            & (filtered["sensitivity"] >= 0.9)
            & (filtered["sensitivity"] <= 1.0)
            & (filtered["degrade_flag"].isin(QDEGRADE))

            # secondary filtering
            # missing tropical_evergreen_broadleaf
            & (filtered["rh100"] >= 0)
            # L2B RH_100 is in cm, not m like L2A
            & (filtered["rh100"] < 12000)
            & (filtered["surface_flag"] == 1)
            & (filtered["elevation_difference_tdx"] > -150)
            & (filtered["elevation_difference_tdx"] < 150)
            & (filtered["water_persistence"] < 10)
            & (filtered["urban_proportion"] < 50)

            # Additional (Amelia) filters:
            # & (~np.isnan(filtered["cover"]))
            # & (filtered["pai"] != -9999.0)
            ]
        filtered = filtered.drop(
            [
                "l2a_quality_flag",
                "l2b_quality_flag",
                # what is this?
                "algorithmrun_flag",
                "surface_flag",
            ],
            axis=1,
        )
        """

        self._cached_data = filtered

    def _get_main_data_dict(self) -> dict:

        gedi_l2b_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["geolocation/delta_time"][:],
            "absolute_time": (gedi_l2b_count_start + pd.to_timedelta(self["delta_time"], unit="seconds")),
            # Quality data
            "algorithmrun_flag": self["algorithmrun_flag"][:],
            "l2a_quality_flag": self["l2a_quality_flag"][:],
            "l2b_quality_flag": self["l2b_quality_flag"][:],
            "sensitivity": self["sensitivity"][:],
            "degrade_flag": self["geolocation/degrade_flag"][:],
            "stale_return_flag": self["stale_return_flag"][:],
            "surface_flag": self["surface_flag"][:],
            "solar_elevation": self["geolocation/solar_elevation"][:],
            "solar_azimuth": self["geolocation/solar_azimuth"][:],
            # Scientific data
            "cover": self["cover"][:],
            "cover_z": list(self["cover_z"][:]),
            "fhd_normal": self["fhd_normal"][:],
            "num_detectedmodes": self["num_detectedmodes"][:],
            "omega": self["omega"][:],
            "pai": self["pai"][:],
            "pai_z": list(self["pai_z"][:]),
            "pavd_z": list(self["pavd_z"][:].tolist()),
            "pgap_theta": self["pgap_theta"][:],
            "pgap_theta_error": self["pgap_theta_error"][:],
            "rg": self["rg"][:],
            "rh100": self["rh100"][:],
            "rhog": self["rhog"][:],
            "rhog_error": self["rhog_error"][:],
            "rhov": self["rhov"][:],
            "rhov_error": self["rhov_error"][:],
            "rossg": self["rossg"][:],
            "rv": self["rv"][:],
            "rx_range_highestreturn": self["rx_range_highestreturn"][:],
            # DEM
            "digital_elevation_model": self["geolocation/digital_elevation_model"][:],
            # Land cover data: NOTE this is gridded and/or derived data
            "leaf_off_flag": self["land_cover_data/leaf_off_flag"][:],
            "leaf_on_doy": self["land_cover_data/leaf_on_doy"][:],
            "leaf_on_cycle": self["land_cover_data/leaf_on_cycle"][:],
            "water_persistence": self["land_cover_data/landsat_water_persistence"][:],
            "urban_proportion": self["land_cover_data/urban_proportion"][:],
            "modis_nonvegetated": self["land_cover_data/modis_nonvegetated"][:],
            "modis_treecover": self["land_cover_data/modis_treecover"][:],
            "pft_class": self["land_cover_data/pft_class"][:],
            "region_class": self["land_cover_data/region_class"][:],
            # Processing data
            "selected_l2a_algorithm": self["selected_l2a_algorithm"][:],
            "selected_rg_algorithm": self["selected_rg_algorithm"][:],
            "dz": np.repeat(self["ancillary/dz"][:], self.n_shots),
            # Geolocation data
            "lon_highestreturn": self["geolocation/lon_highestreturn"][:],
            "lon_lowestmode": self["geolocation/lon_lowestmode"][:],
            "longitude_bin0": self["geolocation/longitude_bin0"][:],
            "longitude_bin0_error": self["geolocation/longitude_bin0_error"][:],
            "lat_highestreturn": self["geolocation/lat_highestreturn"][:],
            "lat_lowestmode": self["geolocation/lat_lowestmode"][:],
            "latitude_bin0": self["geolocation/latitude_bin0"][:],
            "latitude_bin0_error": self["geolocation/latitude_bin0_error"][:],
            "elev_highestreturn": self["geolocation/elev_highestreturn"][:],
            "elev_lowestmode": self["geolocation/elev_lowestmode"][:],
            "elevation_bin0": self["geolocation/elevation_bin0"][:],
            "elevation_bin0_error": self["geolocation/elevation_bin0_error"][:],
            # waveform data
            "waveform_count": self["rx_sample_count"][:],
            "waveform_start": self["rx_sample_start_index"][:] - 1,
        }

        return data
