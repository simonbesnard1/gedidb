import pandas as pd
import geopandas as gpd

from geditoolbox.processor.granule.granule import Granule, QDEGRADE
from geditoolbox.processor.beam.beam import Beam
from geditoolbox.utils.constants import WGS84


class L4ABeam(Beam):

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

    def quality_filter(self):

        filtered = self.main_data

        filtered = filtered[
            (filtered["l2_quality_flag"] == 1)
            & (filtered["sensitivity_a0"] >= 0.9)
            & (filtered["sensitivity_a0"] <= 1.0)
            & (filtered["sensitivity_a2"] <= 1.0)
            & (filtered["degrade_flag"].isin(QDEGRADE))
            & (filtered["surface_flag"] == 1)
        ]

        filtered = filtered[
            ((filtered["pft_class"] == 2) & (filtered["sensitivity_a2"] > 0.98))
            | (
                    (filtered["pft_class"] != 2)
                    & (filtered["sensitivity_a2"] > 0.95)
            )
        ]

        filtered = filtered[
            (filtered["landsat_water_persistence"] < 10)
            & (filtered["urban_proportion"] < 50)
        ]

        filtered = filtered.drop(
            [
                "l2_quality_flag",
                # "l4_quality_flag",
                # "algorithm_run_flag",
                "surface_flag",
            ],
            axis=1,
        )

        self._cached_data = filtered

    def _get_main_data_dict(self) -> dict:

        gedi_l4a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {
            # General identifiable data
            "granule_name": [self.parent_granule.filename] * self.n_shots,
            "shot_number": self["shot_number"][:],
            "beam_type": [self.beam_type] * self.n_shots,
            "beam_name": [self.name] * self.n_shots,
            # Temporal data
            "delta_time": self["delta_time"][:],
            "absolute_time": (gedi_l4a_count_start + pd.to_timedelta(self["delta_time"], unit="seconds")),
            # Quality data
            "sensitivity_a0": self["sensitivity"][:],
            "sensitivity_a2": self["geolocation/sensitivity_a2"][:],
            "sensitivity_a10": self["geolocation/sensitivity_a10"][:],
            "algorithm_run_flag": self["algorithm_run_flag"][:],
            "degrade_flag": self["degrade_flag"][:],
            "l2_quality_flag": self["l2_quality_flag"][:],
            "l4_quality_flag": self["l4_quality_flag"][:],
            "predictor_limit_flag": self["predictor_limit_flag"][:],
            "response_limit_flag": self["response_limit_flag"][:],
            "surface_flag": self["surface_flag"][:],
            # Processing data
            "selected_algorithm": self["selected_algorithm"][:],
            "selected_mode": self["selected_mode"][:],
            # Geolocation data
            "elev_lowestmode": self["elev_lowestmode"][:],
            "lat_lowestmode": self["lat_lowestmode"][:],
            "lon_lowestmode": self["lon_lowestmode"][:],
            # ABGD data
            "agbd": self["agbd"][:],
            "agbd_pi_lower": self["agbd_pi_lower"][:],
            "agbd_pi_upper": self["agbd_pi_upper"][:],
            "agbd_se": self["agbd_se"][:],
            "agbd_t": self["agbd_t"][:],
            "agbd_t_se": self["agbd_t_se"][:],
            # Land cover data: NOTE this is gridded and/or derived data
            "pft_class": self["land_cover_data/pft_class"][:],
            "region_class": self["land_cover_data/region_class"][:],
            "urban_proportion": self["land_cover_data/urban_proportion"][:],
            "landsat_water_persistence": self["land_cover_data/landsat_water_persistence"][:],
            "leaf_on_doy": self["land_cover_data/leaf_on_doy"][:],
            "leaf_on_cycle": self["land_cover_data/leaf_on_cycle"][:],
        }

        return data
