import pandas as pd
import geopandas as gpd

from GEDItools.processor.granule.granule import Granule
from GEDItools.processor.beam.beam import Beam
from GEDItools.utils.constants import WGS84


class L4ABeam(Beam):

    def __init__(self, granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
        
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["lon_lowestmode"],
                y=self["lat_lowestmode"],
                crs=WGS84,
            )
        return self._shot_geolocations

    def quality_filter(self, data):

        data = data[
            (data["l2_quality_flag"] == 1)
            & (data["sensitivity_a0"] >= 0.9)
            & (data["sensitivity_a0"] <= 1.0)
            & (data["sensitivity_a2"] <= 1.0)
            & (data["degrade_flag"].isin([0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]))
            & (data["surface_flag"] == 1)
        ]

        data = data[
            ((data["pft_class"] == 2) & (data["sensitivity_a2"] > 0.98))
            | (
                    (data["pft_class"] != 2)
                    & (data["sensitivity_a2"] > 0.95)
            )
        ]

        data = data[
            (data["landsat_water_persistence"] < 10)
            & (data["urban_proportion"] < 50)
        ]

        data = data.drop(
            [
                "l2_quality_flag",
                # "l4_quality_flag",
                # "algorithm_run_flag",
                "surface_flag",
            ],
            axis=1,
        )

        return data

    def _get_main_data_dict(self) -> dict:

        data = {}        
        # Populate data from general_data section
        for key, source in self.field_mapper.items():
            if key in ["granule_name"]:
                # Handle special case for granule_name
                data[key] = [getattr(self.parent_granule, source.split('.')[-1])] * self.n_shots
            elif key in ["beam_type"]:                
                # Handle special cases for beam_type 
                data[key] = [getattr(self, source)] * self.n_shots
            elif key in ["beam_name"]:                
                # Handle special cases for beam_name
                data[key] = [self.name] * self.n_shots
            elif key in "waveform_start":
                # Handle special cases for waveform_start 
                data[key] = self[source][:] - 1
            elif key in ["absolute_time"]:     
                gedi_l2b_count_start = pd.to_datetime(source)
                data[key] = (gedi_l2b_count_start + pd.to_timedelta(self["delta_time"], unit="seconds"))
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][:]
        
        data = self.apply_filter(pd.DataFrame(data))
        
        return data
