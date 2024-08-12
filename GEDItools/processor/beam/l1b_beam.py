import geopandas as gpd
import pandas as pd

from GEDItools.processor.granule.granule import Granule
from GEDItools.processor.beam.beam import Beam
from GEDItools.utils.constants import WGS84


class L1BBeam(Beam):

    def __init__(self, granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
        
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["geolocation/longitude_lastbin"],
                y=self["geolocation/latitude_lastbin"],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data_dict(self) -> dict:
        
        data = {}
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
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][:]
                
        return pd.DataFrame(data)
