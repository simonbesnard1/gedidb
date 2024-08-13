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
        
        # First pass: gather all data and determine max length of rxwaveform variables
        for key, source in self.field_mapper.items():
            if key in ["granule_name"]:
                data[key] = [getattr(self.parent_granule, source.split('.')[-1])] * self.n_shots
            elif key in ["beam_type"]:
                data[key] = [getattr(self, source)] * self.n_shots
            elif key in ["beam_name"]:
                data[key] = [self.name] * self.n_shots
            elif key == "waveform_start":
                data[key] = self[source][:] - 1
            elif key == "rxwaveform":
                data[key] = self[source][:].tolist()
            else:
                data[key] = self[source][:]
                  
        return pd.DataFrame(data)
