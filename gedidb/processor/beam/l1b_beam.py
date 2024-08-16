import geopandas as gpd
import pandas as pd
import numpy as np

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L1BBeam(Beam):

    def __init__(self,granule: Granule, beam: str, quality_flag:dict, field_mapping:dict, geom: gpd.GeoSeries):
        
        super().__init__(granule, beam, quality_flag, field_mapping, geom)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self["geolocation/longitude_lastbin"],
                y=self["geolocation/latitude_lastbin"],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data(self) -> dict:
        
        # # Filter shot_geolocations and other attributes using the spatial mask
        # filtered_n_shots = np.sum(self.spatial_mask)  # Count of True values in self.spatial_mask

        # if filtered_n_shots >0:
        data = {}
        
        for key, source in self.field_mapper.items():
            if key in ["granule_name"]:
                data[key] = [getattr(self.parent_granule, source.split('.')[-1])] * self.n_shots
            elif key in ["beam_type"]:
                data[key] = [getattr(self, source)] * self.n_shots
            elif key in ["beam_name"]:
                data[key] = [self.name] * self.n_shots
            elif key == "waveform_start":
                data[key] = self[source][()] - 1
            elif key in ["rxwaveform", "txwaveform"]:
                rxwaveform = self[source][()]
                sdsCount = self['rx_sample_count'][()]  # assuming sdsCount is available like this
                sdsStart = self['rx_sample_start_index'][()]  # assuming sdsStart is available like this
                num_shots = len(sdsCount)
                data[key] = []        
                for i in range(num_shots):
                    start_idx = sdsStart[i]
                    count = sdsCount[i]
                    shot_waveform = rxwaveform[start_idx:start_idx + count]
                    data[key].append(shot_waveform.tolist())
            else:
                data[key] = self[source][()]
                
        data = pd.DataFrame(data)
        
        return data
        

