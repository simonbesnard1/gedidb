import geopandas as gpd
import pandas as pd
import os

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L1BBeam(Beam):

    def __init__(self,granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        self._shot_geolocations = gpd.points_from_xy(
            x=self["geolocation/longitude_lastbin"],
            y=self["geolocation/latitude_lastbin"],
            crs=WGS84,
        )
        return self._shot_geolocations

    
    def apply_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        
        if self.quality_filter != "None":
            
            for key, value in self.quality_filter.items():
                if key == 'drop':
                    continue  # Skip dropping columns here
                if isinstance(value, list):
                    for v in value:
                        data = data.query(f"{key} {v}")
                else:
                    data = data.query(f"{key} {value}")
        
            data = data.drop(columns=self.quality_filter.get('drop', []))
        filtered_index = data.index  # Get the filtered indices
        
        self._filtered_index = filtered_index  # Store the filtered indices
        
        return data


    def _get_main_data(self) -> dict:
        
        data = {}
        
        for key, source in self.field_mapper.items():
            if key in ["granule_name"]:
                data[key] = [os.path.basename(os.path.dirname(getattr(self.parent_granule, source['SDS_Name'].split('.')[-1])))] * self.n_shots
            elif key in ["beam_type"]:
                data[key] = [getattr(self, source['SDS_Name'])] * self.n_shots
            elif key in ["beam_name"]:
                data[key] = [self.name] * self.n_shots
            elif key == "waveform_start":
                data[key] = self[source['SDS_Name']][()] - 1
            elif key in ["rxwaveform", "txwaveform"]:
                rxwaveform = self[source['SDS_Name']][()]
                sdsCount = self['rx_sample_count'][()] 
                sdsStart = self['rx_sample_start_index'][()] 
                num_shots = len(sdsCount)
                data[key] = []        
                for i in range(num_shots):
                    start_idx = sdsStart[i]
                    count = sdsCount[i]
                    shot_waveform = rxwaveform[start_idx:start_idx + count]
                    data[key].append(shot_waveform.tolist())
            else:
                data[key] = self[source['SDS_Name']][()]
                
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        data["absolute_time"] = (gedi_count_start + pd.to_timedelta(self["delta_time"][()], unit="seconds"))      
        data = self.apply_filter(pd.DataFrame(data))
        
        if not data.empty:
            return data        

        

