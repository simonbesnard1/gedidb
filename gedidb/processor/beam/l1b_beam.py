import geopandas as gpd
import pandas as pd
import numpy as np

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


DEFAULT_QUALITY_FILTERS = None

class L1BBeam(Beam):

    def __init__(self,granule: Granule, beam: str, field_mapping:dict):
        
        super().__init__(granule, beam, field_mapping)
        self._shot_geolocations = None
        self._filtered_index = None
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        self._shot_geolocations = gpd.points_from_xy(
            x=self["geolocation/longitude_lastbin"],
            y=self["geolocation/latitude_lastbin"],
            crs=WGS84,
        )
        return self._shot_geolocations
    
    def _get_main_data(self) -> dict:
        
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        delta_time = self["delta_time"][()]
        
        # Initialize the data dictionary
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds")

        }
        
        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            if key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = np.array([beam_type] * self.n_shots)
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            elif key == "waveform_start":
                data[key] = np.array(self[sds_name][()] - 1)
            elif key in ["rxwaveform", "txwaveform"]:
                rxwaveform = self[sds_name][()]
                sdsCount = self['rx_sample_count'][()]
                sdsStart = self['rx_sample_start_index'][()]
                data[key] = np.array([
                                        rxwaveform[start:start + count].tolist() 
                                        for start, count in zip(sdsStart, sdsCount)
                                    ])
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply filter and get the filtered indices
        self._filtered_index = self.apply_filter(data, filters=DEFAULT_QUALITY_FILTERS)
        
        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}
        
        return filtered_data if filtered_data else None      

