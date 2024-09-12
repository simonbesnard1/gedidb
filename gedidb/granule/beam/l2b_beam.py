import pandas as pd
import numpy as np
import geopandas as gpd

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.beam import Beam
from gedidb.utils.constants import WGS84


DEFAULT_QUALITY_FILTERS = {
                            'l2a_quality_flag': lambda data: data['l2a_quality_flag'] == 1,
                            'l2b_quality_flag': lambda data: data['l2b_quality_flag'] == 1,
                            'sensitivity': lambda data: (data['sensitivity'] >= 0.9) & (data['sensitivity'] <= 1.0),
                            'degrade_flag': lambda data: np.isin(data['degrade_flag'], [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]),
                            'rh100': lambda data: (data['rh100'] >= 0) & (data['rh100'] < 1200),
                            'surface_flag': lambda data: data['surface_flag'] == 1,
                            'elevation_difference_tdx': lambda data: (data['elevation_difference_tdx'] > -150) & (data['elevation_difference_tdx'] < 150),
                            'water_persistence': lambda data: data['water_persistence'] < 10,
                            'urban_proportion': lambda data: data['urban_proportion'] < 50
                        }

class L2BBeam(Beam):

    def __init__(self,granule: Granule, beam: str, field_mapping:dict):
        
        super().__init__(granule, beam, field_mapping)
        self._shot_geolocations = None
        self._filtered_index = None
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        self._shot_geolocations = gpd.points_from_xy(
            x=self['geolocation/lon_lowestmode'],
            y=self['geolocation/lat_lowestmode'],
            crs=WGS84,
        )
        return self._shot_geolocations

    def _get_main_data(self) -> dict:
        
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        delta_time = self["delta_time"][()]
        elev_lowestmode = self['geolocation/elev_lowestmode'][()]
        digital_elevation_model = self['geolocation/digital_elevation_model'][()]
        
        # Initialize the data dictionary
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "elevation_difference_tdx": elev_lowestmode - digital_elevation_model,
        }
    
        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            if key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = np.array([beam_type] * self.n_shots)
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            elif key == "dz":
                data[key] = np.repeat(self[sds_name][()], self.n_shots)
            elif key == "waveform_start":
                data[key] = np.array(self[sds_name][()] - 1)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply filter and get the filtered indices
        self._filtered_index = self.apply_filter(data, filters=DEFAULT_QUALITY_FILTERS)
        
        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}
        
        return filtered_data if filtered_data else None
        

            
        