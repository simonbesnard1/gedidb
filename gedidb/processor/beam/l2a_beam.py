import pandas as pd
import geopandas as gpd
import os

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L2ABeam(Beam):

    def __init__(self,granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        self._shot_geolocations = gpd.points_from_xy(
            x=self['lon_lowestmode'],
            y=self['lat_lowestmode'],
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
        
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        delta_time = self["delta_time"][()]
        elev_lowestmode = self['elev_lowestmode'][()]
        digital_elevation_model = self['digital_elevation_model'][()]
        
        # Initialize the data dictionary
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "elevation_difference_tdx": elev_lowestmode - digital_elevation_model
        }
        
        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            
            if key == "granule_name":
                granule_name = os.path.basename(os.path.dirname(getattr(self.parent_granule, sds_name.split('.')[-1])))
                data[key] = [granule_name] * self.n_shots
            elif key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = [beam_type] * self.n_shots
            elif key == "beam_name":
                data[key] = [self.name] * self.n_shots
            elif key == "rh":
                data[key] = self[sds_name][()].tolist()
            else:
                data[key] = self[sds_name][()]
        
        # Apply filter and convert to DataFrame
        data = self.apply_filter(pd.DataFrame(data))
    
        if not data.empty:
            return data        

    
        