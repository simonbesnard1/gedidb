import pandas as pd
import geopandas as gpd
import numpy as np
import os

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L4ABeam(Beam):

    def __init__(self,granule: Granule, beam: str, quality_flag:dict, field_mapping:dict, geom: gpd.GeoSeries):
        
        super().__init__(granule, beam, quality_flag, field_mapping, geom)
        
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        self._shot_geolocations = gpd.points_from_xy(
            x=self["lon_lowestmode"],
            y=self["lat_lowestmode"],
            crs=WGS84,
        )
        return self._shot_geolocations

    def apply_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        
        if self.quality_filter != "None":
            
            for key, condition in self.quality_filter.items():
                if key == 'drop':
                    continue  # Skip dropping columns here
        
                # Handle simple conditions
                if key != 'complex_conditions' and isinstance(condition, list):
                    for cond in condition:
                        data = data.query(f"{key} {cond}")
                elif key != 'complex_conditions':
                    data = data.query(f"{key} {condition}")
            
            # Handle complex conditions
            if 'complex_conditions' in self.quality_filter:
                for complex_condition in self.quality_filter['complex_conditions']:
                    data = data.query(complex_condition)
        
            # Drop the specified columns after filtering
            data = data.drop(columns=self.quality_filter.get('drop', []))

        # Store the filtered indices for further use if needed
        filtered_index = data.index  
        self._filtered_index = filtered_index  
        
        return data
    
    def _get_main_data(self) -> dict:
        
        spatial_mask = np.array(self.spatial_mask, dtype=bool)
        filtered_n_shots = np.sum(spatial_mask)  # Count of True values in spatial_mask
        
        if filtered_n_shots > 0:

            data = {}        
    
            # Populate data from general_data section
            for key, source in self.field_mapper.items():
                if key in ["beam_type"]:
                    # Handle special cases for beam_type 
                    data[key] = [getattr(self, source['SDS_Name'])] * filtered_n_shots
                elif key in ["beam_name"]:                
                    # Handle special cases for beam_name
                    data[key] = [self.name] * filtered_n_shots
                elif key in "waveform_start":
                    # Handle special cases for waveform_start 
                    data[key] = self[source['SDS_Name']][(spatial_mask)] - 1
                else:
                    # Default case: Access as if it's a dataset
                    data[key] = self[source['SDS_Name']][(spatial_mask)]
                    
            gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
            data["absolute_time"] = (gedi_count_start + pd.to_timedelta(self["delta_time"][(spatial_mask)], unit="seconds"))

            data = self.apply_filter(pd.DataFrame(data))
            
            if not data.empty:
                return data
        
            

    
                        
