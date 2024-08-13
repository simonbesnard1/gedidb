import pandas as pd
import numpy as np
import geopandas as gpd
from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L2BBeam(Beam):

    def __init__(self, granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['geolocation/lon_lowestmode'],
                y=self['geolocation/lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def apply_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        
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
            elif key in ["cover_z", "pai_z", "pavd_z"]:
                # Handle special cases for cover_z and pai_z
                data[key] = self[source][:].tolist()
            elif key in "dz":
                # Special treatment for keys ending with _z
                data[key] = np.repeat(self[source][:], self.n_shots)
            elif key in "waveform_start":
                # Handle special cases for waveform_start 
                data[key] = self[source][:] - 1
            elif key in ["absolute_time"]:     
                gedi_l2b_count_start = pd.to_datetime(source)
                data[key] = (gedi_l2b_count_start + pd.to_timedelta(self["delta_time"], unit="seconds"))
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][:]
                
        data["elevation_difference_tdx"] = (self['geolocation/elev_lowestmode'][:] - self['geolocation/digital_elevation_model'][:])

        data = self.apply_filter(pd.DataFrame(data))
        
        return data
