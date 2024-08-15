import pandas as pd
import geopandas as gpd
#import numpy as np

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84


class L2ABeam(Beam):

    def __init__(self,granule: Granule, beam: str, quality_flag:dict, field_mapping:dict, geom: gpd.GeoSeries):
        
        super().__init__(granule, beam, quality_flag, field_mapping, geom)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['lon_lowestmode'],
                y=self['lat_lowestmode'],
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

        # spatial_box = self.geom.total_bounds  # [minx, miny, maxx, maxy]
        
        # # Extract x and y coordinates from shot_geolocations
        # longitudes_lastbin = self.shot_geolocations.x
        # latitudes_lastbin = self.shot_geolocations.y
                 
        # spatial_mask = np.logical_and(np.logical_and(longitudes_lastbin >= spatial_box[0], longitudes_lastbin <= spatial_box[2]),
        #                               np.logical_and(latitudes_lastbin >= spatial_box[1], latitudes_lastbin <= spatial_box[3]))
        # # Filter shot_geolocations and other attributes using the spatial mask
        # filtered_n_shots = np.sum(spatial_mask)  # Count of True values in spatial_mask
        
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
            elif key in ["rh_data"]: 
                for i in range(source + 1):
                    rh_key = f"rh_{i}"
                    data[rh_key] = self["rh"][:, i]
            elif key in ["absolute_time"]:                      
                # Handle special cases for beam_name
                gedi_l2a_count_start = pd.to_datetime(source)
                data[key] = (gedi_l2a_count_start + pd.to_timedelta(self["delta_time"][()], unit="seconds"))
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][()] 
            
            data["elevation_difference_tdx"] = (self['elev_lowestmode'][()] - self['digital_elevation_model'][()])
            
        data = self.apply_filter(pd.DataFrame(data))
        
        return data

