import pandas as pd
import geopandas as gpd
from GEDItools.processor.granule.granule import Granule
from GEDItools.processor.beam.beam import Beam
from GEDItools.utils.constants import WGS84


class L2ABeam(Beam):

    def __init__(self, granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['lon_lowestmode'],
                y=self['lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def quality_filter(self):
        filtered = self.main_data

        quality_filters = self.quality_filter()

        for key, value in quality_filters.items():
            if key == 'drop':
                return
            if isinstance(value, list):
                for v in value:
                    filtered = filtered.query(f"{key} {v}")
            else:
                filtered = filtered.query(f"{key} {value}")

        filtered = filtered.drop(quality_filters['drop'], axis=1)
        
        self._cached_data = filtered

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
            elif key in ["rh_data"]: 
                for i in range(source + 1):
                    rh_key = f"rh_{i}"
                    data[rh_key] = self["rh"][:, i]
            elif key in ["absolute_time"]:                
                # Handle special cases for beam_name
                gedi_l2a_count_start = pd.to_datetime(source)
                data[key] = (gedi_l2a_count_start + pd.to_timedelta(self["delta_time"], unit="seconds"))
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][:] 
        
        data["elevation_difference_tdx"] = (self['elev_lowestmode'][:] - self['digital_elevation_model'][:])

        return data

