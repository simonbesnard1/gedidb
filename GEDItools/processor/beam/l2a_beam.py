import pandas as pd
from GEDItools.processor.granule.granule import Granule
from GEDItools.processor.beam.beam import Beam

class L2ABeam(Beam):

    def __init__(self, granule: Granule, beam: str, quality_flag:dict, field_mapping:dict):
        
        super().__init__(granule, beam, quality_flag, field_mapping)
    
    def apply_filter(self, data):

        for key, value in self.quality_filter.items():
            if key == 'drop':
                return
            if isinstance(value, list):
                for v in value:
                    data = data.query(f"{key} {v}")
            else:
                data = data.query(f"{key} {value}")

        data = data.drop(self.quality_filter['drop'], axis=1)
        
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
        
        data = self.apply_filter(pd.DataFrame(data))
        
        return data

