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
        filtered["elevation_difference_tdx"] = (filtered["elev_lowestmode"] - filtered["digital_elevation_model"])

        self._cached_data = filtered

    def _get_main_data_dict(self) -> dict:

        gedi_l2a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        data = {}
        
        # Populate data from general_data section
        for key, source in self.field_mapper["general_data"].items():
            if key == "granule_name":
                # Handle special case for granule_name
                data[key] = [getattr(self.parent_granule, source.split('.')[-1])] * self.n_shots
            elif key in ["beam_type", "beam_name"]:
                # Handle special cases for beam_type and beam_name
                data[key] = [getattr(self, source)] * self.n_shots
            elif source.startswith("parent_granule"):
                # Handle other parent_granule related fields
                data[key] = getattr(self.parent_granule, source.split('.')[-1])
            elif hasattr(self, source):
                # Handle attributes of self
                data[key] = getattr(self, source)
            else:
                # Default case: Access as if it's a dataset
                data[key] = self[source][:]
        
        # Populate data from rh_data section
        rh_data = {}
        for rh_field in self.field_mapper["rh_data"]:
            rh_index = int(rh_field.split("_")[1])
            rh_data[rh_field] = self["rh"][:, rh_index]
    
        data.update(rh_data)
    
        # Convert delta_time to absolute_time if present
        if "absolute_time" in self.field_mapper["general_data"]:
            delta_time_source = self.field_mapper["general_data"]["absolute_time"]
            data["absolute_time"] = gedi_l2a_count_start + pd.to_timedelta(self[delta_time_source], unit="seconds")
    
        return data

    # def _get_main_data_dict(self) -> dict:
    #     gedi_l2a_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
    #     data = {
    #                 # General identifiable data
    #                 "granule_name": [self.parent_granule.filename] * self.n_shots,
    #                 "shot_number": self["shot_number"][:],
    #                 "beam_type": [self.beam_type] * self.n_shots,
    #                 "beam_name": [self.name] * self.n_shots,
    #                 # Temporal data
    #                 "delta_time": self["delta_time"][:],
    #                 "absolute_time": (gedi_l2a_count_start + pd.to_timedelta(self["delta_time"], unit="seconds")),
    #                 # Quality data
    #                 "sensitivity_a0": self["sensitivity"][:],
    #                 "sensitivity_a1": self["geolocation/sensitivity_a1"][:],
    #                 "sensitivity_a2": self["geolocation/sensitivity_a2"][:],
    #                 "sensitivity_a3": self["geolocation/sensitivity_a3"][:],
    #                 "sensitivity_a4": self["geolocation/sensitivity_a4"][:],
    #                 "sensitivity_a5": self["geolocation/sensitivity_a5"][:],
    #                 "sensitivity_a6": self["geolocation/sensitivity_a6"][:],
    #                 "quality_flag": self["quality_flag"][:],
    #                 "degrade_flag": self["degrade_flag"][:],
    #                 "solar_elevation": self["solar_elevation"][:],
    #                 "solar_azimuth": self["solar_elevation"][:],
    #                 "energy_total": self["energy_total"][:],
    #                 "surface_flag": self["surface_flag"][:],
    #                 # DEM
    #                 "digital_elevation_model": self["digital_elevation_model"][:],
    #                 "digital_elevation_model_srtm": self["digital_elevation_model_srtm"][:],
    #                 # Processing data
    #                 "selected_algorithm": self["selected_algorithm"][:],
    #                 "selected_mode": self["selected_mode"][:],
    #                 # Geolocation data
    #                 "lon_lowestmode": self["lon_lowestmode"][:],
    #                 "longitude_bin0_error": self["longitude_bin0_error"][:],
    #                 "lat_lowestmode": self["lat_lowestmode"][:],
    #                 "latitude_bin0_error": self["latitude_bin0_error"][:],
    #                 "elev_lowestmode": self["elev_lowestmode"][:],
    #                 "elevation_bin0_error": self["elevation_bin0_error"][:],
    #                 "lon_highestreturn": self["lon_highestreturn"][:],
    #                 "lat_highestreturn": self["lat_highestreturn"][:],
    #                 "elev_highestreturn": self["elev_highestreturn"][:],
    #             } | {
    #                 f"rh_{i}": self["rh"][:, i] for i in range(101)
    #             }

    #     return data
