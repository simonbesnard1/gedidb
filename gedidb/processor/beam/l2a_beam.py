import numpy as np
import pandas as pd
import geopandas as gpd

from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.utils.constants import WGS84

DEFAULT_QUALITY_FILTERS = {
                            'quality_flag': lambda data: data['quality_flag'] == 1,
                            'sensitivity_a0': lambda data: (data['sensitivity_a0'] >= 0.9) & (data['sensitivity_a0'] <= 1.0),
                            'sensitivity_a2': lambda data: (data['sensitivity_a2'] > 0.95) & (data['sensitivity_a2'] <= 1.0),
                            'degrade_flag': lambda data: np.isin(data['degrade_flag'], [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]),
                            'surface_flag': lambda data: data['surface_flag'] == 1,
                            'elevation_difference_tdx': lambda data: (data['elevation_difference_tdx'] > -150) & (data['elevation_difference_tdx'] < 150),
                        }

class L2ABeam(Beam):
    def __init__(self, granule: Granule, beam: str, field_mapping: dict):
        super().__init__(granule, beam, field_mapping)
        self._shot_geolocations = None
        self._filtered_index = None

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['lon_lowestmode'],
                y=self['lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data(self) -> pd.DataFrame:
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        delta_time = self["delta_time"][()]
        elev_lowestmode = self['elev_lowestmode'][()]
        digital_elevation_model = self['digital_elevation_model'][()]

        # Initialize the data dictionary with NumPy arrays
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "elevation_difference_tdx": elev_lowestmode - digital_elevation_model
        }

        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            if key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = np.array([beam_type] * self.n_shots)
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply filter and get the filtered indices
        self._filtered_index = self.apply_filter(data, filters=DEFAULT_QUALITY_FILTERS)
        
        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}
        
        return filtered_data if filtered_data else None
