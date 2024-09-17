import pandas as pd
import numpy as np
import geopandas as gpd
from typing import Optional, Dict

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.beam import Beam
from gedidb.utils.constants import WGS84


# Default quality filters applied to the L2B beam data
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
    """
    Represents a Level 2B (L2B) GEDI beam and processes the beam data.
    This class extracts geolocation, time, and elevation data, applies quality filters,
    and returns the filtered beam data as a dictionary.
    """
    
    def __init__(self, granule: Granule, beam: str, field_mapping: Dict[str, Dict[str, str]]):
        """
        Initialize the L2BBeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, Dict[str, str]]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)
        self._shot_geolocations: Optional[gpd.array.GeometryArray] = None  # Cache for geolocations
        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices

    @property
    def shot_geolocations(self) -> gpd.array.GeometryArray:
        """
        Get the geolocations (latitude/longitude) of shots in the beam.
        This property lazily loads and caches the geolocations.

        Returns:
            gpd.array.GeometryArray: The geolocations of the shots in the beam.
        """
        if self._shot_geolocations is None:
            self._shot_geolocations = gpd.points_from_xy(
                x=self['geolocation/lon_lowestmode'],
                y=self['geolocation/lat_lowestmode'],
                crs=WGS84,
            )
        return self._shot_geolocations

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and elevation differences.
        This method applies quality filters to the data.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """
        gedi_count_start = pd.to_datetime('2018-01-01T00:00:00Z')
        delta_time = self["delta_time"][()]
        elev_lowestmode = self['geolocation/elev_lowestmode'][()]
        digital_elevation_model = self['geolocation/digital_elevation_model'][()]

        # Initialize the data dictionary with calculated fields
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "elevation_difference_tdx": elev_lowestmode - digital_elevation_model,
        }

        # Populate data dictionary with fields from field mapping
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
                data[key] = np.array(self[sds_name][()] - 1)  # Adjusting waveform start
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store the filtered index
        self._filtered_index = self.apply_filter(data, filters=DEFAULT_QUALITY_FILTERS)

        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}

        return filtered_data if filtered_data else None
