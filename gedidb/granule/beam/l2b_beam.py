# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import geopandas as gpd
from typing import Optional, Dict

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.beam import Beam
from gedidb.utils.constants import WGS84

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
        self.DEFAULT_QUALITY_FILTERS = {
            'l2a_quality_flag': lambda: self["l2a_quality_flag"][()] == 1,
            'l2b_quality_flag': lambda: self["l2b_quality_flag"][()] == 1,
            'sensitivity': lambda: (self['sensitivity'][()] >= 0.9) & (self['sensitivity'][()] <= 1.0),
            'rh100': lambda: (self['rh100'][()] >= 0) & (self['rh100'][()] <= 1200),
            'water_persistence': lambda: self["land_cover_data/landsat_water_persistence"][()] <10,
            'urban_proportion': lambda: self['land_cover_data/urban_proportion'][()] <50,
        }

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
        # Initialize the data dictionary
        data = {}

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
        self._filtered_index = self.apply_filter(data, filters=self.DEFAULT_QUALITY_FILTERS)
            
        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}

        return filtered_data if filtered_data else None
