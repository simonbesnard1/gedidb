# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import pandas as pd
import geopandas as gpd
from typing import Dict, Optional

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.beam import Beam
from gedidb.utils.constants import WGS84

class L2ABeam(Beam):
    """
    Represents a Level 2A (L2A) GEDI beam and processes the beam data.
    This class extracts geolocation and elevation data, applies quality filters, 
    and returns the filtered beam data as a DataFrame.
    """
    
    def __init__(self, granule: Granule, beam: str, field_mapping: Dict[str, str]):
        """
        Initialize the L2ABeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, str]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)
        self._shot_geolocations: Optional[gpd.array.GeometryArray] = None  # Cache for geolocations
        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = {
            'quality_flag': lambda: self["quality_flag"][()] == 1,
            'sensitivity_a0': lambda: (self['sensitivity'][()] >= 0.9) & (self['sensitivity'][()] <= 1.0),
            'sensitivity_a2': lambda: (self['geolocation/sensitivity_a2'][()] > 0.95) & (self['geolocation/sensitivity_a2'][()] <= 1.0),
            'degrade_flag': lambda: np.isin(self['degrade_flag'][()], [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]),
            'surface_flag': lambda: self['surface_flag'][()] == 1,
            'elevation_difference_tdx': lambda: ((self['elev_lowestmode'][()] - self['digital_elevation_model'][()]) > -150) & ((self['elev_lowestmode'][()] - self['digital_elevation_model'][()]) < 150),
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
                x=self['lon_lowestmode'],
                y=self['lat_lowestmode'],
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
        
        # Initialize the data dictionary with calculated fields
        data = {
            "absolute_time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds")
        }

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            if key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = np.array([beam_type] * self.n_shots)
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store filtered index
        self._filtered_index = self.apply_filter(data, filters=self.DEFAULT_QUALITY_FILTERS)
        
        # Filter the data based on the quality filters
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}

        return filtered_data if filtered_data else None
