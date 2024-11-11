# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
from typing import Dict, Optional

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.beam import Beam

class L4ABeam(Beam):
    """
    Represents a Level 4A (L4A) GEDI beam and processes the beam data.
    This class extracts geolocation, time, and elevation data, applies quality filters,
    and returns the filtered beam data as a dictionary.
    """

    def __init__(self, granule: Granule, beam: str, field_mapping: Dict[str, Dict[str, str]]):
        """
        Initialize the L4ABeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, Dict[str, str]]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)
        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = None
        # self.DEFAULT_QUALITY_FILTERS = {
        #     'l2_quality_flag': lambda: self["l2_quality_flag"][()] == 1,
        #     'sensitivity_a0': lambda: (self['sensitivity'][()] >= 0.9) & (self['sensitivity'][()] <= 1.0),
        #     'sensitivity_a2': lambda: (self['geolocation/sensitivity_a2'][()] >= 0.9) & (self['geolocation/sensitivity_a2'][()] <= 1.0),
        #     'pft_sensitivity_filter': lambda: (
        #         (self['land_cover_data/pft_class'][()] == 2) & (self['geolocation/sensitivity_a2'][()] > 0.98)) |
        #         ((self['land_cover_data/pft_class'][()] != 2) & (self['geolocation/sensitivity_a2'][()] > 0.95))
        # }


    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and other variables.
        This method applies quality filters to the data.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """

        # Initialize the data dictionary
        data = {}

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source['SDS_Name']
            # this is needed for tests, see test_gedi_granules.py
            if key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store the filtered index
        self._filtered_index = self.apply_filter(data, filters=self.DEFAULT_QUALITY_FILTERS)

        # Filter the data using the mask
        filtered_data = {key: value[self._filtered_index] for key, value in data.items()}

        return filtered_data if filtered_data else None
