# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
from typing import Optional, Dict

from gedidb.granule.Granule import granule_handler
from gedidb.beam.Beam import beam_handler


class L2BBeam(beam_handler):
    """
    Represents a Level 2B (L2B) GEDI beam and processes the beam data.
    This class extracts geolocation, time, and elevation data, applies quality filters,
    and returns the filtered beam data as a dictionary.
    """

    def __init__(
        self, granule: granule_handler, beam: str, field_mapping: Dict[str, Dict[str, str]]
    ):
        """
        Initialize the L2BBeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, Dict[str, str]]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)

        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = {
            "water_persistence": lambda: self["land_cover_data/landsat_water_persistence"][()] < 10,
            "urban_proportion": lambda: self["land_cover_data/urban_proportion"][()] < 50,
        }

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
            sds_name = source["SDS_Name"]
            if key == "dz":
                data[key] = np.repeat(self[sds_name][()], self.n_shots)
            elif key == "waveform_start":
                data[key] = np.array(self[sds_name][()] - 1)  # Adjusting waveform start
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store the filtered index
        self._filtered_index = self.apply_filter(
            data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data using the mask
        filtered_data = {
            key: value[self._filtered_index] for key, value in data.items()
        }

        return filtered_data if filtered_data else None
