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


class L4CBeam(Beam):
    """
    Represents a Level 4C (L4C) GEDI beam and processes the beam data.
    This class extracts geolocation, time, and additional variables from the beam
    and applies quality filters (if provided).
    """

    def __init__(
        self, granule: Granule, beam: str, field_mapping: Dict[str, Dict[str, str]]
    ):
        """
        Initialize the L4CBeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, Dict[str, str]]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)
        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = None

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and other variables.
        This method applies quality filters if they are defined.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """
        # Initialize the data dictionary with time and geolocation fields
        data = {}

        # Populate data dictionary with fields from the field mapping
        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply filter and get the filtered indices
        self._filtered_index = self.apply_filter(
            data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data using the mask
        filtered_data = {
            key: value[self._filtered_index] for key, value in data.items()
        }

        return filtered_data if filtered_data else None
