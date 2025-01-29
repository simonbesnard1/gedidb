# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import pandas as pd
from typing import Dict, Optional

from gedidb.granule.Granule import granule_handler
from gedidb.beam.Beam import beam_handler


class L2ABeam(beam_handler):
    """
    Represents a Level 2A (L2A) GEDI beam and processes the beam data.
    This class extracts geolocation and elevation data, applies quality filters,
    and returns the filtered beam data as a DataFrame.
    """

    def __init__(self, granule: granule_handler, beam: str, field_mapping: Dict[str, str]):
        """
        Initialize the L2ABeam class.

        Args:
            granule (Granule): The parent granule object.
            beam (str): The beam name within the granule.
            field_mapping (Dict[str, str]): A dictionary mapping fields to SDS names.
        """
        super().__init__(granule, beam, field_mapping)

        self._filtered_index: Optional[np.ndarray] = None  # Cache for filtered indices
        self.DEFAULT_QUALITY_FILTERS = {
                                        "quality_flag": lambda: self["quality_flag"][()] == 1,
                                        "sensitivity_a0": lambda: (
                                            (self["sensitivity"][()] >= 0.5) & (self["sensitivity"][()] <= 1.0)
                                        ),
                                        "sensitivity_a2": lambda: (
                                            (self["geolocation/sensitivity_a2"][()] > 0.7) & (self["geolocation/sensitivity_a2"][()] <= 1.0)
                                        ),
                                        "degrade_flag": lambda: np.isin(
                                            self["degrade_flag"][()],
                                            [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]
                                        ),
                                        "surface_flag": lambda: self["surface_flag"][()] == 1,
                                        "elevation_difference_tdx": lambda: (
                                            (self["elev_lowestmode"][()] - self["digital_elevation_model"][()]) > -150
                                        ) & (
                                            (self["elev_lowestmode"][()] - self["digital_elevation_model"][()]) < 150
                                        ),
                                    }


    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and elevation differences.
        This method applies quality filters to the data.

        Returns:
            Optional[Dict[str, np.ndarray]]: The filtered data as a dictionary or None if no data is present.
        """
        # Define GEDI mission start time and calculate actual timestamps
        gedi_count_start = pd.to_datetime("2018-01-01T00:00:00Z")
        delta_time = self["delta_time"][()]

        # Initialize the data dictionary with calculated fields
        data = {
            "time": gedi_count_start + pd.to_timedelta(delta_time, unit="seconds"),
            "longitude": self["lon_lowestmode"][()],
            "latitude": self["lat_lowestmode"][()],
        }

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if key == "beam_type":
                beam_type = getattr(self, sds_name)
                data[key] = np.array([beam_type] * self.n_shots)
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # Apply quality filters and store filtered index
        self._filtered_index = self.apply_filter(
            data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data based on the quality filters
        filtered_data = {
            key: value[self._filtered_index] for key, value in data.items()
        }

        return filtered_data if filtered_data else None
