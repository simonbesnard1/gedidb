# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from typing import Dict, Optional

import numpy as np

from gedidb.beam.Beam import beam_handler
from gedidb.granule.Granule import granule_handler
from gedidb.utils.profiles import GEDIVerticalProfiler


class L2BBeam(beam_handler):
    """
    Represents a Level 2B (L2B) GEDI beam and processes the beam data.
    This class extracts geolocation, time, and elevation data, applies quality filters,
    and returns the filtered beam data as a dictionary.
    """

    def __init__(
        self,
        granule: granule_handler,
        beam: str,
        field_mapping: Dict[str, Dict[str, str]],
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
            "water_persistence": lambda: self[
                "land_cover_data/landsat_water_persistence"
            ][()]
            < 10,
            "urban_proportion": lambda: self["land_cover_data/urban_proportion"][()]
            < 50,
        }

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Extract the main data for the beam, including time and elevation differences.
        This method applies quality filters to the data and optionally computes
        vertical profiles (cover_z, pai_z, pavd_z) from pgap if requested.
        """
        data: Dict[str, np.ndarray] = {}

        # Populate data dictionary with fields from field mapping
        for key, source in self.field_mapper.items():
            sds_name = source["SDS_Name"]
            if key == "dz":
                data[key] = np.repeat(self[sds_name][()], self.n_shots)
            elif key == "waveform_start":
                data[key] = np.array(self[sds_name][()] - 1)  # 0-based start
            elif key == "beam_name":
                data[key] = np.array([self.name] * self.n_shots)
            else:
                data[key] = np.array(self[sds_name][()])

        # --- only compute CP stuff if any cp_* key is requested ---
        cp_keys = {"cp_cover", "cp_pai", "cp_pavd", "cp_height"}
        if any(k in self.field_mapper for k in cp_keys):
            prof = GEDIVerticalProfiler()

            # read pgap + per-shot height-above-ground grids (both shaped (n_shots, nz))
            pgap, height = prof.read_pgap_theta_z(self)

            # compute profiles (elevation can be scalar or (n_shots,))
            elev = np.asarray(
                self["geolocation/local_beam_elevation"][()], dtype=np.float32
            )
            rossg = np.asarray(self["rossg"][()], dtype=np.float32)
            omega = np.asarray(self["omega"][()], dtype=np.float32)

            cov_rh, pai_rh, pavd_rh, height_ag, H = prof.compute_profiles(
                pgap, height, elev, rossg, omega
            )

            # attach *only* those requested
            if "cp_cover" in self.field_mapper:
                data["cp_cover"] = cov_rh
            if "cp_pai" in self.field_mapper:
                data["cp_pai"] = pai_rh
            if "cp_pavd" in self.field_mapper:
                data["cp_pavd"] = pavd_rh
            if "cp_height" in self.field_mapper:
                data["cp_height"] = height_ag

        # Apply quality filters and store filtered index
        self._filtered_index = self.apply_filter(
            data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # Filter the data based on the quality filters
        filtered_data = {
            key: value[self._filtered_index] for key, value in data.items()
        }

        return filtered_data if filtered_data else None
