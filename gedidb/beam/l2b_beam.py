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
from gedidb.utils.profiles import pgap_to_vertical_profiles


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

        # 1) Populate base fields from field mapping
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

        # 2) Apply quality filters to get a mask over shots
        self._filtered_index = self.apply_filter(
            data, filters=self.DEFAULT_QUALITY_FILTERS
        )

        # 3) Slice only arrays whose first dimension is n_shots; leave others intact (e.g., height bins (nz,))
        filtered_data: Dict[str, np.ndarray] = {}
        for key, arr in data.items():
            a = np.asarray(arr)
            if a.ndim > 0 and a.shape[0] == self.n_shots:
                filtered_data[key] = a[self._filtered_index]
            else:
                filtered_data[key] = a  # keep globals like height(z) unchanged

        # Early exit if nothing left
        if not filtered_data or (
            isinstance(self._filtered_index, np.ndarray)
            and self._filtered_index.size
            and not self._filtered_index.any()
        ):
            return None

        # 4) Compute vertical profiles only if asked for in field_mapper
        requested_profile_keys = {"cover_z", "pai_z", "pavd_z"}
        need_profiles = any(k in self.field_mapper for k in requested_profile_keys)

        if need_profiles:

            def _get_arr(field_key: str) -> np.ndarray:
                """
                Fetch array directly from data or from the file.
                Only slices if first dimension is n_shots.
                """
                if field_key in filtered_data:
                    return filtered_data[field_key]

                if field_key in data:
                    arr = data[field_key]
                else:
                    # direct SDS read
                    arr = np.array(self[field_key][()])

                if arr.ndim > 0 and arr.shape[0] == self.n_shots:
                    return arr[self._filtered_index]
                return arr

            # Required inputs
            pgap = _get_arr("pgap_theta_z")

            # Height bin centers (1D)
            height = _get_arr("rh100")

            # Elevation
            elev = _get_arr("geolocation/local_beam_elevation")

            # Scalars: rossg (G) and omega (clumping)
            rossg = _get_arr("rossg")
            omega = _get_arr("omega")

            cover_z, pai_z, pavd_z = pgap_to_vertical_profiles(
                pgap_theta_z=pgap,
                height=height,
                local_beam_elevation=elev,
                rossg=float(rossg),
                omega=float(omega),
                out_dtype=np.float32,
            )

            # Attach only what was requested
            if "cover_z" in self.field_mapper:
                filtered_data["cover_z"] = cover_z
            if "pai_z" in self.field_mapper:
                filtered_data["pai_z"] = pai_z
            if "pavd_z" in self.field_mapper:
                filtered_data["pavd_z"] = pavd_z

        return filtered_data if filtered_data else None
