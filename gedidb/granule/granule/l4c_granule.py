# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.l4c_beam import L4CBeam
from gedidb.granule.beam.beam import Beam


class L4CGranule(Granule):
    """
    Represents a GEDI Level 4C granule, providing access to its beams and related data.

    This class extends the base Granule class, specifically tailored for Level 4C data,
    and initializes with a file path and a field mapping to access product variables.

    Attributes:
        field_mapping (dict): A dictionary that maps product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: dict):
        """
        Initialize an L4CGranule object.

        Parameters:
            file_path (str): Path to the GEDI Level 4C granule file (HDF5 format).
            field_mapping (dict): Dictionary containing the mapping of product variables to data fields.
        """
        super().__init__(file_path)
        self.field_mapping = field_mapping

    def _beam_from_name(self, beam: str) -> Beam:
        """
        Retrieve a specific beam from the granule by name.

        Parameters:
            beam (str): The name of the beam to retrieve (e.g., "BEAM0000").

        Returns:
            L4CBeam: The corresponding L4CBeam object for the given beam name.

        Raises:
            ValueError: If the specified beam name is not found in the granule.
        """
        if beam not in self.beam_names:
            raise ValueError(f"Invalid beam name '{beam}'. Must be one of {self.beam_names}.")
        
        return L4CBeam(self, beam, self.field_mapping)
