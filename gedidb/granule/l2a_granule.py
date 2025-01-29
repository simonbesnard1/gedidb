# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from typing import Dict

from gedidb.granule.Granule import granule_handler
from gedidb.beam.l2a_beam import L2ABeam
from gedidb.beam.Beam import beam_handler


class L2AGranule(granule_handler):
    """
    Represents a GEDI Level 2A granule, providing access to its beams and related data.

    This class extends the base Granule class and initializes with a specific file path and
    a field mapping that maps product variables to the corresponding data fields in the granule.

    Attributes:
        field_mapping (Dict[str, str]): A dictionary mapping product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: Dict[str, str]):
        """
        Initialize an L2AGranule object.

        Parameters:
            file_path (str): Path to the GEDI Level 2A granule file (HDF5 format).
            field_mapping (Dict[str, str]): Dictionary containing the mapping of product variables to data fields.
        """
        super().__init__(file_path)
        self.file_path = file_path
        self.field_mapping = field_mapping

    def validate_beam_name(self, beam: str) -> None:
        """
        Validate that the provided beam name exists in the granule.

        Parameters:
            beam (str): The name of the beam to validate.

        Raises:
            ValueError: If the specified beam name is not found in the granule.
        """
        if beam not in self.beam_names:
            raise ValueError(
                f"Invalid beam name '{beam}' in file '{self.file_path}'. "
                f"Valid beam names: {self.beam_names}. Ensure the beam exists in the granule."
            )

    def _beam_from_name(self, beam: str) -> beam_handler:
        """
        Retrieve a specific beam from the granule by name.

        Parameters:
            beam (str): The name of the beam to retrieve (e.g., "BEAM0000").

        Returns:
            L2ABeam: The corresponding L2ABeam object for the given beam name.
        """
        self.validate_beam_name(beam)
        return L2ABeam(self, beam, self.field_mapping)
