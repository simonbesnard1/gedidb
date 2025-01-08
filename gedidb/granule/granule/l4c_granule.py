# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de, besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from typing import Dict

from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.l4c_beam import L4CBeam
from gedidb.granule.beam.beam import Beam


class L4CGranule(Granule):
    """
    Represents a GEDI Level 4C granule, providing access to its beams and related data.
    
    This class extends the base Granule class and initializes with a specific file path and
    a field mapping that maps product variables to the corresponding data fields in the granule.
    
    Attributes:
        field_mapping (Dict[str, str]): A dictionary mapping product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: Dict[str, str]):
        """
        Initialize an L4CGranule object.

        Parameters:
            file_path (str): Path to the GEDI Level 4C granule file (HDF5 format).
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

    def _beam_from_name(self, beam: str) -> Beam:
        """
        Retrieve a specific beam from the granule by name.
        
        Parameters:
            beam (str): The name of the beam to retrieve (e.g., "BEAM0000").

        Returns:
            L4CBeam: The corresponding L4CBeam object for the given beam name.
        """
        self.validate_beam_name(beam)
        return L4CBeam(self, beam, self.field_mapping)
