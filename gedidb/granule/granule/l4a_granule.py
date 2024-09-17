from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.l4a_beam import L4ABeam
from gedidb.granule.beam.beam import Beam


class L4AGranule(Granule):
    """
    Represents a GEDI Level 4A granule, providing access to its beams and related data.

    This class extends the base Granule class, specifically tailored for Level 4A data,
    and initializes with a file path and a field mapping to access product variables.

    Attributes:
        field_mapping (dict): A dictionary that maps product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: dict):
        """
        Initialize an L4AGranule object.

        Parameters:
            file_path (str): Path to the GEDI Level 4A granule file (HDF5 format).
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
            L4ABeam: The corresponding L4ABeam object for the given beam name.

        Raises:
            ValueError: If the specified beam name is not found in the granule.
        """
        if beam not in self.beam_names:
            raise ValueError(f"Invalid beam name '{beam}'. Must be one of {self.beam_names}.")
        
        return L4ABeam(self, beam, self.field_mapping)
