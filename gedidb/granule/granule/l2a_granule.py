from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.l2a_beam import L2ABeam
from gedidb.granule.beam.beam import Beam


class L2AGranule(Granule):
    """
    Represents a GEDI Level 2A granule, providing access to its beams and related data.
    
    This class extends the base Granule class, and initializes with a specific file path and
    a field mapping that maps product variables to the corresponding data fields in the granule.
    
    Attributes:
        field_mapping (dict): A dictionary mapping product variables to HDF5 field names.
    """

    def __init__(self, file_path: str, field_mapping: dict):
        """
        Initialize an L2AGranule object.

        Parameters:
            file_path (str): Path to the GEDI Level 2A granule file (HDF5 format).
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
            L2ABeam: The corresponding L2ABeam object for the given beam name.
        
        Raises:
            ValueError: If the specified beam name is not found in the granule.
        """
        if beam not in self.beam_names:
            raise ValueError(f"Invalid beam name '{beam}'. Must be one of {self.beam_names}.")
        
        return L2ABeam(self, beam, self.field_mapping)
