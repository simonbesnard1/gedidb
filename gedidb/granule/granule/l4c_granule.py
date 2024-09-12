from gedidb.granule.granule.granule import Granule
from gedidb.granule.beam.l4c_beam import L4CBeam
from gedidb.granule.beam.beam import Beam


class L4CGranule(Granule):

    def __init__(self, file_path, field_mapping):
        super().__init__(file_path)
        
        self.field_mapping = field_mapping
        
    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L4CBeam(self, beam, self.field_mapping)

