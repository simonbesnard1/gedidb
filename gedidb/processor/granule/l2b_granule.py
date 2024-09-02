from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.beam import Beam
from gedidb.processor.beam.l2b_beam import L2BBeam


class L2BGranule(Granule):

    def __init__(self, file_path, field_mapping):
        super().__init__(file_path)
        
        self.field_mapping = field_mapping
        
    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L2BBeam(self, beam, self.field_mapping)
