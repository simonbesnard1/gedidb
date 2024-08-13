from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.l2a_beam import L2ABeam
from gedidb.processor.beam.beam import Beam


class L2AGranule(Granule):

    def __init__(self, file_path, quality_flag, field_mapping):
        super().__init__(file_path)
        
        self.quality_flag = quality_flag
        self.field_mapping = field_mapping
        
    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L2ABeam(self, beam, self.quality_flag, self.field_mapping)

