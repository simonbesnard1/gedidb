from gedidb.granule.beam.beam import Beam
from gedidb.granule.beam.l1b_beam import L1BBeam
from gedidb.granule.granule.granule import Granule


class L1BGranule(Granule):

    def __init__(self, file_path, field_mapping):
        super().__init__(file_path)
        
        self.field_mapping = field_mapping
        
    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L1BBeam(self, beam, self.field_mappings)