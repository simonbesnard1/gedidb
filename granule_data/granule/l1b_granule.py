from granule_data.beam.beam import Beam
from granule_data.beam.l1b_beam import L1BBeam
from granule_data.granule.granule import Granule


class L1BGranule(Granule):

    def __init__(self, file_path):
        super().__init__(file_path)

    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L1BBeam(self, beam)
