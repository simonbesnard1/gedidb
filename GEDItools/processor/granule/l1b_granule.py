from GEDItools.processor.beam.beam import Beam
from GEDItools.processor.beam.l1b_beam import L1BBeam
from GEDItools.processor.granule.granule import Granule


class L1BGranule(Granule):

    def __init__(self, file_path):
        super().__init__(file_path)

    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L1BBeam(self, beam)
