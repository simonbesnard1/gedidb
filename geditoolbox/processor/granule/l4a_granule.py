from geditoolbox.granule_data.granule.granule import Granule
from geditoolbox.granule_data.beam.l4a_beam import L4ABeam
from geditoolbox.granule_data.beam.beam import Beam


class L4AGranule(Granule):

    def __init__(self, file_path):
        super().__init__(file_path)

    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L4ABeam(self, beam)
