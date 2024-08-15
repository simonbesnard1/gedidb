from gedidb.processor.granule.granule import Granule
from gedidb.processor.beam.l4c_beam import L4CBeam
from gedidb.processor.beam.beam import Beam


class L4CGranule(Granule):

    def __init__(self, file_path, quality_flag, field_mapping, geom):
        super().__init__(file_path)
        
        self.quality_flag = quality_flag
        self.field_mapping = field_mapping
        self.geom = geom
        
        
    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L4CBeam(self, beam, self.quality_flag, self.field_mapping, self.geom)

