import h5py

from typing import Iterable
from GEDItools.processor.beam.beam import Beam

class Granule(h5py.File):

    def __init__(self, file_path):

        super().__init__(file_path, "r")
        self.short_name = self['METADATA']['DatasetIdentification'].attrs['shortName']
        self.beam_names = [name for name in self.keys() if name.startswith("BEAM")]

    @property
    def n_beams(self) -> int:
        return len(self.beam_names)

    def _beam_from_name(self, beam: str) -> Beam:
        raise NotImplementedError

    def iter_beams(self) -> Iterable[Beam]:
        for beam in self.beam_names:
            yield self._beam_from_name(beam)

