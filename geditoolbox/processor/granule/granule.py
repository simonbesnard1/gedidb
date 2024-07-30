import h5py

from typing import Iterable
from geditoolbox.processor.beam.beam import Beam

QDEGRADE = [0, 3, 8, 10, 13, 18, 20, 23, 28, 30, 33, 38, 40, 43, 48, 60, 63, 68]


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

    """
    
        def debug_file(self):

        layer = 0
        with open(f"./debug/{self.short_name}_debug.txt", "w") as f:
            def print_group(group, layer):
                for x in group:
                    if isinstance(group[x], h5py.Group):
                        f.write(f"{'\t' * layer}{x}:\n")
                        print_group(group[x], layer + 1)
                    else:
                        f.write(f"{'\t' * layer}{x}\n")

            print_group(self, layer)
    
    """

