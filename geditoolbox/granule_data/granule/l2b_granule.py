import pandas as pd
import geopandas as gpd
import numpy as np

from geditoolbox.granule_data.granule.granule import Granule
from geditoolbox.granule_data.beam.beam import Beam
from geditoolbox.granule_data.beam.l2b_beam import L2BBeam


class L2BGranule(Granule):

    def __init__(self, file_path):
        super().__init__(file_path)

    def _beam_from_name(self, beam: str) -> Beam:

        if beam not in self.beam_names:
            raise ValueError(f"Beam name must be one of {self.beam_names}")
        return L2BBeam(self, beam)
