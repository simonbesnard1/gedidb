"""L2A quality filtering across product versions."""

import h5py
import numpy as np
import pytest

from gedidb.beam.l2a_beam import L2ABeam


def _granule(path, rel3_path):
    with h5py.File(path, "w") as f:
        beam = f.create_group("BEAM0000")
        # V003 root quality_flag only marks invalid waveforms: all ones here.
        beam.create_dataset("quality_flag", data=np.array([1, 1, 1], dtype="uint8"))
        if rel3_path:
            beam.create_dataset(rel3_path, data=np.array([1, 0, 1], dtype="uint8"))
    return h5py.File(path, "r")


@pytest.mark.parametrize(
    "rel3_path, expected",
    [
        ("l2a_quality_flag_rel3", [1, 0, 1]),
        ("geolocation/l2a_quality_flag_rel3", [1, 0, 1]),
        (None, [1, 1, 1]),  # V002 file: root quality_flag is the L2A flag
    ],
)
def test_quality_flag_prefers_release3_flag(tmp_path, rel3_path, expected):
    with _granule(tmp_path / "g.h5", rel3_path) as granule:
        beam = L2ABeam(granule, "BEAM0000", {})
        mask = beam.DEFAULT_QUALITY_FILTERS["quality_flag"]()
        assert mask.astype(int).tolist() == expected
