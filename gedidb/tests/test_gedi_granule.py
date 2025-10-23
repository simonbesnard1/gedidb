# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import numpy as np
import pandas as pd

# ⬇️ adjust these if your paths differ
from gedidb.core.gedigranule import GEDIGranule
from gedidb.utils.constants import GediProduct

# --------------------------
# Helpers
# --------------------------


def _df(shots, **cols):
    df = pd.DataFrame({"shot_number": shots, **cols})
    return df


# Product code strings once (to reduce typos)
L2A, L2B, L4A, L4C = (
    GediProduct.L2A.value,
    GediProduct.L2B.value,
    GediProduct.L4A.value,
    GediProduct.L4C.value,
)

# --------------------------
# _join_dfs: happy path
# --------------------------


def test_join_dfs_happy_path_suffix_dedup_and_ordering():
    # Overlapping columns between L2A and L2B → L2B gets _L2B
    # L4A/L4C overlap with base → they get _L4A / _L4C
    l2a = _df(
        ["1", "2", "2"],  # duplicate '2' tests dedup keep='first'
        h_can=[10.0, 20.0, 999.0],  # overlap
        qa=[1, 0, 0],
    )
    l2b = _df(["1", "2"], h_can=[11.0, 21.0], snr=[5.0, 6.0])  # overlap: h_can
    l4a = _df(["1", "2"], lai=[3.0, 4.0], qa=[7, 8])  # overlap with base: qa
    l4c = _df(
        ["1", "2"], biomass=[100.0, 200.0], snr=[50, 60]
    )  # overlap with base: snr

    df_dict = {L2A: l2a, L2B: l2b, L4A: l4a, L4C: l4c}
    out = GEDIGranule._join_dfs(df_dict, "G123")

    assert isinstance(out, pd.DataFrame)
    # shot_number enforced to string dtype
    assert pd.api.types.is_string_dtype(out["shot_number"])
    # Inner join on '1','2' -> 2 rows
    assert list(out["shot_number"]) == ["1", "2"]

    # Column suffixing
    assert "h_can" in out.columns  # from L2A
    assert "h_can_L2B" in out.columns  # from L2B overlap
    assert "qa" in out.columns  # from L2A
    assert "qa_L4A" in out.columns  # overlapped from L4A
    assert "snr" in out.columns  # from L2B stayed (not overlapped with L2A)
    assert "snr_L4C" in out.columns  # overlapped from L4C
    assert "lai" in out.columns and "biomass" in out.columns

    # Value checks (row-wise one-to-one validated)
    r0 = out.iloc[0].to_dict()
    assert r0["h_can"] == 10.0 and r0["h_can_L2B"] == 11.0
    assert r0["qa"] == 1 and r0["qa_L4A"] == 7
    assert r0["snr"] == 5.0 and r0["snr_L4C"] == 50

    # Column ordering rule: [shot_number] + L2A columns + (others)
    l2a_cols = [c for c in l2a.columns if c != "shot_number"]
    expected_prefix = ["shot_number"] + l2a_cols
    assert out.columns[: len(expected_prefix)].tolist() == expected_prefix


# --------------------------
# _join_dfs: error / edge cases
# --------------------------


def test_join_dfs_missing_required_returns_none(caplog):
    df_dict = {L2A: _df(["1"], a=[1])}  # others missing
    out = GEDIGranule._join_dfs(df_dict, "G999")
    assert out is None
    assert any("Missing required products" in msg for msg in caplog.text.splitlines())


def test_join_dfs_missing_key_raises_internally_returns_none():
    bad = pd.DataFrame({"nope": ["1"]})
    df_dict = {
        L2A: bad,
        L2B: _df(["1"], x=[1]),
        L4A: _df(["1"], y=[2]),
        L4C: _df(["1"], z=[3]),
    }
    out = GEDIGranule._join_dfs(df_dict, "Gbad")
    assert out is None


def test_join_dfs_no_matching_shots_returns_none():
    df_dict = {
        L2A: _df(["1"], a=[1]),
        L2B: _df(["2"], b=[2]),  # no overlap
        L4A: _df(["1"], c=[3]),
        L4C: _df(["1"], d=[4]),
    }
    out = GEDIGranule._join_dfs(df_dict, "Gnooverlap")
    assert out is None


# --------------------------
# parse_granules
# --------------------------


def test_parse_granules_filters_missing_shot_number_and_cleans_dir(
    monkeypatch, tmp_path
):
    # Arrange a download dir with granule subdir
    download_path = tmp_path
    granule_key = "G123"
    gran_dir = download_path / granule_key
    gran_dir.mkdir(parents=True, exist_ok=True)

    # Mock parser to return dicts with and without 'shot_number'
    calls = {"seen": []}

    def fake_parse_h5(file, product, data_info=None):
        calls["seen"].append((product, file))
        if product == L2A:
            return {
                "shot_number": np.array([b"1", b"2"]),
                "rh98": np.array([10.0, 20.0], dtype=np.float32),
            }
        if product == L2B:
            return {"nope": np.array([1, 2])}  # will be filtered out
        if product == L4A:
            return {
                "shot_number": np.array([b"1", b"2"]),
                "lai": np.array([3.0, 4.0], dtype=np.float32),
            }
        if product == L4C:
            return {
                "shot_number": np.array([b"1", b"2"]),
                "biomass": np.array([100.0, 200.0], dtype=np.float32),
            }
        return None

    from gedidb.core.gedigranule import granule_parser

    monkeypatch.setattr(granule_parser, "parse_h5_file", fake_parse_h5)

    g = GEDIGranule(str(download_path), data_info={"foo": "bar"})
    granules = [
        (L2A, "/f/L2A.h5"),
        (L2B, "/f/L2B.h5"),
        (L4A, "/f/L4A.h5"),
        (L4C, "/f/L4C.h5"),
    ]

    out = g.parse_granules(granules, granule_key)

    # L2B dropped (no 'shot_number'); others present
    assert set(out.keys()) == {L2A, L4A, L4C}
    # Cleaned up the per-granule folder
    assert not gran_dir.exists()
    # Parser was invoked for each
    assert len(calls["seen"]) == 4


def test_parse_granules_on_parser_exception_returns_empty(monkeypatch, tmp_path):
    from gedidb.core.gedigranule import granule_parser

    def boom(*a, **k):
        raise RuntimeError("parse failure")

    monkeypatch.setattr(granule_parser, "parse_h5_file", boom)

    g = GEDIGranule(str(tmp_path), data_info={})
    out = g.parse_granules([(L2A, "/f")], "Gx")
    assert out == {}


# --------------------------
# process_granule
# --------------------------


def test_process_granule_missing_product_short_circuit(caplog, tmp_path, monkeypatch):
    # Row structure expected by process_granule:
    # row[0][0] -> granule_key; each item[1] is (product_code, filepath or None)
    row = [
        ("Gk", (L2A, "/f/L2A.h5")),
        ("Gk", (L2B, None)),  # missing
        ("Gk", (L4A, "/f/L4A.h5")),
        ("Gk", (L4C, "/f/L4C.h5")),
    ]

    g = GEDIGranule(str(tmp_path), data_info={})
    key, df = g.process_granule(row)
    assert key is None and df is None
    assert any("Missing HDF5 file(s)" in l for l in caplog.text.splitlines())


def test_process_granule_parsed_empty_returns_key_none(monkeypatch, tmp_path):
    # Make parse_granules return {} (e.g., all products filtered)
    g = GEDIGranule(str(tmp_path), data_info={})

    monkeypatch.setattr(GEDIGranule, "parse_granules", lambda self, granules, key: {})
    # Provide all products present
    row = [
        ("Gk", (L2A, "/a")),
        ("Gk", (L2B, "/b")),
        ("Gk", (L4A, "/c")),
        ("Gk", (L4C, "/d")),
    ]

    key, df = g.process_granule(row)
    assert key == "Gk" and df is None


def test_process_granule_happy_path_joins(monkeypatch, tmp_path):
    g = GEDIGranule(str(tmp_path), data_info={})

    # Fake parse_granules to return minimal dict of DataFrames
    def fake_parse(self, granules, key):
        return {
            L2A: _df(["1", "2"], a=[1, 2]),
            L2B: _df(["1", "2"], b=[3, 4]),
            L4A: _df(["1", "2"], c=[5, 6]),
            L4C: _df(["1", "2"], d=[7, 8]),
        }

    monkeypatch.setattr(GEDIGranule, "parse_granules", fake_parse)

    row = [
        ("Gkey", (L2A, "/a")),
        ("Gkey", (L2B, "/b")),
        ("Gkey", (L4A, "/c")),
        ("Gkey", (L4C, "/d")),
    ]

    key, df = g.process_granule(row)
    assert key == "Gkey"
    assert isinstance(df, pd.DataFrame)
    assert list(df["shot_number"]) == ["1", "2"]
    assert set(df.columns) >= {"a", "b", "c", "d", "shot_number"}


def test_process_granule_catches_exception_and_returns_none(monkeypatch, tmp_path):
    g = GEDIGranule(str(tmp_path), data_info={})

    def explode(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(GEDIGranule, "parse_granules", explode)

    row = [
        ("Gkey", (L2A, "/a")),
        ("Gkey", (L2B, "/b")),
        ("Gkey", (L4A, "/c")),
        ("Gkey", (L4C, "/d")),
    ]

    key, df = g.process_granule(row)
    assert key is None and df is None
