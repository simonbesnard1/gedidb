# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import pytest

from gedidb.utils.tiledb_consolidation import (
    SpatialConsolidationPlanner,
    SpatialConsolidationPlan,
)

# ----------------------------
# Helpers: fake TileDB objects
# ----------------------------


class FakeFragment:
    def __init__(self, uri, lat_rng, lon_rng, cell_num=10_000_000):
        self.uri = uri
        self.nonempty_domain = (lat_rng, lon_rng)
        self.cell_num = cell_num


class FakeFragmentInfoList(list):
    pass


# ----------------------------
# Tests for _extract_fragments
# ----------------------------


def test_extract_fragments_basic(monkeypatch):
    frags = FakeFragmentInfoList(
        [
            FakeFragment("/abs/path/fragA", (10.0, 11.0), (30.0, 32.0), cell_num=100),
            FakeFragment("/abs/path/fragB", (-5.0, -1.0), (0.0, 5.0), cell_num=200),
        ]
    )

    fragments = SpatialConsolidationPlanner._extract_fragments(frags)

    assert fragments[0]["uri"] == os.path.basename("/abs/path/fragA")
    assert fragments[1]["uri"] == "fragB"
    assert fragments[0]["latitude_range"] == (10.0, 11.0)
    assert fragments[0]["longitude_range"] == (30.0, 32.0)
    assert fragments[0]["cell_num"] == 100
    assert fragments[1]["cell_num"] == 200


# ----------------------------
# Tests for _generate_plan
# ----------------------------


def _frag(uri, lat_min, lat_max, lon_min, lon_max, cell_num=10_000_000):
    return {
        "uri": uri,
        "latitude_range": (lat_min, lat_max),
        "longitude_range": (lon_min, lon_max),
        "cell_num": cell_num,
    }


def test_generate_plan_groups_overlapping_and_separates_non_overlapping():
    # A overlaps B (touching counts as overlap); C is disjoint
    fragments = [
        _frag("A", 0.0, 1.0, 0.0, 1.0),
        _frag("B", 1.0, 2.0, 0.5, 1.5),  # touches A on lat=1.0
        _frag("C", 10.0, 11.0, 10.0, 11.0),
    ]

    plan = SpatialConsolidationPlanner._generate_plan(fragments, min_group_cells=0)

    assert len(plan) == 2
    groups = [set(node["fragment_uris"]) for node in plan.values()]
    assert set(["A", "B"]) in groups
    assert set(["C"]) in groups

    for node in plan.values():
        assert node["num_fragments"] == len(node["fragment_uris"])


@pytest.mark.parametrize(
    "f1,f2,overlap",
    [
        # Overlap in both dims
        (
            _frag("X", 0.0, 1.0, 0.0, 1.0),
            _frag("Y", 0.5, 2.0, 0.5, 2.0),
            True,
        ),
        # Touching edges (should count as overlap due to <= and >=)
        (
            _frag("X", 0.0, 1.0, 0.0, 1.0),
            _frag("Y", 1.0, 2.0, 1.0, 2.0),
            True,
        ),
        # Disjoint in latitude
        (
            _frag("X", 0.0, 1.0, 0.0, 1.0),
            _frag("Y", 2.0, 3.0, 0.0, 1.0),
            False,
        ),
        # Disjoint in longitude
        (
            _frag("X", 0.0, 1.0, 0.0, 1.0),
            _frag("Y", 0.0, 1.0, 2.0, 3.0),
            False,
        ),
    ],
)
def test_generate_plan_overlap_logic_param(f1, f2, overlap):
    frags = [
        f1,
        f2,
        _frag("Z", 10.0, 11.0, 10.0, 11.0),
    ]
    plan = SpatialConsolidationPlanner._generate_plan(frags, min_group_cells=0)
    groups = [set(node["fragment_uris"]) for node in plan.values()]

    in_same_group = any({"X", "Y"}.issubset(g) for g in groups)
    assert in_same_group is overlap


def test_generate_plan_absorbs_small_group():
    # Two disjoint groups: one large, one tiny (below min_group_cells).
    # The tiny group should be absorbed into the large one.
    fragments = [
        _frag("big1", 0.0, 10.0, 0.0, 10.0, cell_num=1_000_000),
        _frag("big2", 5.0, 15.0, 5.0, 15.0, cell_num=1_000_000),
        _frag("tiny", 50.0, 51.0, 50.0, 51.0, cell_num=10),
    ]

    plan = SpatialConsolidationPlanner._generate_plan(fragments, min_group_cells=5_000)

    # tiny should have been absorbed into the big group → one node total
    assert len(plan) == 1
    assert plan[0]["num_fragments"] == 3
    all_uris = set(plan[0]["fragment_uris"])
    assert all_uris == {"big1", "big2", "tiny"}


def test_generate_plan_no_absorption_when_disabled():
    # With min_group_cells=0 absorption is disabled; tiny stays separate.
    fragments = [
        _frag("big1", 0.0, 10.0, 0.0, 10.0, cell_num=1_000_000),
        _frag("tiny", 50.0, 51.0, 50.0, 51.0, cell_num=10),
    ]

    plan = SpatialConsolidationPlanner._generate_plan(fragments, min_group_cells=0)

    assert len(plan) == 2


def test_generate_plan_cell_num_accumulated():
    fragments = [
        _frag("A", 0.0, 1.0, 0.0, 1.0, cell_num=300),
        _frag("B", 0.5, 1.5, 0.5, 1.5, cell_num=700),
    ]

    plan = SpatialConsolidationPlanner._generate_plan(fragments, min_group_cells=0)

    assert len(plan) == 1
    assert plan[0]["cell_num"] == 1000


# ----------------------------
# Tests for compute() with mocks
# ----------------------------


class DummyCtx:
    pass


def test_compute_builds_plan_from_tiledb(monkeypatch):
    fake_list = FakeFragmentInfoList(
        [
            FakeFragment("/dir/f1", (0.0, 1.0), (0.0, 1.0), cell_num=1_000_000),
            FakeFragment("/dir/f2", (1.0, 2.0), (0.5, 1.5), cell_num=1_000_000),
            FakeFragment("/dir/f3", (10.0, 11.0), (10.0, 11.0), cell_num=1_000_000),
        ]
    )

    import tiledb

    monkeypatch.setattr(tiledb, "FragmentInfoList", lambda uri, ctx=None: fake_list)

    plan = SpatialConsolidationPlanner.compute("tiledb://dummy/array", ctx=DummyCtx())
    assert isinstance(plan, SpatialConsolidationPlan)
    # Expect two nodes (f1+f2 together, f3 alone) — both have enough cells
    assert len(plan) == 2
    all_uris = set(u for _, node in plan.items() for u in node["fragment_uris"])
    assert all_uris == {"f1", "f2", "f3"}


def test_compute_no_fragments_returns_empty_plan(monkeypatch):
    import tiledb

    monkeypatch.setattr(
        tiledb, "FragmentInfoList", lambda uri, ctx=None: FakeFragmentInfoList([])
    )
    plan = SpatialConsolidationPlanner.compute("tiledb://empty", ctx=DummyCtx())
    assert isinstance(plan, SpatialConsolidationPlan)
    assert len(plan) == 0
    assert plan.dump() == {}


def test_compute_raises_when_fragmentinfo_fails(monkeypatch):
    import tiledb

    class Boom(Exception):
        pass

    def raise_boom(uri, ctx=None):
        raise Boom("fail to open")

    monkeypatch.setattr(tiledb, "FragmentInfoList", raise_boom)

    with pytest.raises(Exception) as e:
        SpatialConsolidationPlanner.compute("tiledb://bad", ctx=DummyCtx())
    assert "fail to open" in str(e.value)


# ----------------------------
# Tests for SpatialConsolidationPlan API
# ----------------------------


def test_plan_api_iter_len_getitem_dump():
    plan_dict = {
        0: {"num_fragments": 2, "fragment_uris": ["f1", "f2"], "cell_num": 200},
        1: {"num_fragments": 1, "fragment_uris": ["f3"], "cell_num": 100},
    }
    plan = SpatialConsolidationPlan(plan_dict)

    assert len(plan) == 2
    assert plan[0]["num_fragments"] == 2

    nodes = list(iter(plan))
    assert nodes[0]["fragment_uris"] == ["f1", "f2"]

    ids = [i for i, _ in plan.items()]
    assert set(ids) == {0, 1}

    assert plan.dump() is plan_dict  # same object
