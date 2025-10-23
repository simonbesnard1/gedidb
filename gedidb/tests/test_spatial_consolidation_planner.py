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
    def __init__(self, uri, lat_rng, lon_rng):
        # mimic minimal attributes used in _extract_fragments
        self.uri = uri
        # Planner expects fragment.nonempty_domain[0]=latitude_range,
        # and [1]=longitude_range (per your implementation).
        self.nonempty_domain = (lat_rng, lon_rng)


class FakeFragmentInfoList(list):
    # behave like a list of fragments
    pass


# ----------------------------
# Tests for _extract_fragments
# ----------------------------


def test_extract_fragments_basic(monkeypatch):
    frags = FakeFragmentInfoList(
        [
            FakeFragment("/abs/path/fragA", (10.0, 11.0), (30.0, 32.0)),
            FakeFragment("/abs/path/fragB", (-5.0, -1.0), (0.0, 5.0)),
        ]
    )

    fragments = SpatialConsolidationPlanner._extract_fragments(frags)

    # URIs should be basenames
    assert fragments[0]["uri"] == os.path.basename("/abs/path/fragA")
    assert fragments[1]["uri"] == "fragB"
    # Ranges should be carried over
    assert fragments[0]["latitude_range"] == (10.0, 11.0)
    assert fragments[0]["longitude_range"] == (30.0, 32.0)


# ----------------------------
# Tests for _generate_plan
# ----------------------------


def test_generate_plan_groups_overlapping_and_separates_non_overlapping():
    # A overlaps B (touching counts as overlap); C is disjoint
    fragments = [
        {"uri": "A", "latitude_range": (0.0, 1.0), "longitude_range": (0.0, 1.0)},
        {
            "uri": "B",
            "latitude_range": (1.0, 2.0),
            "longitude_range": (0.5, 1.5),
        },  # touches A on lat=1.0
        {"uri": "C", "latitude_range": (10.0, 11.0), "longitude_range": (10.0, 11.0)},
    ]

    plan = SpatialConsolidationPlanner._generate_plan(fragments)

    # Expect two nodes total; one contains A,B and the other contains C
    assert len(plan) == 2
    groups = [set(node["fragment_uris"]) for node in plan.values()]
    assert set(["A", "B"]) in groups
    assert set(["C"]) in groups

    # num_fragments should match list sizes
    for node in plan.values():
        assert node["num_fragments"] == len(node["fragment_uris"])


@pytest.mark.parametrize(
    "f1,f2,overlap",
    [
        # Overlap in both dims
        (
            {"uri": "X", "latitude_range": (0.0, 1.0), "longitude_range": (0.0, 1.0)},
            {"uri": "Y", "latitude_range": (0.5, 2.0), "longitude_range": (0.5, 2.0)},
            True,
        ),
        # Touching edges (should count as overlap due to <= and >=)
        (
            {"uri": "X", "latitude_range": (0.0, 1.0), "longitude_range": (0.0, 1.0)},
            {"uri": "Y", "latitude_range": (1.0, 2.0), "longitude_range": (1.0, 2.0)},
            True,
        ),
        # Disjoint in latitude
        (
            {"uri": "X", "latitude_range": (0.0, 1.0), "longitude_range": (0.0, 1.0)},
            {"uri": "Y", "latitude_range": (2.0, 3.0), "longitude_range": (0.0, 1.0)},
            False,
        ),
        # Disjoint in longitude
        (
            {"uri": "X", "latitude_range": (0.0, 1.0), "longitude_range": (0.0, 1.0)},
            {"uri": "Y", "latitude_range": (0.0, 1.0), "longitude_range": (2.0, 3.0)},
            False,
        ),
    ],
)
def test_generate_plan_overlap_logic_param(f1, f2, overlap):
    # Build a tiny set with the two frags plus a far-away one
    frags = [
        f1,
        f2,
        {"uri": "Z", "latitude_range": (10.0, 11.0), "longitude_range": (10.0, 11.0)},
    ]
    plan = SpatialConsolidationPlanner._generate_plan(frags)
    groups = [set(node["fragment_uris"]) for node in plan.values()]

    in_same_group = any({"X", "Y"}.issubset(g) for g in groups)
    assert in_same_group is overlap


# ----------------------------
# Tests for compute() with mocks
# ----------------------------


class DummyCtx:
    # minimal stand-in; your code just passes it to FragmentInfoList
    pass


def test_compute_builds_plan_from_tiledb(monkeypatch):
    # Fake FragmentInfoList returned by tiledb
    fake_list = FakeFragmentInfoList(
        [
            FakeFragment("/dir/f1", (0.0, 1.0), (0.0, 1.0)),
            FakeFragment("/dir/f2", (1.0, 2.0), (0.5, 1.5)),
            FakeFragment("/dir/f3", (10.0, 11.0), (10.0, 11.0)),
        ]
    )

    # monkeypatch tiledb.FragmentInfoList to return our fake list
    import tiledb

    monkeypatch.setattr(tiledb, "FragmentInfoList", lambda uri, ctx=None: fake_list)

    plan = SpatialConsolidationPlanner.compute("tiledb://dummy/array", ctx=DummyCtx())
    assert isinstance(plan, SpatialConsolidationPlan)
    # Expect two nodes (f1+f2 together, f3 alone)
    assert len(plan) == 2
    all_uris = set(u for _, node in plan.items() for u in node["fragment_uris"])
    assert all_uris == {"f1", "f2", "f3"}  # basenames applied


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
        0: {"num_fragments": 2, "fragment_uris": ["f1", "f2"]},
        1: {"num_fragments": 1, "fragment_uris": ["f3"]},
    }
    plan = SpatialConsolidationPlan(plan_dict)

    assert len(plan) == 2
    assert plan[0]["num_fragments"] == 2

    # iter yields node dicts
    nodes = list(iter(plan))
    assert nodes[0]["fragment_uris"] == ["f1", "f2"]

    # items() yields (id, node) pairs
    ids = [i for i, _ in plan.items()]
    assert set(ids) == {0, 1}

    # dump returns the backing dict
    assert plan.dump() is plan_dict  # same object
