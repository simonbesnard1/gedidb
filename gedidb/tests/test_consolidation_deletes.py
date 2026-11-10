# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

"""Spatial consolidation of an array with deleted and rewritten cells.

Mirrors a correction run: rows are deleted with a condition and written back
with new values, then the array is consolidated with purge_deleted_cells on.
Merging already consolidated fragments with the purge on used to drop the
rewritten rows.
"""

import numpy as np
import pytest
import tiledb

from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.utils.tiledb_consolidation import SpatialConsolidationPlanner


def _make_array(uri):
    dom = tiledb.Domain(
        tiledb.Dim("latitude", domain=(-90.0, 90.0), tile=1.0, dtype=np.float64),
        tiledb.Dim("longitude", domain=(-180.0, 180.0), tile=1.0, dtype=np.float64),
    )
    attrs = [tiledb.Attr("record", dtype=np.int64), tiledb.Attr("value", dtype=np.float32)]
    tiledb.Array.create(
        uri, tiledb.ArraySchema(domain=dom, attrs=attrs, sparse=True, allows_duplicates=True)
    )


def _write(uri, ctx, lat, lon, record, value):
    with tiledb.open(uri, "w", ctx=ctx) as a:
        a[lat, lon] = {"record": record, "value": value.astype(np.float32)}


def _snapshot(uri, ctx):
    with tiledb.open(uri, ctx=ctx) as a:
        r = a.query(attrs=["record", "value"])[:]
    order = np.argsort(r["record"])
    return {k: r[k][order] for k in ("record", "value")}


def _database(uri, ctx, workers=1):
    db = GEDIDatabase.__new__(GEDIDatabase)
    db.array_uri = uri
    db.ctx = ctx
    db.config = {"tiledb": {"consolidation_settings": {"workers": workers}}}
    db.tiledb_config = tiledb.Config({"sm.consolidation.purge_deleted_cells": "true"})
    return db


@pytest.fixture
def corrected_array(tmp_path):
    """Eight overlapping write fragments, then a delete and a rewrite."""
    uri = str(tmp_path / "array")
    _make_array(uri)
    ctx = tiledb.Ctx()
    rng = np.random.default_rng(0)
    for i in range(8):
        lat = rng.uniform(-10, 10, 500)
        lon = rng.uniform(-10, 10, 500)
        _write(uri, ctx, lat, lon, np.arange(i * 500, (i + 1) * 500), np.zeros(500))
    with tiledb.open(uri, ctx=ctx) as a:
        south = a.query(cond="latitude < 0.0")[:]
    with tiledb.open(uri, "d", ctx=ctx) as a:
        a.query(cond="latitude < 0.0").submit()
    half = len(south["record"]) // 2
    for part in (slice(0, half), slice(half, None)):
        _write(uri, ctx, south["latitude"][part], south["longitude"][part],
               south["record"][part], np.ones(len(south["record"][part])))
    return uri, ctx, _snapshot(uri, ctx)


def _assert_same(expected, actual):
    assert len(actual["record"]) == len(expected["record"])
    for k in expected:
        np.testing.assert_array_equal(expected[k], actual[k])


@pytest.mark.parametrize("max_passes,workers", [(1, 1), (5, 1), (5, 3)])
def test_consolidation_keeps_rewritten_rows(corrected_array, max_passes, workers):
    uri, ctx, expected = corrected_array
    assert (expected["value"] == 1).sum() > 0
    db = _database(uri, ctx, workers)
    db.consolidate_fragments("spatial", max_fragments_per_group=2, max_passes=max_passes)
    _assert_same(expected, _snapshot(uri, ctx))
    assert len(tiledb.FragmentInfoList(uri, ctx=ctx)) < 10


def test_rerun_resumes_and_repeats_safely(corrected_array):
    uri, ctx, expected = corrected_array
    db = _database(uri, ctx)
    # interrupted run: only one group consolidated
    groups = SpatialConsolidationPlanner.compute(
        uri, ctx, max_fragments_per_group=2, include_consolidated=False
    )
    tiledb.consolidate(uri, ctx=ctx, config=db.tiledb_config,
                       fragment_uris=next(iter(groups))["fragment_uris"])
    for _ in range(3):  # resume, then repeated runs (commit files included)
        db.consolidate_fragments("spatial", max_fragments_per_group=2)
        _assert_same(expected, _snapshot(uri, ctx))
    remaining = SpatialConsolidationPlanner.compute(uri, ctx, include_consolidated=False)
    assert len(remaining) == 0


def test_extract_fragments_flags_consolidated():
    class Frag:
        def __init__(self, uri, ts):
            self.uri, self.timestamp_range = uri, ts
            self.nonempty_domain = ((0.0, 1.0), (0.0, 1.0))
            self.cell_num = 1

    out = SpatialConsolidationPlanner._extract_fragments(
        [Frag("/a/f1", (5, 5)), Frag("/a/f2", (1, 9)), Frag("/a/f3", None)]
    )
    assert [f["consolidated"] for f in out] == [False, True, False]
