# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely.geometry as sgeom
import pytest

from gedidb.providers.tiledb_provider import TileDBProvider

# ---------------------------
# Minimal fake TileDB objects
# ---------------------------


class FakeQuery:
    def __init__(self, data_dict):
        self._data = data_dict
        self._return_incomplete = False

    @property
    def multi_index(self):
        # Return a slicer object that handles [lat_min:lat_max, lon_min:lon_max, start:end]
        q = self

        class _Slicer:
            def __getitem__(self, slc):
                lat_slc, lon_slc, _time_slc = slc
                lat_min, lat_max = float(lat_slc.start), float(lat_slc.stop)
                lon_min, lon_max = float(lon_slc.start), float(lon_slc.stop)

                # basic bbox mask
                lats = q._data["latitude"]
                lons = q._data["longitude"]
                mask = (
                    (lats >= lat_min)
                    & (lats <= lat_max)
                    & (lons >= lon_min)
                    & (lons <= lon_max)
                )

                # filter all keys consistently
                out = {k: v[mask] for k, v in q._data.items()}
                return out

        return _Slicer()


class FakeArrayCtxMgr:
    """Context manager returned by tiledb.open(...)."""

    def __init__(self, meta, data_dict, schema):
        self.meta = meta
        self._data = data_dict
        self.schema = schema

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def query(self, attrs=None, cond=None, coords=True, return_incomplete=False):
        # emulate attribute selection: keep only attrs + coords
        # coords=True means 'latitude','longitude','shot_number' are returned
        keep = set(attrs or [])
        if coords:
            keep |= {"latitude", "longitude", "shot_number"}

        pruned = {k: v for k, v in self._data.items() if k in keep}
        # If selecting a profile attribute that doesn't exist, emulate empty result
        if any((a not in pruned) for a in keep if not a.startswith("cond:")):
            # return empty selection with required coordinate keys
            n0 = 0
            pruned = {
                k: np.array([], dtype=v.dtype)
                for k, v in self._data.items()
                if k in keep
            }
            if "shot_number" not in pruned:
                pruned["shot_number"] = np.array([], dtype=np.int64)
        return FakeQuery(pruned)


class FakeDim:
    def __init__(self, dom):
        self.domain = dom


class FakeDomain:
    def __init__(self, dims):
        # dims[0] -> latitude, dims[1] -> longitude per the provider's expectation
        self._dims = dims

    def dim(self, i):
        return self._dims[i]


class FakeSchema:
    def __init__(self, domain):
        self.domain = domain


# ---------------------------
# Utilities for monkeypatching
# ---------------------------


class TDOpenCounter:
    """Track how many times tiledb.open was called."""

    def __init__(self):
        self.count = 0

    def __call__(self, uri, mode="r", ctx=None):
        self.count += 1
        return self.factory(uri, mode, ctx)

    def set_factory(self, f):
        self.factory = f


def fake_tiledb_ctx(monkeypatch):
    # Replace tiledb.Ctx with a no-op class capturing config
    import tiledb

    class DummyCtx:
        def __init__(self, cfg=None):
            self.cfg = cfg or {}

    monkeypatch.setattr(tiledb, "Ctx", DummyCtx)
    return DummyCtx


# ---------------------------
# Constructor validation
# ---------------------------


def test_constructor_validation_local_and_s3(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    # local ok
    p = TileDBProvider(storage_type="local", local_path=str(tmp_path))
    assert p.scalar_array_uri.endswith("array_uri")
    assert isinstance(p.ctx, DummyCtx)

    # s3 ok (no-sign)
    p2 = TileDBProvider(
        storage_type="s3",
        s3_bucket="bucket",
        url="https://s3.xyz",
        region="eu-central-1",
    )
    assert p2.scalar_array_uri == "s3://bucket/array_uri"
    assert isinstance(p2.ctx, DummyCtx)
    assert p2.ctx.cfg["vfs.s3.no_sign_request"] == "true"

    # s3 missing bucket
    with pytest.raises(ValueError):
        TileDBProvider(storage_type="s3")

    # invalid storage type
    with pytest.raises(ValueError):
        TileDBProvider(storage_type="foo")

    # local missing path
    with pytest.raises(ValueError):
        TileDBProvider(storage_type="local", local_path=None)


# ---------------------------
# get_available_variables + caching
# ---------------------------


def test_get_available_variables_and_cache(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    # meta contains normal variables and some ignored keys
    meta = {
        "rh98.description": "Relative height 98",
        "rh98.units": "m",
        "height.units": "m",
        "granule_foo": "ignore",
        "some.array_type": "ignore",
    }
    # fake data (not used by get_available_variables)
    data = {
        "latitude": np.array([0.0]),
        "longitude": np.array([0.0]),
        "shot_number": np.array([1], dtype=np.int64),
    }
    schema = FakeSchema(FakeDomain([FakeDim((0.0, 1.0)), FakeDim((0.0, 1.0))]))

    open_counter = TDOpenCounter()
    open_counter.set_factory(
        lambda uri, mode, ctx=None: FakeArrayCtxMgr(meta, data, schema)
    )

    import tiledb

    monkeypatch.setattr(tiledb, "open", open_counter)

    prov = TileDBProvider(storage_type="local", local_path=str(tmp_path))
    df = prov.get_available_variables()
    # Only non-granule and keys without 'array_type' should be included
    assert set(df.index) == {"rh98", "height"}
    assert set(df.columns) >= {"description", "units"}

    # Cached: second call should not reopen TileDB
    _ = prov.get_available_variables()
    assert open_counter.count == 1


# ---------------------------
# _build_profile_attrs
# ---------------------------


def test_build_profile_attrs():
    prov = TileDBProvider(storage_type="local", local_path=".")
    vars_in = ["pai", "rh98", "pavd"]
    meta = {"pai.profile_length": 3, "pavd.profile_length": 2}
    attr_list, prof = prov._build_profile_attrs(vars_in, meta)
    # Expect pai_1..3, rh98, pavd_1..2
    assert attr_list == ["pai_1", "pai_2", "pai_3", "rh98", "pavd_1", "pavd_2"]
    assert prof == {"pai": ["pai_1", "pai_2", "pai_3"], "pavd": ["pavd_1", "pavd_2"]}


# ---------------------------
# _build_condition_string
# ---------------------------


def test_build_condition_string_simple_and_compound():
    prov = TileDBProvider(storage_type="local", local_path=".")
    cond = prov._build_condition_string(
        {"rh98": ">= 10 and < 30", "quality": "== 1", "note": "no_operator_here"}
    )
    # order within a key’s split is deterministic in our builder; across keys may vary
    # just assert required clauses are present
    expected_bits = {"rh98 >= 10", "rh98 < 30", "quality == 1"}
    for bit in expected_bits:
        assert bit in cond
    # malformed key should be ignored (with a warning) and not break the string
    assert "note" not in cond


# ---------------------------
# _query_array: bbox & empty
# ---------------------------


def test_query_array_bbox_and_empty(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    # Create two points; only one inside bbox we will query
    data = {
        "latitude": np.array([50.0, 51.0]),
        "longitude": np.array([10.0, 12.0]),
        "shot_number": np.array([101, 102], dtype=np.int64),
        "rh98": np.array([15.0, 25.0], dtype=np.float32),
    }
    meta = {}
    schema = FakeSchema(FakeDomain([FakeDim((40.0, 60.0)), FakeDim((5.0, 15.0))]))

    import tiledb

    monkeypatch.setattr(
        tiledb,
        "open",
        lambda uri, mode="r", ctx=None: FakeArrayCtxMgr(meta, data, schema),
    )

    prov = TileDBProvider(storage_type="local", local_path=str(tmp_path))

    # BBox that only catches the 2nd point
    out, prof = prov._query_array(
        variables=["rh98"],
        lat_min=50.5,
        lat_max=51.5,
        lon_min=11.5,
        lon_max=12.5,
        start_time=None,
        end_time=None,
        return_coords=True,
        use_polygon_filter=False,
    )
    assert out is not None
    assert out["shot_number"].size == 1
    assert out["rh98"][0] == 25.0
    assert prof == {}  # no profile vars

    # BBox with no hits → None
    out2, _ = prov._query_array(
        variables=["rh98"],
        lat_min=-1,
        lat_max=0,
        lon_min=-1,
        lon_max=0,
        start_time=None,
        end_time=None,
    )
    assert out2 is None


# ---------------------------
# Polygon filter
# ---------------------------


def test_query_array_with_polygon_filter(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    data = {
        "latitude": np.array([0.0, 0.5, 1.0]),
        "longitude": np.array([0.0, 0.5, 1.0]),
        "shot_number": np.array([1, 2, 3], dtype=np.int64),
        "rh98": np.array([10.0, 20.0, 30.0], dtype=np.float32),
    }
    meta = {}
    schema = FakeSchema(FakeDomain([FakeDim((-10.0, 10.0)), FakeDim((-10.0, 10.0))]))

    import tiledb

    monkeypatch.setattr(
        tiledb,
        "open",
        lambda uri, mode="r", ctx=None: FakeArrayCtxMgr(meta, data, schema),
    )

    prov = TileDBProvider(storage_type="local", local_path=str(tmp_path))

    poly = sgeom.Polygon([(0.25, 0.25), (0.75, 0.25), (0.75, 0.75), (0.25, 0.75)])
    gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")

    out, _ = prov._query_array(
        variables=["rh98"],
        lat_min=-1,
        lat_max=2,
        lon_min=-1,
        lon_max=2,
        start_time=None,
        end_time=None,
        geometry=gdf,
        use_polygon_filter=True,
    )
    # Only the middle point (0.5,0.5) should remain
    assert out["shot_number"].tolist() == [2]
    assert out["rh98"].tolist() == [20.0]


# ---------------------------
# query_dataframe: profile reconstruction
# ---------------------------


def test_query_dataframe_profile_reconstruction(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    meta = {"pavd.profile_length": 3}
    data = {
        "latitude": np.array([1.0, 2.0]),
        "longitude": np.array([3.0, 4.0]),
        "shot_number": np.array([10, 11], dtype=np.int64),
        "pavd_1": np.array([0.1, 0.2], dtype=np.float32),
        "pavd_2": np.array([0.3, 0.4], dtype=np.float32),
        "pavd_3": np.array([0.5, 0.6], dtype=np.float32),
        "rh98": np.array([25.0, 27.0], dtype=np.float32),
    }
    schema = FakeSchema(FakeDomain([FakeDim((0.0, 5.0)), FakeDim((0.0, 5.0))]))

    import tiledb

    monkeypatch.setattr(
        tiledb,
        "open",
        lambda uri, mode="r", ctx=None: FakeArrayCtxMgr(meta, data, schema),
    )

    prov = TileDBProvider(storage_type="local", local_path=str(tmp_path))
    df = prov.query_dataframe(
        variables=["pavd", "rh98"],
        lat_min=0.0,
        lat_max=5.0,
        lon_min=0.0,
        lon_max=5.0,
    )

    assert "pavd" in df.columns
    assert all(col not in df.columns for col in ["pavd_1", "pavd_2", "pavd_3"])

    # Use tolerant comparison (either works)
    assert df.loc[0, "pavd"] == pytest.approx([0.1, 0.3, 0.5], rel=1e-6, abs=1e-7)
    assert df.loc[1, "pavd"] == pytest.approx([0.2, 0.4, 0.6], rel=1e-6, abs=1e-7)


# ---------------------------
# Spatial domain & caching
# ---------------------------


def test_get_tiledb_spatial_domain_and_cache(monkeypatch, tmp_path):
    DummyCtx = fake_tiledb_ctx(monkeypatch)

    # Build dims so that provider reads:
    # min_lon,max_lon from dim(1); min_lat,max_lat from dim(0)
    schema = FakeSchema(
        FakeDomain(
            [
                FakeDim((40.0, 60.0)),  # dim(0) -> latitude
                FakeDim((5.0, 15.0)),  # dim(1) -> longitude
            ]
        )
    )
    meta = {}
    data = {
        "latitude": np.array([50.0]),
        "longitude": np.array([10.0]),
        "shot_number": np.array([1], dtype=np.int64),
    }

    open_counter = TDOpenCounter()
    open_counter.set_factory(
        lambda uri, mode="r", ctx=None: FakeArrayCtxMgr(meta, data, schema)
    )

    import tiledb

    monkeypatch.setattr(tiledb, "open", open_counter)

    prov = TileDBProvider(storage_type="local", local_path=str(tmp_path))
    bbox = prov._get_tiledb_spatial_domain()
    assert bbox == (5.0, 15.0, 40.0, 60.0)

    # Cached → no additional opens
    bbox2 = prov._get_tiledb_spatial_domain()
    assert bbox2 == bbox
    assert open_counter.count == 1
