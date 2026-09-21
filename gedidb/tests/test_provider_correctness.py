"""Spatial and profile regressions using a small real TileDB database."""

import numpy as np
import pandas as pd
import geopandas as gpd
import pytest
from shapely.geometry import Polygon, box

from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.core.gediprovider import GEDIProvider
from gedidb.utils.geo_processing import _datetime_to_timestamp_days


@pytest.fixture
def provider(tmp_path):
    cfg = {
        "required_products": ["level2A"],
        "tiledb": {
            "local_path": str(tmp_path),
            "config_overrides": {
                "sm.compute_concurrency_level": "2",
                "sm.io_concurrency_level": "2",
            },
            "dimensions": ["latitude", "longitude", "time"],
            "spatial_range": {
                "lat_min": -90.0,
                "lat_max": 90.0,
                "lon_min": -180.0,
                "lon_max": 180.0,
            },
            "time_range": {"start_time": "2018-01-01", "end_time": "2030-01-01"},
        },
        "level_2a": {
            "variables": {
                "shot_number": {"dtype": "uint64"},
                "quality_flag": {"dtype": "uint8"},
                "rh": {
                    "dtype": "float32",
                    "is_profile": True,
                    "profile_length": 3,
                    "profile_labels": "0,50,100",
                    "profile_label_name": "percentile",
                },
                "cover_z": {
                    "dtype": "float64",
                    "is_profile": True,
                    "profile_length": 2,
                    "profile_labels": "0,5",
                    "profile_label_name": "height_m",
                },
            }
        },
    }
    db = GEDIDatabase(cfg)
    db._create_arrays()
    frame = pd.DataFrame(
        {
            "shot_number": np.arange(1, 8, dtype="uint64"),
            "latitude": [0.5, 0.9, 0.0, 0.0, 60.0, 60.08, 0.0],
            "longitude": [0.5, 0.9, 179.95, -179.98, 0.1, 0.0, 0.0],
            "time": pd.to_datetime(["2020-01-01"] * 7),
            "quality_flag": [1, 0, 1, 1, 1, 1, 1],
            "rh_1": [0.0] * 7,
            "rh_2": [10.0] * 7,
            "rh_3": [20.0] * 7,
            "cover_z_1": [0.123456789012345] * 7,
            "cover_z_2": [0.5] * 7,
        }
    )
    db.write_granule(frame)
    with GEDIProvider(
        local_path=str(tmp_path),
        config_overrides={
            "sm.compute_concurrency_level": "2",
            "sm.io_concurrency_level": "2",
            "py.init_buffer_bytes": "16",
        },
    ) as prov:
        yield prov


def geometry(shape=None):
    return gpd.GeoDataFrame(
        geometry=[shape if shape is not None else box(-1, -1, 1, 1)], crs=4326
    )


def test_profiles_have_independent_physical_coordinates(provider):
    data = provider.get_data(["rh", "cover_z", "rh:50"], geometry=geometry())
    assert data.rh.dims == ("shot_number", "rh_percentile")
    assert data.cover_z.dims == ("shot_number", "cover_z_height_m")
    assert data.rh_percentile.values.tolist() == [0, 50, 100]
    assert data.cover_z_height_m.values.tolist() == [0, 5]
    assert data.cover_z.shape[1] == 2
    assert data.cover_z.dtype == np.float64
    assert (data.rh_p50.values == data.rh.sel(rh_percentile=50).values).all()
    assert "percentile: 50" in data.rh_p50.attrs["description"]


def test_full_profile_and_label_dataframe(provider):
    data = provider.get_data(
        ["rh", "rh:50"], geometry=geometry(), return_type="dataframe"
    )
    assert data.rh.iloc[0] == [0.0, 10.0, 20.0]
    assert (data.rh_p50 == 10).all()


def test_projected_geometry_and_exact_polygon(provider):
    # Small missing corner (<10% of bbox) used to disable automatic filtering.
    shape = Polygon([(0, 0), (1, 0), (1, 0.8), (0.8, 1), (0, 1)])
    region = geometry(shape)
    exact = provider.get_data(
        ["quality_flag"], geometry=region.to_crs(3857), return_type="dataframe"
    )
    assert 1 in exact.shot_number.values
    assert 2 not in exact.shot_number.values
    # Points on the outer boundary belong to the polygon.
    assert 7 in exact.shot_number.values
    bbox = provider.get_data(
        ["quality_flag"],
        geometry=region,
        use_polygon_filter=False,
        return_type="dataframe",
    )
    assert 2 in bbox.shot_number.values


def test_polygon_holes_and_complex_geometry_preserved(provider):
    outer = box(-1, -1, 1, 1)
    hole = box(0.4, 0.4, 0.6, 0.6)
    shape = Polygon(outer.exterior.coords, [hole.exterior.coords])
    # More vertices than the CMR limit must not cause provider convex-hull replacement.
    shape = shape.segmentize(0.001)
    data = provider.get_data(
        ["quality_flag"], geometry=geometry(shape), return_type="dataframe"
    )
    assert 1 not in data.shot_number.values
    assert 2 in data.shot_number.values


def test_geometry_without_crs_fails(provider):
    region = gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)])
    with pytest.raises(ValueError, match="CRS"):
        provider.get_data(["quality_flag"], geometry=region)


def test_nearest_uses_geographic_distance_and_wraps_dateline(provider):
    # At 60 degrees, 0.1 degrees longitude is closer than 0.08 degrees latitude.
    data = provider.get_data(
        ["quality_flag"],
        query_type="nearest",
        point=(0.0, 60.0),
        radius=0.2,
        num_shots=1,
        return_type="dataframe",
    )
    assert data.shot_number.tolist() == [5]
    data = provider.get_data(
        ["quality_flag"],
        query_type="nearest",
        point=(179.99, 0.0),
        radius=0.1,
        num_shots=1,
        return_type="dataframe",
    )
    assert data.shot_number.tolist() == [4]


def test_invalid_filter_fails_and_or_expression_is_applied(provider):
    with pytest.raises(ValueError, match="Invalid quality filter"):
        provider.get_data(["quality_flag"], geometry=geometry(), quality_flag="1")
    data = provider.get_data(
        ["quality_flag"],
        geometry=geometry(),
        quality_flag="= 1 or == 2",
        return_type="dataframe",
    )
    assert 2 not in data.shot_number.values


def test_streaming_matches_eager_dataframe(provider):
    kwargs = dict(
        variables=["rh", "rh:50", "cover_z"],
        lat_min=-1.0,
        lat_max=1.0,
        lon_min=-1.0,
        lon_max=1.0,
        start_time=18262,
        end_time=18262,
    )
    eager = (
        provider.query_dataframe(**kwargs)
        .sort_values("shot_number")
        .reset_index(drop=True)
    )
    chunks = list(provider.iter_query_dataframe(**kwargs))
    result = (
        pd.concat(chunks, ignore_index=True)
        .sort_values("shot_number")
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(result, eager)


def test_timezone_offsets_are_converted_to_utc():
    assert _datetime_to_timestamp_days("2020-01-01T00:30:00+02:00") == 18261
