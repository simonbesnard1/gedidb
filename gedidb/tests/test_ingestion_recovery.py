"""Recovery and schema regressions exercised against temporary TileDB arrays."""

import concurrent.futures
import copy
import csv
import time
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import tiledb

from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.core.gedigranule import GEDIGranule
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.utils.constants import GediProduct


@pytest.fixture
def database(tmp_path):
    config = {
        "required_products": ["level2A"],
        "earth_data_info": {"CMR_PRODUCT_IDS": {"GediProduct.L2A": "test-collection"}},
        "tiledb": {
            "local_path": str(tmp_path / "db"),
            "config_overrides": {
                "sm.compute_concurrency_level": "2",
                "sm.io_concurrency_level": "2",
            },
            "spatial_range": {
                "lat_min": -90.0,
                "lat_max": 90.0,
                "lon_min": -180.0,
                "lon_max": 180.0,
            },
            "time_range": {"start_time": "2018-01-01", "end_time": "2030-01-01"},
            "dimensions": ["latitude", "longitude", "time"],
        },
        "level_2a": {
            "variables": {
                "shot_number": {"dtype": "uint64"},
                "agbd": {"dtype": "float32"},
            }
        },
    }
    db = GEDIDatabase(config)
    db._create_arrays()
    return db


def shots():
    return pd.DataFrame(
        {
            "shot_number": np.array(
                [10000000000000001, 10000000000000002], dtype="uint64"
            ),
            "latitude": [0.0, 0.0],
            "longitude": [0.0, 0.0],
            "time": pd.to_datetime(["2020-01-01", "2020-01-01"]).as_unit("us"),
            "agbd": [1.0, 2.0],
        }
    )


def read(db):
    with tiledb.open(db.array_uri, "r", ctx=db.ctx) as array:
        return array[:]


def test_distinct_colocated_shots_survive_replay(database):
    frame = shots()
    database.write_granule(frame)
    database.write_granule(frame)
    data = read(database)
    assert set(data["shot_number"]) == set(frame.shot_number)
    assert len(data["shot_number"]) == 2
    assert (data["timestamp_ns"] == pd.Timestamp("2020-01-01").value).all()
    assert (data["time"] == 18262).all()


def test_ambiguous_commit_retry_does_not_duplicate(database, monkeypatch):
    original = tiledb.open
    attempts = []

    class CommitThenFail:
        def __enter__(self):
            self.array = original(database.array_uri, "w", ctx=database.ctx)
            return self.array

        def __exit__(self, *args):
            self.array.close()
            raise ConnectionError("acknowledgment lost after commit")

    def open_array(uri, mode="r", **kwargs):
        if mode == "w" and not attempts:
            attempts.append(True)
            return CommitThenFail()
        return original(uri, mode, **kwargs)

    # Warm schema cache before injecting the write failure.
    database._get_schema_cache()
    monkeypatch.setattr(tiledb, "open", open_array)
    monkeypatch.setattr(time, "sleep", lambda *_: None)
    database.write_granule(shots())
    assert len(read(database)["shot_number"]) == 2


def processor(db, tmp_path, engine):
    proc = GEDIProcessor.__new__(GEDIProcessor)
    proc.data_info = {"tiledb": {"max_in_flight": 2}}
    proc.progress_dir = str(tmp_path / "progress")
    proc.download_path = str(tmp_path / "download")
    proc.flush_every, proc.report_every = 50, 1
    proc.database_writer = db
    proc.parallel_engine = engine
    return proc


def test_partial_spatial_flush_is_recoverable(database, tmp_path, monkeypatch):
    frame = shots()
    frame.loc[1, "latitude"] = 10.0
    monkeypatch.setattr(
        GEDIProcessor,
        "process_single_granule",
        staticmethod(lambda gid, *args: (gid, frame, {"n_records": 2})),
    )
    original = database.write_granule
    count = 0

    def fail_second(tile):
        nonlocal count
        count += 1
        if count == 2:
            raise RuntimeError("second tile failed")
        original(tile)

    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc = processor(database, tmp_path, executor)
        monkeypatch.setattr(database, "write_granule", fail_second)
        with pytest.raises(RuntimeError, match="second tile failed"):
            proc._process_granules({"G1": []})
        assert database.check_granules_status(["G1"]) == {"G1": False}
        assert len(read(database)["shot_number"]) == 1
        rows = list(csv.DictReader((tmp_path / "progress/all/ledger.csv").open()))
        assert [row["status"] for row in rows] == ["fail"]
        monkeypatch.setattr(database, "write_granule", original)
        proc._process_granules({"G1": []})
    assert len(read(database)["shot_number"]) == 2
    assert database.check_granules_status(["G1"]) == {"G1": True}


def test_empty_success_and_parse_failure_have_distinct_status(
    database, tmp_path, monkeypatch
):
    def parse(gid, *args):
        if gid == "bad":
            raise ValueError("missing SDS")
        return gid, pd.DataFrame(), {"n_records": 0}

    monkeypatch.setattr(GEDIProcessor, "process_single_granule", staticmethod(parse))
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        proc = processor(database, tmp_path, executor)
        with pytest.raises(RuntimeError, match="1 granule"):
            proc._process_granules({"empty": [], "bad": []})
    assert database.check_granules_status(["empty", "bad"]) == {
        "empty": True,
        "bad": False,
    }


def test_status_read_failure_is_not_treated_as_unprocessed(database):
    with patch(
        "gedidb.core.gedidatabase.tiledb.open",
        side_effect=tiledb.TileDBError("read failed"),
    ):
        with pytest.raises(tiledb.TileDBError, match="read failed"):
            database.check_granules_status(["G1"])


def test_resume_rejects_policy_and_schema_changes(database):
    GEDIDatabase(database.config)._create_arrays()
    for change in ("collection", "dtype", "domain", "filter"):
        config = copy.deepcopy(database.config)
        if change == "collection":
            config["earth_data_info"]["CMR_PRODUCT_IDS"][
                "GediProduct.L2A"
            ] = "different-version"
        elif change == "dtype":
            config["level_2a"]["variables"]["agbd"]["dtype"] = "float64"
        elif change == "domain":
            config["tiledb"]["spatial_range"]["lat_min"] = -80.0
        else:
            config["quality_filters"] = {"level2A": []}
        with pytest.raises(ValueError):
            GEDIDatabase(config)._create_arrays()


def test_source_change_rejected_before_checkpoint(database):
    database.register_granule_sources(
        {"G1": [("https://host/v3.h5?token=secret", "level2A", None)]}
    )
    database.register_granule_sources(
        {"G1": [("https://host/v3.h5?token=new", "level2A", None)]}
    )
    with tiledb.open(database.array_uri, ctx=database.ctx) as array:
        assert "secret" not in array.meta["granule_G1_sources"]
    with pytest.raises(ValueError, match="Source products changed"):
        database.register_granule_sources(
            {"G1": [("https://host/v4.h5", "level2A", None)]}
        )


def test_spatial_chunking_uses_positions(database):
    frame = shots().set_axis([10, 20])
    result = pd.concat([part for _, part in database.spatial_chunking(frame)])
    pd.testing.assert_frame_equal(result, frame)


def test_parse_failure_preserves_downloads(tmp_path, monkeypatch):
    path = tmp_path / "G1"
    path.mkdir()
    (path / "L2A.h5").write_text("placeholder")
    granule = GEDIGranule(str(tmp_path), {"required_products": ["level2A"]})
    monkeypatch.setattr(
        "gedidb.core.gedigranule.granule_parser.parse_h5_file",
        lambda *args, **kwargs: None,
    )
    with pytest.raises(ValueError, match="Failed to parse"):
        granule.process_granule([("G1", ("level2A", str(path / "L2A.h5")))])
    assert path.exists()


def test_join_rejects_missing_or_duplicate_product_rows():
    with pytest.raises(ValueError, match="Missing parsed product"):
        GEDIGranule._join_dfs({}, "G1")
    frame = shots()
    frame.loc[1, "shot_number"] = frame.loc[0, "shot_number"]
    with pytest.raises(ValueError, match="Duplicate shot_number"):
        GEDIGranule._join_dfs({"level2A": frame}, "G1", [GediProduct.L2A])


def test_dask_path_releases_futures_and_checkpoints_empty_results(
    database, tmp_path, monkeypatch
):
    import gedidb.core.gediprocessor as module

    class DaskFuture(concurrent.futures.Future):
        def release(self):
            self.released = True

    class Client:
        def __init__(self):
            self.futures = []
            self.max_unreleased = 0

        def submit(self, fn, *args, pure):
            assert pure is False
            future = DaskFuture()
            future.released = False
            future.set_result(fn(*args))
            self.futures.append(future)
            self.max_unreleased = max(
                self.max_unreleased, sum(not f.released for f in self.futures)
            )
            return future

    monkeypatch.setattr(module, "Client", Client)
    monkeypatch.setattr(module, "dask_wait", concurrent.futures.wait)
    monkeypatch.setattr(
        GEDIProcessor,
        "process_single_granule",
        staticmethod(lambda gid, *args: (gid, pd.DataFrame(), {"n_records": 0})),
    )
    client = Client()
    proc = processor(database, tmp_path, client)
    ids = {f"G{i}": [] for i in range(5)}
    proc._process_granules(ids)
    assert client.max_unreleased <= 2
    assert all(f.released for f in client.futures)
    assert all(database.check_granules_status(list(ids)).values())
