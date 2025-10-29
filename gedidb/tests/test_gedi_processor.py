# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

from pathlib import Path
from datetime import datetime
import pandas as pd
import geopandas as gpd
import shapely.geometry as sgeom
import pytest
import yaml

from gedidb.core.gediprocessor import GEDIProcessor

# -------------------------
# Fakes / test doubles
# -------------------------


class FakeAuthenticator:
    def __init__(self, earth_data_dir=None, strict=False):
        self.earth_data_dir = earth_data_dir
        self.strict = strict
        self.called = False

    def authenticate(self):
        self.called = True


class FakeCMRDownloader:
    def __init__(self, geom, start, end, info):
        self.geom = geom
        self.start = start
        self.end = end
        self.info = info

    def download(self):
        # Returns dict[granule_id] -> list[(url, product, start_time)]
        return {
            "G1": [
                ("https://u/1", "level2A", None),
                ("https://u/2", "level2B", None),
                ("https://u/3", "level4A", None),
                ("https://u/4", "level4C", None),
            ],
            "G2": [
                ("https://u/a", "level2A", None),
                ("https://u/b", "level2B", None),
                ("https://u/c", "level4A", None),
                ("https://u/d", "level4C", None),
            ],
        }


class FakeH5Downloader:
    def __init__(self, download_path):
        self.download_path = download_path
        self.calls = []

    def download(self, granule_id, url, product_enum):
        self.calls.append((granule_id, url, product_enum.value))
        # return (granule_key, (product.value, "/path"))
        return granule_id, (
            product_enum.value,
            f"{self.download_path}/{granule_id}/{product_enum.name}.h5",
        )


class FakeDatabase:
    def __init__(self, config=None, credentials=None):
        self.config = config
        self.creds = credentials
        self.created = False
        self.consolidations = []
        self.checked = {}
        self.marked = []
        self.writes = []
        self.spatial_chunks = []

    def _create_arrays(self):
        self.created = True

    def check_granules_status(self, gran_ids):
        # Let’s say nothing is processed by default
        return {g: False for g in gran_ids}

    def spatial_chunking(self, df, chunk_size=None):
        self.spatial_chunks.append((len(df), chunk_size))
        # return dict of “quadrants”: here 1 chunk is enough
        return {"Q": df}

    def write_granule(self, df):
        self.writes.append(len(df))

    def mark_granule_as_processed(self, gid):
        self.marked.append(gid)

    def consolidate_fragments(self, consolidation_type="spatial", parallel_engine=None):
        self.consolidations.append(consolidation_type)


class FakeGEDIGranule:
    def __init__(self, download_path, data_info):
        self.download_path = download_path
        self.data_info = data_info
        self.calls = []

    def process_granule(self, download_results):
        # return a tiny DataFrame keyed by shot_number
        rows = []
        for gid, (prod, path) in download_results:
            rows.append({"shot_number": f"{gid}_shot", "prod": prod})
        return download_results[0][0], pd.DataFrame(rows)


class FakeExecutor:
    """A minimal executor that runs submitted callables synchronously."""

    def __init__(self, max_workers=1):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs):
        class _F:
            def __init__(self, val):
                self._val = val

            def result(self):
                return self._val

        return _F(fn(*args, **kwargs))


class FakeDaskClient:
    """Minimal Dask-like interface: submit→call, gather→list."""

    def __init__(self):
        self.submits = 0

    def submit(self, fn, *a, **k):
        self.submits += 1

        class _F:
            def __init__(self, val):
                self._v = val

        return _F(fn(*a, **k))

    def gather(self, futures):
        return [f._v for f in futures]

    def close(self):
        pass


# -------------------------
# Fixtures
# -------------------------


@pytest.fixture
def config_file(tmp_path):
    cfg = {
        "data_dir": str(tmp_path / "data"),
        "progress_dir": str(tmp_path / "progress"),
        "earth_data_info": {"foo": "bar"},
        "tiledb": {
            "chunk_size": 100,
            "temporal_batching": None,  # or "daily"/"weekly"
        },
    }
    p = tmp_path / "conf.yml"
    p.write_text(yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False))
    return str(p)


@pytest.fixture
def earth_dir(tmp_path):
    d = tmp_path / "earth"
    d.mkdir()
    return str(d)


@pytest.fixture
def simple_geom():
    poly = sgeom.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    return gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")


# -------------------------
# Helpers to monkeypatch
# -------------------------


def patch_deps(
    monkeypatch,
    *,
    auth=None,
    cmr=None,
    h5=None,
    db=None,
    gran=None,
    geo=None,
    tiling=None,
):
    # downloader/auth/db classes resolved inside module under test
    import gedidb.core.gediprocessor as mod

    if auth is not None:
        monkeypatch.setattr(mod, "EarthDataAuthenticator", auth)
    if cmr is not None:
        monkeypatch.setattr(mod, "CMRDataDownloader", cmr)
    if h5 is not None:
        monkeypatch.setattr(mod, "H5FileDownloader", h5)
    if db is not None:
        monkeypatch.setattr(mod, "GEDIDatabase", db)
    if gran is not None:
        monkeypatch.setattr(mod, "GEDIGranule", gran)
    if geo is not None:
        monkeypatch.setattr(mod, "check_and_format_shape", geo)
    if tiling is not None:
        monkeypatch.setattr(mod, "_temporal_tiling", tiling)


# -------------------------
# Constructor validations
# -------------------------


def test_ctor_validates_and_initializes(
    monkeypatch, config_file, earth_dir, simple_geom, tmp_path
):
    # Replace heavy deps
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        cmr=FakeCMRDownloader,
        h5=FakeH5Downloader,
        db=FakeDatabase,
        gran=FakeGEDIGranule,
        geo=lambda gdf, simplify=True: gdf,  # pass-through
    )

    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-02-01",
        config_file=config_file,
        earth_data_dir=earth_dir,
        credentials={"abc": 1},
        parallel_engine=None,
        log_dir=str(tmp_path / "logs"),
    )

    # download path created under data_dir/download
    assert (Path(proc.data_info["data_dir"]) / "download").exists()
    # DB arrays created
    assert proc.database_writer.created is True
    # geometry was normalized
    assert isinstance(proc.geom, gpd.GeoDataFrame)


def test_ctor_errors(monkeypatch, tmp_path, simple_geom, earth_dir):
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        db=FakeDatabase,
        geo=lambda gdf, simplify=True: gdf,
    )

    # missing config
    with pytest.raises(ValueError):
        GEDIProcessor(
            geometry=simple_geom,
            start_date="2020-01-01",
            end_date="2020-01-02",
            config_file=None,
            earth_data_dir=earth_dir,
        )

    # wrong suffix
    bad = tmp_path / "cfg.txt"
    bad.write_text("a: 1")
    with pytest.raises(ValueError):
        GEDIProcessor(
            geometry=simple_geom,
            start_date="2020-01-01",
            end_date="2020-01-02",
            config_file=str(bad),
            earth_data_dir=earth_dir,
        )

    # file not found
    with pytest.raises(FileNotFoundError):
        GEDIProcessor(
            geometry=simple_geom,
            start_date="2020-01-01",
            end_date="2020-01-02",
            config_file=str(tmp_path / "missing.yml"),
            earth_data_dir=earth_dir,
        )

    # invalid date order
    good = tmp_path / "ok.yml"
    good.write_text(
        "data_dir: {}\nearth_data_info: {{}}\ntiledb: {{}}\n".format(tmp_path)
    )
    with pytest.raises(ValueError):
        GEDIProcessor(
            geometry=simple_geom,
            start_date="2020-02-02",
            end_date="2020-01-01",
            config_file=str(good),
            earth_data_dir=earth_dir,
        )

    # geometry path string not found
    with pytest.raises(FileNotFoundError):
        GEDIProcessor(
            geometry=str(tmp_path / "no.geojson"),
            start_date="2020-01-01",
            end_date="2020-01-02",
            config_file=str(good),
            earth_data_dir=earth_dir,
        )


# -------------------------
# compute(): nothing to do → consolidate
# -------------------------


def test_compute_when_all_processed_consolidates(
    monkeypatch, config_file, earth_dir, simple_geom
):
    db = FakeDatabase()

    def DBFactory(config=None, credentials=None):
        return db

    class AllProcessedCMR(FakeCMRDownloader):
        def download(self):
            return {
                "G1": [
                    ("u", "level2A", None),
                    ("u", "level2B", None),
                    ("u", "level4A", None),
                    ("u", "level4C", None),
                ]
            }

    def check_all(gran_ids):
        return {g: True for g in gran_ids}

    db.check_granules_status = check_all

    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        cmr=AllProcessedCMR,
        h5=FakeH5Downloader,
        db=DBFactory,
        gran=FakeGEDIGranule,
        geo=lambda gdf, simplify=True: gdf,
    )

    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-10",
        config_file=config_file,
        earth_data_dir=earth_dir,
        parallel_engine=None,
    )
    proc.compute(consolidate=True, consolidation_type="spatial")

    # should have called consolidation exactly once, no writes
    assert db.consolidations == ["spatial"]
    assert db.writes == []


# -------------------------
# compute(): processing path with ThreadPool (default)
# -------------------------


def test_compute_processes_and_writes_with_executor(
    monkeypatch, config_file, earth_dir, simple_geom
):
    import concurrent.futures as cf

    # ---- Synchronous executor that passes isinstance(..., Executor) check
    class SyncExecutor(cf.Executor):
        def __init__(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args, **kwargs):
            fut = cf.Future()
            try:
                fut.set_result(fn(*args, **kwargs))
            except BaseException as e:
                fut.set_exception(e)
            return fut

        def shutdown(self, wait=True):
            pass

    # ---- Fakes
    db = FakeDatabase()

    def DBFactory(config=None, credentials=None):
        return db

    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        cmr=FakeCMRDownloader,
        h5=FakeH5Downloader,
        db=DBFactory,
        gran=FakeGEDIGranule,
        geo=lambda gdf, simplify=True: gdf,
    )

    # Pass our sync executor directly (no monkeypatching ThreadPoolExecutor)
    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-10",
        config_file=config_file,
        earth_data_dir=earth_dir,
        parallel_engine=SyncExecutor(),  # <= key change
    )

    proc.compute(consolidate=True, consolidation_type="default")

    # We had two granules, each turned into a small DF; both should be marked processed
    assert set(db.marked) == {"G1", "G2"}

    # One chunk written; total rows > 0
    assert sum(db.writes) > 0

    # Consolidation called with requested type
    assert db.consolidations == ["default"]


# -------------------------
# _process_granules(): temporal tiling validation
# -------------------------


def test_process_granules_invalid_tiling_raises(
    monkeypatch, config_file, earth_dir, simple_geom
):
    import yaml

    p = Path(config_file)
    cfg = {
        "data_dir": str(p.parent / "data"),
        "progress_dir": str(p.parent / "progress"),
        "earth_data_info": {},
        "tiledb": {"temporal_batching": "monthly"},  # invalid on purpose
    }
    p.write_text(yaml.safe_dump(cfg, sort_keys=False, default_flow_style=False))

    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        cmr=FakeCMRDownloader,
        h5=FakeH5Downloader,
        db=FakeDatabase,
        gran=FakeGEDIGranule,
        geo=lambda gdf, simplify=True: gdf,
    )

    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-10",
        config_file=str(p),
        earth_data_dir=earth_dir,
        parallel_engine=None,
    )

    with pytest.raises(ValueError):
        proc._process_granules({"G": []})


# -------------------------
# process_single_granule()
# -------------------------


def test_process_single_granule_calls_download_and_process(monkeypatch, tmp_path):
    # Patch inside the module under test
    def H5Factory(download_path):
        return FakeH5Downloader(download_path)

    patch_deps(
        monkeypatch,
        h5=H5Factory,
        gran=FakeGEDIGranule,
        db=FakeDatabase,
        auth=FakeAuthenticator,
        cmr=FakeCMRDownloader,
    )

    data_info = {"data_dir": str(tmp_path)}
    product_info = [
        ("https://u/1", "level2A", None),
        ("https://u/2", "level2B", None),
        ("https://u/3", "level4A", None),
        ("https://u/4", "level4C", None),
    ]
    gid, df, _ = GEDIProcessor.process_single_granule(
        "GZ", product_info, data_info, str(tmp_path / "dl")
    )
    assert gid == "GZ"
    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) >= {"shot_number", "prod"}
    assert len(df) == 4


# -------------------------
# geometry loading from path
# -------------------------


def test_validate_and_load_geometry_from_path(
    monkeypatch, config_file, earth_dir, tmp_path
):
    # Make a tiny GeoJSON on disk
    poly = sgeom.Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
    gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")
    gj = tmp_path / "roi.geojson"
    gdf.to_file(gj, driver="GeoJSON")

    # check_and_format_shape returns the same object
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        db=FakeDatabase,
        geo=lambda g, simplify=True: g,
    )

    proc = GEDIProcessor(
        geometry=str(gj),
        start_date="2020-01-01",
        end_date="2020-01-02",
        config_file=config_file,
        earth_data_dir=earth_dir,
    )
    assert isinstance(proc.geom, gpd.GeoDataFrame)
    assert len(proc.geom) == 1


# -------------------------
# date parsing helpers
# -------------------------


def test_validate_and_parse_date_good_bad(
    monkeypatch, config_file, earth_dir, simple_geom
):
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        db=FakeDatabase,
        geo=lambda gdf, simplify=True: gdf,
    )
    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-02",
        config_file=config_file,
        earth_data_dir=earth_dir,
    )
    assert proc._validate_and_parse_date("2021-12-31", "end_date") == datetime(
        2021, 12, 31
    )
    with pytest.raises(ValueError):
        proc._validate_and_parse_date("31-12-2021", "end_date")


# -------------------------
# close() behavior
# -------------------------


def test_close_executor(monkeypatch, config_file, earth_dir, simple_geom):
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        db=FakeDatabase,
        geo=lambda gdf, simplify=True: gdf,
    )
    # Use our fake executor explicitly
    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-02",
        config_file=config_file,
        earth_data_dir=earth_dir,
        parallel_engine=None,
    )
    proc.close()  # should not raise


def test_close_dask_client(monkeypatch, config_file, earth_dir, simple_geom):
    patch_deps(
        monkeypatch,
        auth=FakeAuthenticator,
        db=FakeDatabase,
        geo=lambda gdf, simplify=True: gdf,
    )
    proc = GEDIProcessor(
        geometry=simple_geom,
        start_date="2020-01-01",
        end_date="2020-01-02",
        config_file=config_file,
        earth_data_dir=earth_dir,
        parallel_engine=None,
    )
    proc.close()  # should not raise
