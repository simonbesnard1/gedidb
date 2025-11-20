# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import io
import time
import pytest
import h5py
import requests
from pathlib import Path

from gedidb.downloader.data_downloader import H5FileDownloader, _get_session
from gedidb.utils.constants import GediProduct


# -------------------------------
# Helpers
# -------------------------------
def make_valid_h5_bytes() -> bytes:
    bio = io.BytesIO()
    with h5py.File(bio, "w") as f:
        f.create_dataset("x", data=[1, 2, 3])
    return bio.getvalue()


def make_corrupt_bytes() -> bytes:
    return b"THIS_IS_NOT_HDF5"


class FakeResponse:
    def __init__(self, content=b"", status=200, headers=None):
        self._content = content
        self.status_code = status
        self.headers = headers or {}

    @property
    def content(self):
        return self._content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


# -------------------------------
# Fixtures
# -------------------------------
@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda *_: None)


@pytest.fixture
def tmp_out(tmp_path):
    return tmp_path


@pytest.fixture
def fake_session(monkeypatch):
    """
    Replace the per-thread session with a fresh FakeSession object.
    This ensures each test uses isolated request mocks.
    """

    class FakeSession:
        def __init__(self):
            self.headers = {"User-Agent": "gedidb/1.0"}

        def get(self, url, headers=None, timeout=30, stream=False):
            return self._handler(url, headers, timeout, stream)

    fake_sess = FakeSession()
    monkeypatch.setattr(
        "gedidb.downloader.data_downloader._get_session", lambda: fake_sess
    )
    return fake_sess


# -------------------------------
# Tests
# -------------------------------
def test_full_download_success(monkeypatch, tmp_out, fake_session):
    valid = make_valid_h5_bytes()
    total = len(valid)

    # First probe → Content-Range
    # Second GET → full file, no streaming
    def handler(url, headers, timeout, stream):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                content=b"\x89",
                status=200,
                headers={"Content-Range": f"bytes 0-1/{total}"},
            )
        return FakeResponse(content=valid, status=200)

    fake_session._handler = handler

    dl = H5FileDownloader(download_path=str(tmp_out))
    key, (prod_value, fpath) = dl.download(
        "G123", "http://example/file.h5", GediProduct.L2A
    )

    assert prod_value == "level2A"
    assert Path(fpath).exists()

    with h5py.File(fpath, "r") as f:
        assert list(f["x"]) == [1, 2, 3]


def test_resume_download(monkeypatch, tmp_out, fake_session):
    valid = make_valid_h5_bytes()
    total = len(valid)
    half = total // 2

    dl = H5FileDownloader(download_path=str(tmp_out))
    key = "G_RESUME"
    granule_dir = tmp_out / key
    granule_dir.mkdir(parents=True, exist_ok=True)

    # Fake partial existing file
    part_path = granule_dir / f"{GediProduct.L2B.name}.h5.part"
    part_path.write_bytes(valid[:half])

    def handler(url, headers, timeout, stream):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                content=b"\x89",
                status=200,
                headers={"Content-Range": f"bytes 0-1/{total}"},
            )

        assert headers.get("Range") == f"bytes={half}-"
        return FakeResponse(
            content=valid[half:],
            status=200,
            headers={"Content-Range": f"bytes {half}-{total-1}/{total}"},
        )

    fake_session._handler = handler

    _, (_, final_path) = dl.download(key, "http://example/file.h5", GediProduct.L2B)
    with h5py.File(final_path, "r") as f:
        assert list(f["x"]) == [1, 2, 3]


def test_no_range_support(monkeypatch, tmp_out, fake_session):
    valid = make_valid_h5_bytes()
    total = len(valid)

    def handler(url, headers, timeout, stream):
        # First probe returns NO Content-Range
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(content=b"\x00", status=200, headers={})
        return FakeResponse(content=valid, status=200)

    fake_session._handler = handler

    dl = H5FileDownloader(str(tmp_out))
    _, (_, fpath) = dl.download("G_NORANGE", "http://example/file.h5", GediProduct.L4A)

    with h5py.File(fpath, "r") as f:
        assert f["x"].shape[0] == 3


def test_corrupt_file(monkeypatch, tmp_out, fake_session):
    bad = make_corrupt_bytes()
    total = len(bad)

    def handler(url, headers, timeout, stream):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                content=b"\x89",
                status=200,
                headers={"Content-Range": f"bytes 0-1/{total}"},
            )
        return FakeResponse(content=bad, status=200)

    fake_session._handler = handler

    dl = H5FileDownloader(str(tmp_out))
    with pytest.raises(ValueError):
        dl.download("G_CORRUPT", "http://example/file.h5", GediProduct.L4A)
