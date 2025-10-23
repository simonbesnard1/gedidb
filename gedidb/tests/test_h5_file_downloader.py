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
from pathlib import Path

# Adjust this import to your actual module path
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.utils.constants import GediProduct


# -------------------------------
# Helpers: valid/corrupt HDF5 bytes
# -------------------------------
def make_valid_h5_bytes() -> bytes:
    # Create a tiny valid HDF5 in-memory
    bio = io.BytesIO()
    with h5py.File(bio, "w") as f:
        f.create_dataset("x", data=[1, 2, 3])
    return bio.getvalue()


def make_corrupt_bytes() -> bytes:
    return b"NOT_HDF5_AT_ALL"


# -------------------------------
# Fake requests.Response
# -------------------------------
class FakeResponse:
    def __init__(self, content=b"", status=200, headers=None):
        self._content = content
        self.status_code = status
        self.headers = headers or {}

    def raise_for_status(self):
        if 400 <= self.status_code:
            import requests

            raise requests.HTTPError(f"status {self.status_code}")

    def iter_content(self, chunk_size=8192):
        # Yield content in chunks
        c = self._content
        for i in range(0, len(c), chunk_size):
            yield c[i : i + chunk_size]

    def close(self):  # pragma: no cover
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


# -------------------------------
# Common fixtures
# -------------------------------
@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    # Speed up retry backoff: make time.sleep a no-op
    monkeypatch.setattr(time, "sleep", lambda *_: None)


@pytest.fixture
def tmp_out(tmp_path):
    return tmp_path


# -------------------------------
# Tests
# -------------------------------
def test_new_download_success(monkeypatch, tmp_out):
    """
    Server supports Range and we download a new file successfully.
    """
    valid = make_valid_h5_bytes()
    total = len(valid)

    # First "probe" request with Range=0-1 returns Content-Range with total.
    def fake_get(url, headers=None, stream=None, timeout=30):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                b"\x89", 200, headers={"Content-Range": f"bytes 0-1/{total}"}
            )
        # Second request returns full content
        return FakeResponse(valid, 200, headers={"Content-Length": str(total)})

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    dl = H5FileDownloader(download_path=str(tmp_out))
    key = "G123"
    result_key, (prod_value, path) = dl.download(
        key, "https://example/file.h5", GediProduct.L2A
    )
    assert result_key == key
    assert prod_value == "level2A"
    assert Path(path).exists()
    # Valid HDF5
    with h5py.File(path, "r") as f:
        assert "x" in f


def test_resume_download_success(monkeypatch, tmp_out):
    """
    A partial .part exists; server supports Range and sends the remainder.
    """
    valid = make_valid_h5_bytes()
    total = len(valid)
    half = total // 2

    dl = H5FileDownloader(download_path=str(tmp_out))
    key = "G999"
    granule_dir = Path(tmp_out) / key
    granule_dir.mkdir(parents=True, exist_ok=True)
    part_path = granule_dir / f"{GediProduct.L2B.name}.h5.part"
    # Seed half of the bytes as if previously downloaded
    part_path.write_bytes(valid[:half])

    def fake_get(url, headers=None, stream=None, timeout=30):
        # Probe shows total; then Range request starts from half
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                b"\x89", 200, headers={"Content-Range": f"bytes 0-1/{total}"}
            )
        # Resume from half:
        rng = headers.get("Range")
        assert rng == f"bytes={half}-"
        return FakeResponse(
            valid[half:],
            206,
            headers={"Content-Range": f"bytes {half}-{total-1}/{total}"},
        )

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    result_key, (prod_value, path) = dl.download(
        key, "https://example/file.h5", GediProduct.L2B
    )
    assert result_key == key
    assert prod_value == "level2B"
    with h5py.File(path, "r") as f:
        assert list(f["x"][:]) == [1, 2, 3]


def test_server_no_range_support(monkeypatch, tmp_out):
    """
    If the server doesn't send Content-Range on probe, code should download full file (no resume).
    """
    valid = make_valid_h5_bytes()
    total = len(valid)

    def fake_get(url, headers=None, stream=None, timeout=30):
        # First call: no Content-Range → no resume support
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(b"\x89", 200, headers={})
        # Full file
        return FakeResponse(valid, 200, headers={"Content-Length": str(total)})

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    dl = H5FileDownloader(download_path=str(tmp_out))
    k, (_, path) = dl.download("G777", "https://example/file.h5", GediProduct.L4A)
    with h5py.File(path, "r") as f:
        assert f["x"].shape[0] == 3


def test_416_range_not_satisfiable_resets_and_raises(monkeypatch, tmp_out):
    """
    If server returns 416 for resume, downloader deletes .part and raises ValueError (then retry decor will re-call).
    We assert that a ValueError is raised from this invocation.
    """

    def fake_get(url, headers=None, stream=None, timeout=30):
        # Second call returns 416
        status = 200 if (headers and headers.get("Range") == "bytes=0-1") else 416
        headers = {"Content-Range": "bytes 0-1/100"} if status == 200 else {}
        return FakeResponse(b"", status=status, headers=headers)

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    dl = H5FileDownloader(download_path=str(tmp_out))
    # Create a dangling .part so code tries to resume
    key = "G416"
    part = Path(tmp_out) / key / f"{GediProduct.L4C.name}.h5.part"
    part.parent.mkdir(parents=True, exist_ok=True)
    part.write_bytes(b"partial")

    # Because of @retry(tries=10), we will see multiple attempts. We just assert it eventually raises.
    with pytest.raises(ValueError):
        dl.download(key, "https://example/file.h5", GediProduct.L4C)
    # .part should be gone after 416 handling on first round
    assert not part.exists()


def test_final_size_mismatch_raises(monkeypatch, tmp_out):
    """
    If final .part size != expected total_size → raises ValueError (and retry triggers).
    """
    valid = make_valid_h5_bytes()
    total = len(valid)

    # Fake server lies: says total=N but sends N-1 bytes.
    def fake_get(url, headers=None, stream=None, timeout=30):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                b"\x89", 200, headers={"Content-Range": f"bytes 0-1/{total}"}
            )
        return FakeResponse(valid[:-1], 200)

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    dl = H5FileDownloader(download_path=str(tmp_out))
    with pytest.raises(ValueError):
        dl.download("Gbad", "https://example/file.h5", GediProduct.L2A)


def test_corrupt_download_rejected(monkeypatch, tmp_out):
    """
    If the downloaded file isn't a valid HDF5, it is deleted and a ValueError is raised (triggering retry).
    """
    bad = make_corrupt_bytes()
    total = len(bad)

    def fake_get(url, headers=None, stream=None, timeout=30):
        if headers and headers.get("Range") == "bytes=0-1":
            return FakeResponse(
                b"\x89", 200, headers={"Content-Range": f"bytes 0-1/{total}"}
            )
        return FakeResponse(bad, 200)

    import requests

    monkeypatch.setattr(requests, "get", fake_get)

    dl = H5FileDownloader(download_path=str(tmp_out))
    with pytest.raises(ValueError):
        dl.download("Gbad2", "https://example/file.h5", GediProduct.L4A)
