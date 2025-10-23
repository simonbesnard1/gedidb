# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import io
import types
from types import ModuleType

from gedidb.utils.print_versions import (
    get_sys_info,
    netcdf_and_hdf5_versions,
    show_versions,
)

# -------------------------
# get_sys_info
# -------------------------


def test_get_sys_info_includes_commit_and_platform(monkeypatch):
    # Pretend we're in a git repo with 'gedidb' dir
    monkeypatch.setattr("os.path.isdir", lambda p: p in (".git", "gedidb"))

    # Fake `git log` call
    class P:
        returncode = 0

        def communicate(self):
            return (b"deadbeefcafebabe\n", b"")

    monkeypatch.setattr("subprocess.Popen", lambda *a, **k: P())

    # Stable platform info
    monkeypatch.setattr(
        "platform.uname",
        lambda: ("Linux", "node", "6.0", "#1", "x86_64", "GenuineIntel"),
    )
    monkeypatch.setattr("struct.calcsize", lambda _: 8)  # 64-bit
    monkeypatch.setattr("locale.getlocale", lambda: ("en_US", "UTF-8"))

    out = dict(get_sys_info())  # convert list[tuple] to dict for assertions

    assert out["commit"] == "deadbeefcafebabe"
    assert out["python-bits"] == 64
    assert out["OS"] == "Linux"
    assert out["machine"] == "x86_64"
    assert out["LOCALE"] == "('en_US', 'UTF-8')"


# -------------------------
# netcdf_and_hdf5_versions
# -------------------------


def test_netcdf_prefers_netcdf4_when_present(monkeypatch):
    # Provide a dummy netCDF4 module
    mod = ModuleType("netCDF4")
    mod.__hdf5libversion__ = "hdf5-1.14.0"
    mod.__netcdf4libversion__ = "netcdf-4.9.2"
    monkeypatch.setitem(sys.modules, "netCDF4", mod)
    # Ensure h5py won't be used in this branch, but it wouldn't matter
    res = dict(netcdf_and_hdf5_versions())
    assert res["libhdf5"] == "hdf5-1.14.0"
    assert res["libnetcdf"] == "netcdf-4.9.2"


def test_netcdf_falls_back_to_h5py(monkeypatch):
    import builtins
    import sys

    # Force import netCDF4 to raise ImportError
    real_import = builtins.__import__

    def fake_import(name, *a, **k):
        if name == "netCDF4":
            raise ImportError("forced")
        return real_import(name, *a, **k)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    # Provide h5py with version.hdf5_version
    h5 = types.ModuleType("h5py")
    h5.version = types.SimpleNamespace(hdf5_version="hdf5-1.12.2")
    monkeypatch.setitem(sys.modules, "h5py", h5)

    res = dict(netcdf_and_hdf5_versions())
    assert res["libhdf5"] == "hdf5-1.12.2"
    assert res["libnetcdf"] is None


# -------------------------
# show_versions
# -------------------------

import sys


def test_show_versions_prints_all_fields(monkeypatch):
    import sys, types, importlib

    # Make system info stable
    monkeypatch.setattr("os.path.isdir", lambda p: False)
    monkeypatch.setattr(
        "platform.uname",
        lambda: ("Linux", "node", "6.0", "#1", "x86_64", "GenuineIntel"),
    )
    monkeypatch.setattr("struct.calcsize", lambda _: 8)
    monkeypatch.setattr("locale.getlocale", lambda: ("en_US", "UTF-8"))

    # Helper to make a module with __version__
    def mkmod(name, ver):
        m = types.ModuleType(name)
        m.__version__ = ver
        return m

    # Ensure known versions for some modules by preloading sys.modules
    monkeypatch.setitem(sys.modules, "gedidb", mkmod("gedidb", "0.0.0-dev"))
    monkeypatch.setitem(sys.modules, "pandas", mkmod("pandas", "2.2.2"))
    monkeypatch.setitem(sys.modules, "numpy", mkmod("numpy", "1.26.4"))

    # For missing/optional modules, force ImportError so the code prints "None"
    real_import = importlib.import_module

    def fake_import(name, package=None):
        if name in {"distributed", "matplotlib"}:
            raise ImportError("forced")
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    # If you want to fix versions for these too, inject them; otherwise we just assert presence
    # monkeypatch.setitem(sys.modules, "geopandas", mkmod("geopandas", "0.14.4"))
    # monkeypatch.setitem(sys.modules, "pyarrow", mkmod("pyarrow", "16.0.0"))

    buf = io.StringIO()
    show_versions(file=buf)
    text = buf.getvalue()

    # Header & core fields
    assert "INSTALLED VERSIONS" in text
    assert "python-bits:" in text
    assert "OS:" in text

    # Deterministic ones we set
    assert "gedidb: 0.0.0-dev" in text
    assert "pandas: 2.2.2" in text
    assert "numpy: 1.26.4" in text

    # Presence checks (don’t pin exact version; environment may differ)
    assert "geopandas:" in text
    assert "pyarrow:" in text

    # Optional modules we forced to ImportError should appear with None/installed
    assert "distributed:" in text
    assert "matplotlib:" in text
