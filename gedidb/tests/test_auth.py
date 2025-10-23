# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import stat
import builtins
import pytest
from pathlib import Path
from gedidb.downloader.authentication import EarthDataAuthenticator, URS_HOST


# -----------------------
# Helper: read file text
# -----------------------
def _text(p: Path) -> str:
    return p.read_text() if p.exists() else ""


# -----------------------
# Fixtures
# -----------------------
@pytest.fixture
def auth_tmpdir(tmp_path, monkeypatch):
    """
    Create an isolated 'home' for .netrc and .cookies,
    and force the authenticator to use it.
    """
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


# -----------------------
# Tests
# -----------------------


def test_strict_mode_raises_when_missing_credentials(auth_tmpdir):
    # No .netrc present → strict=True should fail fast
    with pytest.raises(FileNotFoundError):
        EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir), strict=True)


def test_write_credentials_idempotent(auth_tmpdir):
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    # First write
    A._write_urs_credentials_idempotent("alice", "secret")
    t1 = _text(A.netrc_file)
    assert f"machine {URS_HOST} login alice password secret" in t1

    # Write again with same creds → no duplicates
    A._write_urs_credentials_idempotent("alice", "secret")
    t2 = _text(A.netrc_file)
    assert t2.count(f"machine {URS_HOST}") == 1

    # Overwrite with new creds → single updated stanza
    A._write_urs_credentials_idempotent("bob", "topsecret")
    t3 = _text(A.netrc_file)
    assert "login bob password topsecret" in t3
    assert t3.count(f"machine {URS_HOST}") == 1

    # Permissions 0600
    st = A.netrc_file.stat().st_mode
    assert st & (stat.S_IRUSR | stat.S_IWUSR) == (stat.S_IRUSR | stat.S_IWUSR)
    assert (st & (stat.S_IRWXG | stat.S_IRWXO)) == 0


def test_read_credentials_from_netrc(auth_tmpdir):
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    A._write_urs_credentials_idempotent("carol", "pw123")
    login, password = A._read_urs_credentials()
    assert (login, password) == ("carol", "pw123")


def test_read_credentials_fallback_regex_on_parse_error(auth_tmpdir, monkeypatch):
    """
    Simulate a NetrcParseError and ensure the regex fallback still finds credentials.
    """
    # Write a "malformed" netrc but with a valid single-line stanza inside
    malformed = auth_tmpdir / ".netrc"
    malformed.write_text(
        f"""
# bad header !!
machine {URS_HOST} login dave password s3cr3t
garbage garbage
"""
    )

    # Monkeypatch the imported netrc constructor in the module to raise NetrcParseError
    import gedidb.downloader.authentication as auth_mod

    def raise_parse_error(*args, **kwargs):
        raise auth_mod.NetrcParseError("bad", 0)

    monkeypatch.setattr(auth_mod, "netrc", raise_parse_error)

    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    login, password = A._read_urs_credentials()
    assert (login, password) == ("dave", "s3cr3t")


def test_cookies_valid_flag(auth_tmpdir):
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    # No cookies yet
    assert A._cookies_valid() is False
    # Create non-empty cookie file
    A.cookie_file.write_text("cookie")
    assert A._cookies_valid() is True


def test_ensure_netrc_permissions_tightens(auth_tmpdir):
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    A._write_urs_credentials_idempotent("eve", "pw")
    # Loosen permissions artificially, then ensure() should tighten them
    os.chmod(A.netrc_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)
    A._ensure_netrc_permissions()
    st = A.netrc_file.stat().st_mode
    assert (st & (stat.S_IRWXG | stat.S_IRWXO)) == 0


def test_authenticate_prompts_then_writes_and_fetches_cookies(auth_tmpdir, monkeypatch):
    """
    No creds, not strict → prompt. Then ensure cookies are fetched via wget.
    """
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir), strict=False)

    # Simulate interactive input
    monkeypatch.setattr(builtins, "input", lambda msg: "frank")  # username
    import getpass as gp

    monkeypatch.setattr(gp, "getpass", lambda msg: "pass123")  # password

    # Mock subprocess.run to emulate successful wget
    calls = {}

    def fake_run(cmd, check, stdout, stderr):
        calls["ran"] = True
        # create a non-empty cookie file as wget would do
        A.cookie_file.write_text("cookiejar")
        return 0

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    A.authenticate()

    # Creds should be stored
    assert f"machine {URS_HOST} login frank password pass123" in _text(A.netrc_file)
    # Cookies should be present
    assert A.cookie_file.exists() and A.cookie_file.stat().st_size > 0
    assert calls.get("ran", False) is True


def test_authenticate_strict_with_existing_creds_fetches_cookies(
    auth_tmpdir, monkeypatch
):
    """
    With strict=True and creds already present, authenticate should skip prompting
    and go straight to cookie fetch when cookies are missing.
    """
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    A._write_urs_credentials_idempotent("grace", "pw")
    A = EarthDataAuthenticator(
        earth_data_dir=str(auth_tmpdir), strict=True
    )  # should not raise now

    ran = {"wget": False}

    def fake_run(cmd, check, stdout, stderr):
        ran["wget"] = True
        A.cookie_file.write_text("cookiejar")
        return 0

    import subprocess

    monkeypatch.setattr(subprocess, "run", fake_run)

    A.authenticate()
    assert ran["wget"] is True
    assert A._cookies_valid() is True


def test_fetch_cookies_failure_bubbles_up(auth_tmpdir, monkeypatch):
    """
    If wget fails (non-zero), _fetch_earthdata_cookies should raise.
    """
    A = EarthDataAuthenticator(earth_data_dir=str(auth_tmpdir))
    A._write_urs_credentials_idempotent("heidi", "pw")

    class CalledProcessError(Exception):
        pass

    # Use real subprocess.CalledProcessError to match code path
    import subprocess

    def fake_run(cmd, check, stdout, stderr):
        raise subprocess.CalledProcessError(returncode=1, cmd=cmd)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        A._fetch_earthdata_cookies("heidi", "pw")
