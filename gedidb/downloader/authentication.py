# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import getpass
import logging
import os
import re
import stat
import subprocess
from netrc import netrc, NetrcParseError
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

URS_HOST = "urs.earthdata.nasa.gov"


class EarthDataAuthenticator:
    """
    Handles Earthdata authentication by managing .netrc and cookie files for automated login.
    Idempotent: will reuse existing .netrc credentials and will not append duplicates.
    """

    def __init__(self, earth_data_dir: Optional[str] = None, strict: bool = False):
        """
        Initialize the authenticator.
        :param earth_data_dir: Directory to store .netrc and cookies. Defaults to the user's home directory.
        :param strict: If True, fail if credentials are missing without prompting.
        """
        self.earth_data_path = Path(earth_data_dir) if earth_data_dir else Path.home()
        self.netrc_file = self.earth_data_path / ".netrc"
        self.cookie_file = self.earth_data_path / ".cookies"
        self.strict = strict

        # If strict and no credentials present, fail early
        if strict and not self._has_urs_credentials():
            raise FileNotFoundError(
                f"No Earthdata credentials for '{URS_HOST}' found in {self.netrc_file}. "
                "Create them with EarthDataAuthenticator or place valid entries in your .netrc."
            )

    # --------------------------
    # Public API
    # --------------------------
    def authenticate(self):
        """
        Ensure valid Earthdata credentials exist in .netrc and that cookies are present.
        - If creds missing: prompt (unless strict=True, then raise).
        - If cookies missing/empty: fetch via wget.
        """
        login, password = self._read_urs_credentials()

        if not login or not password:
            if self.strict:
                raise FileNotFoundError(
                    f"EarthData authentication missing for '{URS_HOST}'. "
                    f"Please add credentials to {self.netrc_file}."
                )
            logger.info("No Earthdata credentials found; prompting user.")
            login, password = self._prompt_for_credentials()
            self._write_urs_credentials_idempotent(login, password)

        if not self._cookies_valid():
            logger.info(
                "Earthdata cookies not found or empty; fetching new cookies into %s",
                self.cookie_file,
            )
            self._fetch_earthdata_cookies(login, password)
        else:
            logger.debug("Earthdata cookies already present; skipping fetch.")

        logger.info("EarthData authentication setup is complete.")

    # --------------------------
    # Credential / cookie checks
    # --------------------------
    def _has_urs_credentials(self) -> bool:
        login, password = self._read_urs_credentials()
        return bool(login and password)

    def _cookies_valid(self) -> bool:
        try:
            return self.cookie_file.exists() and self.cookie_file.stat().st_size > 0
        except OSError:
            return False

    # --------------------------
    # .netrc helpers (idempotent)
    # --------------------------
    def _read_urs_credentials(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Read credentials for URS_HOST from .netrc using the stdlib parser.
        Returns (login, password) or (None, None) if not present/parsable.
        """
        try:
            if not self.netrc_file.exists():
                return None, None
            # Ensure permissions are acceptable for netrc parser on some systems
            self._ensure_netrc_permissions()
            nrc = netrc(str(self.netrc_file))
            if URS_HOST in nrc.hosts:
                login, _, password = nrc.hosts[URS_HOST]
                return login, password
            return None, None
        except (NetrcParseError, FileNotFoundError):
            # Fallback to naive text scan if parsing fails
            try:
                text = self.netrc_file.read_text()
            except Exception:
                return None, None
            m = re.search(
                rf"machine\s+{re.escape(URS_HOST)}\s+login\s+(\S+)\s+password\s+(\S+)",
                text,
            )
            if m:
                return m.group(1), m.group(2)
            return None, None

    def _write_urs_credentials_idempotent(self, login: str, password: str) -> None:
        """
        Rewrites/creates .netrc to contain exactly one stanza for URS_HOST, preserving any other entries.
        Sets permissions to 0600.
        """
        self.earth_data_path.mkdir(parents=True, exist_ok=True)
        existing = self.netrc_file.read_text() if self.netrc_file.exists() else ""

        # Remove any existing URS entries (single-line forms)
        cleaned = re.sub(
            rf"(?m)^\s*machine\s+{re.escape(URS_HOST)}\b.*?$", "", existing
        ).rstrip()

        urs_entry = f"\nmachine {URS_HOST} login {login} password {password}\n"
        # Keep a trailing newline and ensure separation if file had content
        new_text = (cleaned + urs_entry) if cleaned else urs_entry.lstrip("\n")

        self.netrc_file.write_text(new_text)
        self._chmod_600(self.netrc_file)
        logger.info("Stored Earthdata credentials in %s (idempotent).", self.netrc_file)

    def _ensure_netrc_permissions(self) -> None:
        """
        Ensure .netrc is owner-read/write only (0600). Some systems/netrc parser complain otherwise.
        """
        try:
            st = self.netrc_file.stat()
            # If group/other bits set, tighten to 0600
            if st.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
                self._chmod_600(self.netrc_file)
        except Exception:
            # Don't fail on permission check
            pass

    @staticmethod
    def _chmod_600(path: Path) -> None:
        try:
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        except Exception as e:
            logger.warning("Could not set 0600 permissions on %s: %s", path, e)

    # --------------------------
    # Prompting
    # --------------------------
    def _prompt_for_credentials(self) -> Tuple[str, str]:
        """Prompt user for username and password (password masked when possible)."""
        username = input("Please enter your Earthdata Login username: ").strip()
        try:
            password = getpass.getpass("Please enter your Earthdata Login password: ")
        except Exception:
            logger.warning("Password input not secure; text will be visible.")
            password = input("Please enter your Earthdata Login password (visible): ")
        return username, password

    # --------------------------
    # Cookies via wget
    # --------------------------
    def _fetch_earthdata_cookies(self, login: str, password: str) -> None:
        """
        Fetch Earthdata cookies via wget and save to cookie file, using .netrc or --user/--password fallback.
        Prefer using .netrc (already written) so no secrets leak to process list.
        """
        try:
            self.cookie_file.parent.mkdir(parents=True, exist_ok=True)
            self.cookie_file.touch(exist_ok=True)

            # wget will read .netrc; we avoid passing creds on CLI.
            # We still ensure .netrc has the right creds (already written above).
            logger.debug("Running wget to obtain Earthdata cookies.")
            subprocess.run(
                [
                    "wget",
                    "--no-check-certificate",
                    "--load-cookies",
                    str(self.cookie_file),
                    "--save-cookies",
                    str(self.cookie_file),
                    "--keep-session-cookies",
                    "https://urs.earthdata.nasa.gov",
                ],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            logger.info("Earthdata cookies saved to %s.", self.cookie_file)
        except subprocess.CalledProcessError as e:
            logger.error("Error fetching Earthdata cookies with wget: %s", e)
            raise
