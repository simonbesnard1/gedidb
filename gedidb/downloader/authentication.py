# SPDX-License-Identifier: EUPL-1.2

# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import getpass
import subprocess
import logging
from pathlib import Path
from typing import Optional

# Configure logging
logger = logging.getLogger(__name__)


class EarthDataAuthenticator:
    """
    Handles Earthdata authentication by managing .netrc and cookie files for automated login.
    """

    def __init__(self, earth_data_dir: Optional['str'] = None, strict: bool = False):
        """
        Initialize the authenticator.
        :param earth_data_dir: Directory to store .netrc and cookies. Defaults to the user's home directory.
        :param strict: If True, fail if credentials are missing without prompting.
        """
        self.earth_data_path = Path(earth_data_dir) if earth_data_dir else Path.home()
        self.netrc_file = self.earth_data_path / ".netrc"
        self.cookie_file = self.earth_data_path / ".cookies"
        self.strict = strict

        if strict and not self._credentials_in_netrc():
            raise FileNotFoundError(
                f"No Earthdata credentials found in {self.netrc_file}. "
                "Please create credentials using the EarthDataAuthenticator module."
            )

    def authenticate(self):
        """Main method to manage authentication and cookies."""
        if not self._credentials_in_netrc():
            logger.info("EarthData authentication setup incomplete; starting setup.")
            if self.strict:
                raise FileNotFoundError(
                    f"EarthData authentication files missing or incomplete. Please check {self.netrc_file} and {self.cookie_file}."
                )
            else:
                logger.info("Prompting user for credentials.")
                self._prompt_for_credentials()
                self._fetch_earthdata_cookies()
        else:
            logger.info("EarthData authentication setup is complete.")

    def _credentials_in_netrc(self) -> bool:
        """Check if .netrc file contains Earthdata credentials and cookie file is valid."""
        try:
            # Check if .netrc file exists and contains Earthdata credentials
            if not self.netrc_file.exists():
                logger.warning(f"Missing .netrc file at {self.netrc_file}.")
                return False
            
            with self.netrc_file.open("r") as f:
                if "urs.earthdata.nasa.gov" not in f.read():
                    logger.warning(f"No Earthdata credentials found in {self.netrc_file}.")
                    return False
            
            # Check if cookie file exists and is non-empty
            if not self.cookie_file.exists():
                logger.warning(f"Missing cookie file at {self.cookie_file}.")
                return False
            
            if self.cookie_file.stat().st_size == 0:
                logger.warning(f"Cookie file at {self.cookie_file} is empty.")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Error checking EarthData authentication files: {e}")
            return False


    def _prompt_for_credentials(self):
        """Prompt user for credentials and store them in .netrc."""
        self.username = input("Please enter your Earthdata Login username: ")
        self.password = self._prompt_password()
        self._add_netrc_credentials()

    def _prompt_password(self) -> str:
        """Securely prompt for password."""
        try:
            return getpass.getpass("Please enter your Earthdata Login password: ")
        except Exception:
            logger.warning("Password input not secure; text will be visible.")
            return input("Please enter your Earthdata Login password (input visible): ")

    def _add_netrc_credentials(self):
        """Add credentials to .netrc and set secure permissions."""
        try:
            with self.netrc_file.open("a+") as f:
                f.write(
                    f"\nmachine urs.earthdata.nasa.gov login {self.username} password {self.password}"
                )
            self.netrc_file.chmod(0o600)
            logger.info("Credentials added to .netrc file.")
        except OSError as e:
            logger.error(f"Error writing to .netrc: {e}")
            raise

    def _fetch_earthdata_cookies(self):
        """Fetch Earthdata cookies via wget, saving to the cookie file."""
        try:
            self.cookie_file.touch(exist_ok=True)
            logger.info(
                "Attempting to fetch Earthdata cookies and save to %s", self.cookie_file
            )
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
                stdout=subprocess.DEVNULL,  # Suppress standard output
                stderr=subprocess.DEVNULL,  # Suppress error output, including warnings
            )
            logger.info(
                "Earthdata cookies successfully fetched and saved to %s.",
                self.cookie_file,
            )
        except subprocess.CalledProcessError as e:
            logger.error("Error fetching Earthdata cookies: %s", e)
            raise
