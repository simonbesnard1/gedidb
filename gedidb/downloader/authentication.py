# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import getpass
import subprocess
import logging
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)


class EarthDataAuthenticator:
    """
    Handles Earthdata authentication by managing .netrc and cookie files for automated login.
    """

    def __init__(self):
        """
        Prompt the user for credentials and directory to store files.
        """
        self.username = input("Please enter your Earthdata Login username: ")
        self.password = self._prompt_password()
        self.earth_data_dir = self._get_storage_directory()
        self.netrc_file = self.earth_data_dir / ".netrc"
        self.cookie_file = self.earth_data_dir / ".cookies"

        logger.info(
            "Authenticator initialized with storage directory set to %s",
            self.earth_data_dir,
        )

    def authenticate(self):
        """Main method to manage authentication and cookies."""
        if self.cookie_file.exists():
            logger.info(
                "Authentication cookie file found at %s. Skipping authentication.",
                self.cookie_file,
            )
        else:
            logger.info("No authentication cookies found; starting authentication.")
            self._ensure_netrc_credentials()
            self._fetch_earthdata_cookies()

    def _prompt_password(self) -> str:
        """Securely prompt for password or fall back if unsupported."""
        try:
            return getpass.getpass("Please enter your Earthdata Login password: ")
        except Exception:
            logger.warning("Password input not secure; text will be visible.")
            return input("Please enter your Earthdata Login password (input visible): ")

    def _get_storage_directory(self) -> Path:
        """Prompt user for storage directory or use default (home directory)."""
        default_dir = Path.home()
        user_dir = input(
            f"Enter directory to store authentication files (default: {default_dir}): "
        )
        storage_dir = Path(user_dir) if user_dir else default_dir
        return self._ensure_directory(storage_dir)

    @staticmethod
    def _ensure_directory(path: Path) -> Path:
        """Create directory if it doesn't exist and log the action."""
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info("Directory ensured: %s", path)
        except OSError as e:
            logger.error("Error creating directory %s: %s", path, e)
            raise
        return path

    def _ensure_netrc_credentials(self):
        """Ensure .netrc file contains required Earthdata login credentials."""
        if self.netrc_file.exists() and self._credentials_in_netrc():
            logger.info("Credentials already present in .netrc file.")
        else:
            self._add_netrc_credentials()

    def _credentials_in_netrc(self) -> bool:
        """Check if .netrc file already contains Earthdata credentials."""
        try:
            with self.netrc_file.open("r") as f:
                return "urs.earthdata.nasa.gov" in f.read()
        except FileNotFoundError:
            logger.warning(
                ".netrc file not found at %s; will create a new one.", self.netrc_file
            )
            return False

    def _add_netrc_credentials(self):
        """Add Earthdata credentials to .netrc file and set secure permissions."""
        try:
            with self.netrc_file.open("a+") as f:
                f.write(
                    f"\nmachine urs.earthdata.nasa.gov login {self.username} password {self.password}"
                )

            # Set secure permissions for the file
            self.netrc_file.chmod(0o600)
            logger.info(
                "Credentials added to .netrc file with secure permissions at %s.",
                self.netrc_file,
            )
        except OSError as e:
            logger.error(
                "Error adding credentials to .netrc file at %s: %s", self.netrc_file, e
            )
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
