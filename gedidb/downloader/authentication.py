# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import subprocess
import yaml
import logging
from pathlib import Path
import platform
import stat

# Configure logging
logger = logging.getLogger(__name__)

class EarthDataAuthenticator:
    """
    EarthDataAuthenticator handles Earthdata authentication by managing .netrc and cookie files for
    automated authentication.
    """

    def __init__(self, config_file: str):
        """
        Initialize the EarthDataAuthenticator with the given configuration file.

        :param config_file: YAML file containing Earthdata credentials and directory paths.
        """
        self._load_config(config_file)
        self.earth_data = self._ensure_directory(Path(self.data_info['data_dir']) / 'earth_data')
        self.netrc_file = self.earth_data / ".netrc"
        self.cookie_file = self.earth_data / ".cookies"

    def _load_config(self, config_file: str):
        """Load configuration from the YAML file."""
        try:
            with open(config_file, 'r') as file:
                self.data_info = yaml.safe_load(file)
                self.username = self.data_info['earth_data_info']['credentials']['username']
                self.password = self.data_info['earth_data_info']['credentials']['password']
                logger.info("Earthdata configuration loaded successfully.")
        except (FileNotFoundError, KeyError) as e:
            logger.error(f"Error loading configuration: {e}")
            raise

    @staticmethod
    def _ensure_directory(path: Path) -> Path:
        """
        Ensure that a directory exists.

        :param path: The directory path to create if it does not exist.
        :return: The path as a Path object.
        """
        try:
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Directory ensured: {path}")
        except OSError as e:
            logger.error(f"Error ensuring directory {path}: {e}")
            raise
        return path

    def authenticate(self):
        """Handle authentication by checking for existing cookies or fetching new ones."""
        if self.cookie_file.exists():
            logger.info("Authentication cookie file found. Skipping authentication.")
            return

        logger.info("No authentication cookies found, fetching Earthdata cookies...")
        self._ensure_netrc_credentials()
        self._fetch_earthdata_cookies()

    def _ensure_netrc_credentials(self):
        """Ensure that the .netrc file has the required Earthdata login credentials."""
        if self.netrc_file.exists() and self._netrc_contains_credentials():
            logger.info("Credentials already present in .netrc file.")
        else:
            self._add_netrc_credentials()

    def _netrc_contains_credentials(self) -> bool:
        """Check if the .netrc file already contains Earthdata credentials."""
        try:
            with self.netrc_file.open("r") as f:
                return "urs.earthdata.nasa.gov" in f.read()
        except FileNotFoundError:
            logger.warning(f".netrc file not found at {self.netrc_file}.")
            return False

    def _add_netrc_credentials(self):
        """Add Earthdata credentials to the .netrc file and set appropriate file permissions."""
        try:
            with self.netrc_file.open("a+") as f:
                f.write(
                    f"\nmachine urs.earthdata.nasa.gov login {self.username} password {self.password}"
                )
                # Get file descriptor for Unix-based systems
                fileno = f.fileno()
    
            # Set file permissions based on the operating system
            if platform.system() != "Windows":
                # Set permissions for Unix-like systems
                os.fchmod(fileno, 0o600)
            else:
                # Set permissions for Windows: make file hidden and read/write only for the owner
                os.chmod(self.netrc_file, stat.S_IWRITE | stat.S_IREAD)
    
            logger.info("Credentials added to .netrc file.")
        except OSError as e:
            logger.error(f"Error adding credentials to .netrc file: {e}")
            raise

    def _fetch_earthdata_cookies(self):
        """Use wget to fetch Earthdata cookies and store them in the cookie file."""
        try:
            self.cookie_file.touch(exist_ok=True)
            subprocess.run(
                [
                    "wget",
                    "--no-check-certificate",  # Skip SSL certificate verification
                    "--load-cookies", str(self.cookie_file),
                    "--save-cookies", str(self.cookie_file),
                    "--keep-session-cookies",
                    "https://urs.earthdata.nasa.gov",
                ],
                check=True,
            )
            logger.info("Earthdata cookies successfully fetched and saved.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error fetching Earthdata cookies: {e}")
            raise
