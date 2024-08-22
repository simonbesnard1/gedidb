import os
import subprocess
from pathlib import Path
import yaml

class EarthDataAuthenticator:
    def __init__(self, config_file: str):
        # Load the configuration from the YAML file
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        self.username = config['earth_data_info']['credentials']['username']
        self.password = config['earth_data_info']['credentials']['password']
        self.user_path = Path(config['earth_data_info']['paths']['user_path'])
        self.cookie_file = Path(config['earth_data_info']['paths']['earth_data_cookie_file'])
        self.netrc_file = self.user_path / ".netrc"
    
    def authenticate(self):
        if self.cookie_file.exists():
            print("Authentication cookie file found. Skipping authentication.")
            return

        print("No authentication cookies found, fetching Earthdata cookies...")
        self._ensure_netrc_credentials()
        self._fetch_earthdata_cookies()

    def _ensure_netrc_credentials(self):
        """Ensures that the .netrc file has the required Earthdata login credentials."""
        if self.netrc_file.exists() and self._netrc_contains_credentials():
            print("Credentials already present in .netrc file.")
        else:
            self._add_netrc_credentials()
    
    def _netrc_contains_credentials(self) -> bool:
        """Checks if the .netrc file already contains Earthdata credentials."""
        with open(self.netrc_file, "r") as f:
            return "urs.earthdata.nasa.gov" in f.read()

    def _add_netrc_credentials(self):
        """Adds Earthdata credentials to the .netrc file and sets appropriate file permissions."""
        with open(self.netrc_file, "a+") as f:
            f.write(
                "\nmachine urs.earthdata.nasa.gov login {} password {}".format(
                    self.username, self.password
                )
            )
            # Set file permissions before the file is closed
            fileno = f.fileno()
        
        # Set file permissions outside of the 'with' block to avoid closing the file first
        os.fchmod(fileno, 0o600)
        print("Credentials added to .netrc file.")


    def _fetch_earthdata_cookies(self):
        """Uses wget to fetch Earthdata cookies and store them in the cookie file."""
        self.cookie_file.touch()
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
        print("Earthdata cookies successfully fetched and saved.")
