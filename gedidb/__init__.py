# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from importlib.metadata import version as _version

# Import core functionality and classes from internal modules
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.core.gedigranule import GEDIGranule
from gedidb.core.gedimetadata import GEDIMetadataManager
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.providers.gedi_provider import GEDIProvider
from gedidb.downloader.authentication import EarthDataAuthenticator
from gedidb.utils.print_versions import show_versions

try:
    __version__ = _version("gedidb")
except Exception:
    __version__ = "9999"

# Define the public API, so users only see these when importing the package
__all__ = [
    "GEDIDatabase",
    "GEDIProcessor",
    "GEDIProvider",
    "GEDIGranule",
    "GEDIMetadataManager",
    "GEDIProcessor",
    "EarthDataAuthenticator",
    "show_versions",
    "__version__",
]
