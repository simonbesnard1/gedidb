from importlib.metadata import version as _version

# Import core functionality and classes from internal modules
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.core.gedigranule import GEDIGranule
from gedidb.core.gedimetadata import GEDIMetadataManager
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.utils.print_versions import show_versions

try:
    __version__ = _version("gedidb")
except Exception:
    __version__ = "9999"

# Define the public API, so users only see these when importing the package
__all__ = [
    "GEDIDatabase",
    "GEDIProcessor",
    "GEDIGranule",
    "GEDIMetadataManager",
    "GEDIProcessor",
    "show_versions",
    "__version__",
]
