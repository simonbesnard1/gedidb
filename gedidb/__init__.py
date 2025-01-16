# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from importlib.metadata import version as _version
from gedidb.utils.print_versions import show_versions

try:
    __version__ = _version("gedidb")
except Exception:
    __version__ = "9999"

from gedidb.core import (
    gedidatabase,
    gedigranule,
    gediprocessor,
    gediprovider,
)

from gedidb.providers import tiledb_provider

from gedidb.utils import (
    constants,
    geo_processing,
    print_versions,
    tiledb_consolidation,
)

from gedidb.utils.tiledb_consolidation import (
    SpatialConsolidationPlan,
    SpatialConsolidationPlanner
    
)
from gedidb.core.gediprocessor import GEDIProcessor
from gedidb.core.gedigranule import GEDIGranule
from gedidb.core.gedidatabase import GEDIDatabase
from gedidb.core.gediprovider import GEDIProvider


from gedidb.downloader import (
    authentication,
    cmr_query,
    data_downloader
)

from gedidb.downloader.authentication import EarthDataAuthenticator
from gedidb.downloader.cmr_query import CMRQuery
from gedidb.downloader.data_downloader import GEDIDownloader



from gedidb.granule import (
    granule_parser,
    Granule,
    granule_name,
    l2a_granule,
    l2b_granule,
    l4a_granule,
    l4c_granule
)
from gedidb.granule.Granule import granule_handler

from gedidb.beam import (
    Beam,
    l2a_beam,
    l2b_beam,
    l4a_beam,
    l4c_beam
)

from gedidb.beam.Beam import beam_handler


from gedidb.providers.tiledb_provider import TileDBProvider

__all__ = [
    "gedidatabase",
    "gedigranule",
    "gediprocessor",
    "gediprovider",
    "granule_parser",
    "tiledb_provider",
    "constants",
    "geo_processing",
    "print_versions",
    "tiledb_consolidation",
    "SpatialConsolidationPlan",
    "SpatialConsolidationPlanner",
    "GEDIProcessor",
    "GEDIGranule",
    "GEDIProvider",
    "GEDIDatabase",
    "GEDIDownloader",
    "authentication",
    "cmr_query",
    "CMRQuery",
    "data_downloader",
    "EarthDataAuthenticator",
    "granule_parser",
    "Beam",
    "beam_handler",
    "l2a_beam",
    "l2b_beam",
    "l4a_beam",
    "l4c_beam",
    "granule_name",
    "Granule",
    "granule_handler",
    "l2a_granule",
    "l2b_granule",
    "l4a_granule",
    "l4c_granule",
    "TileDBProvider",   
    "show_versions",
    "__version__",
]
