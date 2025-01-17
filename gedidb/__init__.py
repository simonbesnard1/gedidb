# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
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
from gedidb.downloader.data_downloader import H5FileDownloader
from gedidb.downloader.data_downloader import CMRDataDownloader

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
from gedidb.granule.granule_name import GediNameMetadata
from gedidb.granule.granule_parser import GranuleParser
from gedidb.granule.l2a_granule import L2AGranule
from gedidb.granule.l2b_granule import L2BGranule
from gedidb.granule.l4a_granule import L4AGranule
from gedidb.granule.l4c_granule import L4CGranule



from gedidb.beam import (
    Beam,
    l2a_beam,
    l2b_beam,
    l4a_beam,
    l4c_beam
)

from gedidb.beam.Beam import beam_handler
from gedidb.beam.l2a_beam import L2ABeam
from gedidb.beam.l2b_beam import L2BBeam
from gedidb.beam.l4a_beam import L4ABeam
from gedidb.beam.l4c_beam import L4CBeam


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
    "CMRDataDownloader",
    "H5FileDownloader",
    "authentication",
    "cmr_query",
    "CMRQuery",
    "data_downloader",
    "EarthDataAuthenticator",
    "granule_parser",
    "GranuleParser",
    "L2AGranule",
    "L2BGranule",
    "L4AGranule",
    "L4CGranule",
    "Beam",
    "beam_handler",
    "l2a_beam",
    "l2b_beam",
    "l4a_beam",
    "l4c_beam",
    "L2ABeam",
    "L2BBeam",
    "L4ABeam",
    "L4CBeam",
    "granule_name",
    "Granule",
    "granule_handler",
    "GediNameMetadata",
    "l2a_granule",
    "l2b_granule",
    "l4a_granule",
    "l4c_granule",
    "TileDBProvider",   
    "show_versions",
    "__version__",
]
