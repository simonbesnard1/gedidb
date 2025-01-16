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
from gedidb.downloader import (
    authentication,
    cmr_query,
    data_downloader,
)
from gedidb.granule import granule_parser
from gedidb.providers import tiledb_provider
from gedidb.utils import (
    constants,
    geo_processing,
    print_versions,
    tiledb_consolidation,
)

__all__ = [
    "gedidatabase",
    "gedigranule",
    "gediprocessor",
    "gediprovider",
    "authentication",
    "cmr_query",
    "data_downloader",
    "granule_parser",
    "tiledb_provider",
    "constants",
    "geo_processing",
    "print_versions",
    "tiledb_consolidation",
    "show_versions",
    "__version__",
]
