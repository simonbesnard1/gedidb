# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from enum import Enum


class GediProduct(Enum):
    """
    Enum representing different GEDI data products.

    Attributes:
        L2A (str): Level 2A GEDI product (land and canopy structure).
        L2B (str): Level 2B GEDI product (biomass and carbon).
        L4A (str): Level 4A GEDI product (terrain elevation and canopy height).
        L4C (str): Level 4C GEDI product (ecosystem carbon and dynamics).
    """

    L2A = "level2A"
    L2B = "level2B"
    L4A = "level4A"
    L4C = "level4C"

    @classmethod
    def list_products(cls):
        """
        Get a list of all available GEDI product names.

        :return: List of product names as strings.
        """
        return [product.value for product in cls]


# Constant for the WGS84 coordinate reference system (CRS)
WGS84 = "EPSG:4326"
"""
WGS84 Coordinate Reference System (CRS), commonly used for global latitude and longitude representation.
EPSG:4326 is the code representing the WGS84 standard.
"""


def required_products(config: dict) -> list[GediProduct]:
    """Return a deterministic product set; L2A supplies shot coordinates."""
    filters = config.get("quality_filters", {})
    if not isinstance(filters, dict) or set(filters) - set(GediProduct.list_products()):
        raise ValueError(
            "quality_filters must map GEDI product values to lists of filter names."
        )
    if any(
        not isinstance(names, list) or any(not isinstance(name, str) for name in names)
        for names in filters.values()
    ):
        raise ValueError("quality_filters selections must be lists of filter names.")
    requested = config.get("required_products", GediProduct.list_products())
    if not isinstance(requested, (list, tuple)):
        raise ValueError("required_products must be a list of GEDI product values.")
    products = {GediProduct(value) for value in requested}
    if GediProduct.L2A not in products:
        raise ValueError(
            "required_products must include level2A for coordinates and time."
        )
    return [product for product in GediProduct if product in products]
