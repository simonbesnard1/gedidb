# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class GediNameMetadata:
    """
    Container for metadata derived from GEDI file name conventions.

    Attributes:
        product (str): The product type (e.g., L1B, L2A).
        year (str): The year of data acquisition (4 digits).
        julian_day (str): The Julian day of the year when data was acquired.
        hour (str): The hour (UTC) when data was acquired.
        minute (str): The minute (UTC) when data was acquired.
        second (str): The second (UTC) when data was acquired.
        orbit (str): The orbit number associated with the granule.
        sub_orbit_granule (str): The sub-orbit granule identifier.
        ground_track (str): The ground track number.
        positioning (str): The positioning number.
        release_number (str): The release version of the product.
        granule_production_version (str): The granule production version.
        major_version_number (str): The major version of the product.
    """

    product: str
    year: str
    julian_day: str
    hour: str
    minute: str
    second: str
    orbit: str
    sub_orbit_granule: str
    ground_track: str
    positioning: str
    release_number: str
    granule_production_version: str
    major_version_number: str


# Precompile the GEDI filename pattern for efficient reuse
GEDI_FILENAME_PATTERN = re.compile(
    (
        r"(?P<product>\w+_\w)"  # Product identifier (e.g., GEDI_L2A)
        r"_(?P<year>\d{4})"  # Year (4 digits)
        r"(?P<julian_day>\d{3})"  # Julian day (3 digits)
        r"(?P<hour>\d{2})"  # Hour (2 digits)
        r"(?P<minute>\d{2})"  # Minute (2 digits)
        r"(?P<second>\d{2})"  # Second (2 digits)
        r"_(?P<orbit>O\d+)"  # Orbit (O followed by digits)
        r"_(?P<sub_orbit_granule>\d{2})"  # Sub-orbit granule (2 digits)
        r"_(?P<ground_track>T\d+)"  # Ground track (T followed by digits)
        r"_(?P<positioning>\d{2})"  # Positioning number (2 digits)
        r"_(?P<release_number>\d{3})"  # Release number (3 digits)
        r"_(?P<granule_production_version>\d{2})"  # Granule production version (2 digits)
        r"_(?P<major_version_number>V\d+)"  # Major version number (V followed by digits)
    )
)


def parse_granule_filename(gedi_filename: str) -> GediNameMetadata:
    """
    Parses the GEDI filename and extracts metadata into a structured format.

    Parameters:
        gedi_filename (str): The GEDI filename to parse.

    Returns:
        GediNameMetadata: An instance of GediNameMetadata with extracted components.

    Raises:
        ValueError: If the filename does not match the expected GEDI naming pattern.
    """
    match = GEDI_FILENAME_PATTERN.search(gedi_filename)
    if match is None:
        raise ValueError(
            f"Filename '{gedi_filename}' does not match the expected GEDI naming pattern: {GEDI_FILENAME_PATTERN.pattern}"
        )
    return GediNameMetadata(**match.groupdict())
