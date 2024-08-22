from dataclasses import dataclass
import re

@dataclass(frozen=True)
class GediNameMetadata:
    """
    Container for metadata derived from GEDI file name conventions.

    The order of attributes must match the order of the regex groups used in parsing.
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

# Precompile the GEDI filename pattern for reuse
GEDI_FILENAME_PATTERN = re.compile(
    (
        r"(?P<product>\w+_\w)"
        r"_(?P<year>\d{4})"
        r"(?P<julian_day>\d{3})"
        r"(?P<hour>\d{2})"
        r"(?P<minute>\d{2})"
        r"(?P<second>\d{2})"
        r"_(?P<orbit>O\d+)"
        r"_(?P<sub_orbit_granule>\d{2})"
        r"_(?P<ground_track>T\d+)"
        r"_(?P<positioning>\d{2})"
        r"_(?P<release_number>\d{3})"
        r"_(?P<granule_production_version>\d{2})"
        r"_(?P<major_version_number>V\d+)"
    )
)

def parse_granule_filename(gedi_filename: str) -> GediNameMetadata:
    """
    Parses the GEDI filename into its metadata components.

    Parameters:
    - gedi_filename (str): The filename to parse.

    Returns:
    - GediNameMetadata: An instance of GediNameMetadata containing parsed components.

    Raises:
    - ValueError: If the filename does not conform to the expected pattern.
    """
    parse_result = GEDI_FILENAME_PATTERN.search(gedi_filename)
    if parse_result is None:
        raise ValueError(
            f"Filename '{gedi_filename}' does not conform to the expected GEDI naming pattern: {GEDI_FILENAME_PATTERN.pattern}"
        )
    return GediNameMetadata(**parse_result.groupdict())
