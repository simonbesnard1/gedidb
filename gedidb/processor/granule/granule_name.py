from dataclasses import dataclass
import re

@dataclass
class GediNameMetadata:
    """Data class container for metadata derived from GEDI file name conventions. THE ORDER MUST MATCH THE ORDER OF THE REGEX GROUPS."""

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


GEDI_SUBPATTERN = GediNameMetadata(
    product=r"\w+_\w",
    year=r"\d{4}",
    julian_day=r"\d{3}",
    hour=r"\d{2}",
    minute=r"\d{2}",
    second=r"\d{2}",
    orbit=r"O\d+",
    sub_orbit_granule=r"\d{2}",
    ground_track=r"T\d+",
    positioning=r"\d{2}",
    release_number=r"\d{3}",
    granule_production_version=r"\d{2}",
    major_version_number=r"V\d+",
)


def parse_granule_filename(gedi_filename: str) -> GediNameMetadata:
    gedi_naming_pattern = re.compile(
        (
            f"({GEDI_SUBPATTERN.product})"
            f"_({GEDI_SUBPATTERN.year})"
            f"({GEDI_SUBPATTERN.julian_day})"
            f"({GEDI_SUBPATTERN.hour})"
            f"({GEDI_SUBPATTERN.minute})"
            f"({GEDI_SUBPATTERN.second})"
            f"_({GEDI_SUBPATTERN.orbit})"
            f"_({GEDI_SUBPATTERN.sub_orbit_granule})"
            f"_({GEDI_SUBPATTERN.ground_track})"
            f"_({GEDI_SUBPATTERN.positioning})"
            f"_({GEDI_SUBPATTERN.release_number})"
            f"_({GEDI_SUBPATTERN.granule_production_version})"
            f"_({GEDI_SUBPATTERN.major_version_number})"
        )
    )
    parse_result = re.search(gedi_naming_pattern, gedi_filename)
    if parse_result is None:
        raise ValueError(
            f"Filename {gedi_filename} does not conform the the GEDI naming pattern."
        )
    return GediNameMetadata(*parse_result.groups())
