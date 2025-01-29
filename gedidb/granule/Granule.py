# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import h5py
from typing import Iterable, Union, List
import pathlib
import pandas as pd

from gedidb.beam.Beam import beam_handler
from gedidb.granule.granule_name import GediNameMetadata, parse_granule_filename


class granule_handler(h5py.File):
    """
    Represents a GEDI Granule HDF5 file, providing access to metadata and beams.

    Attributes:
        file_path (pathlib.Path): The path to the granule file.
        beam_names (List[str]): A list of beam names in the granule.
    """

    def __init__(self, file_path: pathlib.Path):
        """
        Initialize the Granule object by opening the HDF5 file and extracting beam names.

        Args:
            file_path (pathlib.Path): Path to the HDF5 granule file.
        """
        super().__init__(file_path, "r")
        self.file_path = file_path
        self.beam_names = [name for name in self.keys() if name.startswith("BEAM")]
        self._parsed_filename_metadata = None

    @property
    def filename_metadata(self) -> GediNameMetadata:
        """
        Lazily load and cache the parsed metadata from the granule's filename.

        Returns:
            GediNameMetadata: Parsed metadata from the granule filename.
        """
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = parse_granule_filename(self.filename)
        return self._parsed_filename_metadata

    def _get_metadata_attr(self, attr_name: str) -> str:
        """
        Helper to retrieve metadata attributes with error handling.

        Args:
            attr_name (str): Attribute name to fetch.

        Returns:
            str: Attribute value if it exists, otherwise an empty string.
        """
        try:
            return self["METADATA"]["DatasetIdentification"].attrs.get(attr_name, "")
        except KeyError:
            return ""

    @property
    def version_product(self) -> str:
        """Get the product version from the granule metadata."""
        return self._get_metadata_attr("VersionID")

    @property
    def version_granule(self) -> str:
        """Get the granule version from the granule metadata."""
        return self._get_metadata_attr("fileName")

    @property
    def start_datetime(self) -> pd.Timestamp:
        """Get the start datetime from the parsed filename metadata."""
        metadata = self.filename_metadata
        return pd.Timestamp(
            f"{metadata.year}-{metadata.julian_day} {metadata.hour}:{metadata.minute}:{metadata.second}",
            format="%Y-%j %H:%M:%S",
        )

    @property
    def product(self) -> str:
        """Get the product name from the granule metadata."""
        return self._get_metadata_attr("shortName")

    @property
    def uuid(self) -> str:
        """Get the UUID of the granule."""
        return self._get_metadata_attr("uuid")

    @property
    def filename(self) -> str:
        """Get the file name of the granule."""
        return self.file_path.name

    @property
    def abstract(self) -> str:
        """Get the abstract description of the granule."""
        return self._get_metadata_attr("abstract")

    @property
    def n_beams(self) -> int:
        """Get the number of beams in the granule."""
        return len(self.beam_names)

    def beam(self, identifier: Union[str, int]) -> beam_handler:
        """
        Get a Beam object by its name or index.

        Args:
            identifier (Union[str, int]): Beam name (str) or beam index (int).

        Returns:
            Beam: The corresponding Beam object.

        Raises:
            ValueError: If the identifier is neither a valid beam index nor beam name.
        """
        if isinstance(identifier, int):
            return self._beam_from_index(identifier)
        elif isinstance(identifier, str):
            return self._beam_from_name(identifier)
        else:
            raise ValueError(
                "Identifier must either be the beam index (int) or beam name (str)"
            )

    def _beam_from_index(self, beam_index: int) -> beam_handler:
        """
        Retrieve a Beam object by its index.

        Args:
            beam_index (int): The index of the beam.

        Returns:
            Beam: The corresponding Beam object.

        Raises:
            ValueError: If the index is out of bounds.
        """
        if not 0 <= beam_index < self.n_beams:
            raise ValueError(f"Beam index must be between 0 and {self.n_beams - 1}")
        return self._beam_from_name(self.beam_names[beam_index])

    def _beam_from_name(self, beam_name: str) -> beam_handler:
        """
        Retrieve a Beam object by its name.

        Args:
            beam_name (str): The name of the beam.

        Returns:
            Beam: The corresponding Beam object.

        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError("Subclasses must implement _beam_from_name")

    def iter_beams(self) -> Iterable[beam_handler]:
        """Iterate over all beams in the granule."""
        return (self._beam_from_index(idx) for idx in range(self.n_beams))

    def list_beams(self) -> List[beam_handler]:
        """Get a list of all beams in the granule."""
        return list(self.iter_beams())

    def close(self) -> None:
        """Close the granule file."""
        super().close()

    def __repr__(self) -> str:
        """Return a string representation of the granule."""
        try:
            metadata = self.filename_metadata
            description = (
                f"GEDI Granule:\n"
                f" Granule name: {self.filename}\n"
                f" Sub-granule:  {metadata.sub_orbit_granule}\n"
                f" Product:      {self.product}\n"
                f" Release:      {metadata.release_number}\n"
                f" No. beams:    {self.n_beams}\n"
                f" Start date:   {self.start_datetime.date()}\n"
                f" Start time:   {self.start_datetime.time()}\n"
                f" HDF object:   {super().__repr__()}"
            )
        except AttributeError as e:
            description = f"GEDI Granule (Error in metadata: {e})"
        return description
