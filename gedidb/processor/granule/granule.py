import h5py
from typing import Iterable, Union
import pathlib
import pandas as pd

from gedidb.processor.beam.beam import Beam
from gedidb.processor.granule.granule_name import GediNameMetadata, parse_granule_filename

class Granule(h5py.File):
    """Represents a GEDI Granule HDF5 file, providing access to metadata and beams."""
    
    def __init__(self, file_path: pathlib.Path):
        super().__init__(file_path, "r")
        self.file_path = file_path
        self.beam_names = [name for name in self.keys() if name.startswith("BEAM")]
        self._parsed_filename_metadata = None

    @property
    def filename_metadata(self) -> GediNameMetadata:
        if self._parsed_filename_metadata is None:
            self._parsed_filename_metadata = parse_granule_filename(self.filename)
        return self._parsed_filename_metadata

    @property
    def version_product(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["VersionID"]

    @property
    def version_granule(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def start_datetime(self) -> pd.Timestamp:
        metadata = self.filename_metadata
        return pd.to_datetime(
            f"{metadata.year}.{metadata.julian_day}.{metadata.hour}:{metadata.minute}:{metadata.second}",
            format="%Y.%j.%H:%M:%S",
        )

    @property
    def product(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["shortName"]

    @property
    def uuid(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["uuid"]

    @property
    def filename(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def abstract(self) -> str:
        return self["METADATA"]["DatasetIdentification"].attrs["abstract"]

    @property
    def n_beams(self) -> int:
        return len(self.beam_names)

    def beam(self, identifier: Union[str, int]) -> Beam:
        if isinstance(identifier, int):
            return self._beam_from_index(identifier)
        elif isinstance(identifier, str):
            return self._beam_from_name(identifier)
        else:
            raise ValueError("Identifier must either be the beam index (int) or beam name (str)")

    def _beam_from_index(self, beam_index: int) -> Beam:
        if not 0 <= beam_index < self.n_beams:
            raise ValueError(f"Beam index must be between 0 and {self.n_beams - 1}")
        beam_name = self.beam_names[beam_index]
        return self._beam_from_name(beam_name)

    def _beam_from_name(self, beam_name: str) -> Beam:
        """Subclass must implement this method to return a Beam object."""
        raise NotImplementedError("Subclasses must implement _beam_from_name")

    def iter_beams(self) -> Iterable[Beam]:
        """Iterate over all beams in the granule."""
        for beam_index in range(self.n_beams):
            yield self._beam_from_index(beam_index)

    def list_beams(self) -> list[Beam]:
        """Return a list of all beams in the granule."""
        return list(self.iter_beams())

    def close(self) -> None:
        """Close the granule file."""
        super().close()

    def __repr__(self) -> str:
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
            description = (
                f"GEDI Granule:\n"
                f" Granule name: {self.filename}\n"
                f" Product:      {self.product}\n"
                f" No. beams:    {self.n_beams}\n"
                f" Start date:   {self.start_datetime.date()}\n"
                f" Start time:   {self.start_datetime.time()}\n"
                f" HDF object:   {super().__repr__()}\n"
                f" (Error: {e})"
            )
        return description
