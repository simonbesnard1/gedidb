import h5py
from typing import Iterable, Union, List
import pathlib
import pandas as pd

from gedidb.granule.beam.beam import Beam
from gedidb.granule.granule.granule_name import GediNameMetadata, parse_granule_filename


class Granule(h5py.File):
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

    @property
    def version_product(self) -> str:
        """
        Get the product version from the granule metadata.
        
        Returns:
            str: The product version ID.
        """
        return self["METADATA"]["DatasetIdentification"].attrs["VersionID"]

    @property
    def version_granule(self) -> str:
        """
        Get the granule version from the granule metadata.
        
        Returns:
            str: The granule file name (version).
        """
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def start_datetime(self) -> pd.Timestamp:
        """
        Get the start datetime from the parsed filename metadata.
        
        Returns:
            pd.Timestamp: The start datetime of the granule.
        """
        metadata = self.filename_metadata
        return pd.to_datetime(
            f"{metadata.year}.{metadata.julian_day}.{metadata.hour}:{metadata.minute}:{metadata.second}",
            format="%Y.%j.%H:%M:%S",
        )

    @property
    def product(self) -> str:
        """
        Get the product name from the granule metadata.
        
        Returns:
            str: The product name (shortName).
        """
        return self["METADATA"]["DatasetIdentification"].attrs["shortName"]

    @property
    def uuid(self) -> str:
        """
        Get the UUID of the granule.
        
        Returns:
            str: The granule UUID.
        """
        return self["METADATA"]["DatasetIdentification"].attrs["uuid"]

    @property
    def filename(self) -> str:
        """
        Get the file name of the granule.
        
        Returns:
            str: The granule file name.
        """
        return self["METADATA"]["DatasetIdentification"].attrs["fileName"]

    @property
    def abstract(self) -> str:
        """
        Get the abstract description of the granule.
        
        Returns:
            str: The granule abstract.
        """
        return self["METADATA"]["DatasetIdentification"].attrs["abstract"]

    @property
    def n_beams(self) -> int:
        """
        Get the number of beams in the granule.
        
        Returns:
            int: The number of beams.
        """
        return len(self.beam_names)

    def beam(self, identifier: Union[str, int]) -> Beam:
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
            raise ValueError("Identifier must either be the beam index (int) or beam name (str)")

    def _beam_from_index(self, beam_index: int) -> Beam:
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
        beam_name = self.beam_names[beam_index]
        return self._beam_from_name(beam_name)

    def _beam_from_name(self, beam_name: str) -> Beam:
        """
        Retrieve a Beam object by its name. This method should be implemented in subclasses.
        
        Args:
            beam_name (str): The name of the beam.
        
        Returns:
            Beam: The corresponding Beam object.
        
        Raises:
            NotImplementedError: If the method is not implemented in a subclass.
        """
        raise NotImplementedError("Subclasses must implement _beam_from_name")

    def iter_beams(self) -> Iterable[Beam]:
        """
        Iterate over all beams in the granule.
        
        Returns:
            Iterable[Beam]: An iterable of Beam objects.
        """
        for beam_index in range(self.n_beams):
            yield self._beam_from_index(beam_index)

    def list_beams(self) -> List[Beam]:
        """
        Get a list of all beams in the granule.
        
        Returns:
            List[Beam]: A list of all Beam objects in the granule.
        """
        return list(self.iter_beams())

    def close(self) -> None:
        """
        Close the granule file.
        """
        super().close()

    def __repr__(self) -> str:
        """
        Return a string representation of the granule, including its metadata and beams.
        
        Returns:
            str: The string representation of the granule.
        """
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
