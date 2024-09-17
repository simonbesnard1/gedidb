# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de and besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import geopandas as gpd
import h5py
import numpy as np
from typing import Union, List, Dict, Callable, Optional

from gedidb.utils.constants import WGS84


class Beam(h5py.Group):
    """
    Represents a single beam in a GEDI granule file, inheriting from h5py.Group.
    Provides methods to extract and process the beam data, including filtering, caching, and SQL formatting.
    """
    
    def __init__(self, granule: h5py.File, beam: str, field_mapping: Dict[str, str]):
        """
        Initialize the Beam class.

        Args:
            granule (h5py.File): The parent granule file.
            beam (str): The name of the beam in the granule.
            field_mapping (Dict[str, str]): A dictionary mapping field names in the data.
        """
        super().__init__(granule[beam].id)
        self.parent_granule = granule
        self.field_mapping = field_mapping
        self._cached_data: Optional[gpd.GeoDataFrame] = None  # Cache for the beam's main data

    @staticmethod
    def apply_filter(data: Dict[str, np.ndarray], filters: Optional[Dict[str, Callable]] = None) -> np.ndarray:
        """
        Apply a set of filters to the beam data.

        Args:
            data (Dict[str, np.ndarray]): The beam data in dictionary form.
            filters (Optional[Dict[str, Callable]]): A dictionary of filter functions.

        Returns:
            np.ndarray: A boolean mask indicating which rows pass the filters.
        """
        if filters is not None:
            mask = np.ones(len(data['beam_name']), dtype=bool)
            for filter_name, filter_func in filters.items():
                filter_mask = filter_func(data)
                mask &= filter_mask
            return mask
        return np.ones(len(data['beam_name']), dtype=bool)

    @property
    def n_shots(self) -> int:
        """
        Get the number of shots in the beam.

        Returns:
            int: The number of shots in the beam.
        """
        return len(self["beam"])

    @property
    def beam_type(self) -> str:
        """
        Get the beam type (e.g., 'full', 'coverage') based on the beam description attribute.

        Returns:
            str: The beam type.
        """
        return self.attrs["description"].split(" ")[0].lower()

    @property
    def field_mapper(self) -> Dict[str, str]:
        """
        Return the field mapping dictionary for the beam.

        Returns:
            Dict[str, str]: The field mapping dictionary.
        """
        return self.field_mapping

    @property
    def main_data(self) -> gpd.GeoDataFrame:
        """
        Get the main data for the beam as a GeoDataFrame, cached for efficiency.

        Returns:
            gpd.GeoDataFrame: The main beam data in GeoDataFrame format.
        """
        if self._cached_data is None:
            data = self._get_main_data()  # Fetch main data

            if data is not None:
                geometry = self.shot_geolocations  # Get shot geolocations

                # Apply any filters that have been set
                if hasattr(self, '_filtered_index'):
                    geometry = geometry[self._filtered_index]

                # Convert multi-dimensional arrays to lists for GeoDataFrame compatibility
                for key, value in data.items():
                    if isinstance(value, np.ndarray) and value.ndim > 1:
                        data[key] = value.tolist()

                # Create the GeoDataFrame and cache it
                self._cached_data = gpd.GeoDataFrame(data, geometry=geometry, crs=WGS84)

                # Apply SQL formatting to array-type fields
                self.sql_format_arrays()

        return self._cached_data

    def sql_format_arrays(self):
        """
        Convert array-type columns to SQL-compatible string representations for insertion into databases.
        """
        array_cols = [c for c in self._cached_data.columns if c.endswith("_z") or c == "rh"]

        for col in array_cols:
            self._cached_data[col] = self._cached_data[col].map(self._arr_to_str)

    @staticmethod
    def _arr_to_str(arr: Union[List[float], np.ndarray, float]) -> str:
        """
        Convert a list or array to a string representation suitable for SQL storage.

        Args:
            arr (Union[List[float], np.ndarray, float]): The array or list to convert.

        Returns:
            str: The string representation of the array.
        """
        if isinstance(arr, (list, np.ndarray)):
            return "{" + ", ".join(map(str, arr)) + "}"
        return str(arr)

    def _get_main_data(self) -> Optional[Dict[str, np.ndarray]]:
        """
        Retrieve the main data for the beam. 
        This method should be implemented in a subclass or elsewhere in the code.

        Returns:
            Optional[Dict[str, np.ndarray]]: The main beam data, or None if not available.
        """
        raise NotImplementedError("The '_get_main_data' method needs to be implemented.")
    
    @property
    def shot_geolocations(self) -> gpd.GeoSeries:
        """
        Get the geolocations of the shots in the beam.
        This property should return the GeoSeries containing shot geolocations.

        Returns:
            gpd.GeoSeries: A GeoSeries of shot geolocations.
        """
        raise NotImplementedError("The 'shot_geolocations' property needs to be implemented.")
