# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import pandas as pd
import h5py
import numpy as np
from typing import Dict, Callable, Optional
import logging

# Configure logging
logger = logging.getLogger(__name__)


class beam_handler(h5py.Group):
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
        self._cached_data: Optional[pd.DataFrame] = (
            None  # Cache for the beam's main data
        )

    @staticmethod
    def apply_filter(
        data: Dict[str, np.ndarray], filters: Optional[Dict[str, Callable]] = None
    ) -> np.ndarray:
        """
        Apply a set of filters to the beam data.

        Args:
            data (Dict[str, np.ndarray]): The beam data in dictionary form.
            filters (Optional[Dict[str, Callable]]): A dictionary of filter functions.

        Returns:
            np.ndarray: A boolean mask indicating which rows pass the filters.
        """
        if not filters:
            return np.ones(len(data["shot_number"]), dtype=bool)

        mask = np.ones(len(data["shot_number"]), dtype=bool)
        for filter_name, filter_func in filters.items():
            try:
                filter_mask = filter_func()
                mask &= filter_mask
            except KeyError:
                logger.warning(
                    f"Filter '{filter_name}' not found in granule. Skipping."
                )
                continue  # Skip filters that are missing in the granule
        return mask

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
    def main_data(self) -> Dict[str, np.ndarray]:
        """
        Retrieve the main data for the beam from the granule file.

        Returns:
            Dict[str, np.ndarray]: A dictionary where keys are variable names and values are NumPy arrays.
        """
        if self._cached_data is None:
            data = self._get_main_data()  # Fetch main data

            # Flatten multi-dimensional profile data
            flattened_data = {}
            for key, value in data.items():
                if isinstance(value, np.ndarray) and value.ndim > 1:
                    flattened_data.update(
                        {f"{key}_{i + 1}": value[:, i] for i in range(value.shape[1])}
                    )
                else:
                    flattened_data[key] = value

            # Create the DataFrame and cache it
            self._cached_data = pd.DataFrame(flattened_data)

        return self._cached_data
