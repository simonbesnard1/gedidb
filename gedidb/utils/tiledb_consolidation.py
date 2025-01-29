# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import os
import tiledb
from typing import Dict, List, Iterator, Tuple, Union
import logging

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


class SpatialConsolidationPlan:
    def __init__(self, plan_dict: Dict[int, Dict[str, List[str]]]):
        """
        Initialize the spatial consolidation plan.

        Parameters
        ----------
        plan_dict : Dict[int, Dict[str, List[str]]]
            Dictionary where keys are node IDs and values are dictionaries
            with 'num_fragments' and 'fragment_uris'.
        """
        self._plan_dict = plan_dict

    def __getitem__(self, index: int) -> Dict[str, List[str]]:
        """Get the plan node by index."""
        return self._plan_dict[index]

    def __len__(self) -> int:
        """Return the number of nodes in the plan."""
        return len(self._plan_dict)

    def __iter__(self) -> Iterator[Dict[str, List[str]]]:
        """Iterate over the nodes in the plan."""
        return iter(self._plan_dict.values())

    def items(self) -> Iterator:
        """Iterate over node IDs and their corresponding details."""
        return self._plan_dict.items()

    def dump(self) -> Dict[int, Dict[str, List[str]]]:
        """Dump the full plan as a dictionary."""
        return self._plan_dict


class SpatialConsolidationPlanner:
    """
    Generate a spatial consolidation plan for a TileDB array.
    """
    @staticmethod
    def compute(array_uri: str, ctx: tiledb.Ctx) -> SpatialConsolidationPlan:
        """
        Generate a spatial consolidation plan for a TileDB array.

        Parameters
        ----------
        array_uri : str
            URI of the TileDB array.
        ctx : tiledb.Ctx
            TileDB context.

        Returns
        -------
        SpatialConsolidationPlan
            The spatial consolidation plan object.
        """
        logger.info(f"Generating spatial consolidation plan for array: {array_uri}")

        try:
            fragment_info = tiledb.FragmentInfoList(array_uri, ctx=ctx)
        except Exception as e:
            logger.error(f"Failed to retrieve fragment info for {array_uri}: {e}")
            raise

        fragments = SpatialConsolidationPlanner._extract_fragments(fragment_info)
        if not fragments:
            logger.warning(f"No fragments found for array: {array_uri}")
            return SpatialConsolidationPlan({})

        plan = SpatialConsolidationPlanner._generate_plan(fragments)
        return SpatialConsolidationPlan(plan)

    @staticmethod
    def _extract_fragments(
        fragment_info: tiledb.FragmentInfoList,
    ) -> List[Dict[str, object]]:
        """
        Extract fragment metadata and spatial domains.

        Parameters:
        ----------
        fragment_info : tiledb.FragmentInfoList
            List of fragment metadata.

        Returns:
        -------
        List[Dict[str, object]]
            List of fragments with spatial domains.
        """
        fragments = []
        for fragment in fragment_info:
            nonempty_domain = fragment.nonempty_domain
            fragments.append(
                {
                    "uri": os.path.basename(fragment.uri),
                    "latitude_range": nonempty_domain[0],
                    "longitude_range": nonempty_domain[1],
                }
            )
        return fragments

    @staticmethod
    def _generate_plan(
        fragments: List[Dict[str, Union[str, Tuple[float, float]]]]
    ) -> Dict[int, Dict[str, List[str]]]:
        """
        Generate a plan by grouping overlapping fragments.

        Parameters
        ----------
        fragments : List[Dict[str, Union[str, Tuple[float, float]]]]
            List of fragments with spatial domains.

        Returns
        -------
        Dict[int, Dict[str, List[str]]]
            Consolidation plan grouped by spatial overlap.
        """

        def has_spatial_overlap(
            frag1: Dict[str, Union[str, Tuple[float, float]]],
            frag2: Dict[str, Union[str, Tuple[float, float]]],
        ) -> bool:
            """Check if two fragments spatially overlap."""
            return (
                frag1["latitude_range"][0] <= frag2["latitude_range"][1] and frag1["latitude_range"][1] >= frag2["latitude_range"][0] and frag1["longitude_range"][0] <= frag2["longitude_range"][1] and frag1["longitude_range"][1] >= frag2["longitude_range"][0]
            )

        visited = set()
        plan = {}
        node_id = 0

        # Create a lookup dictionary for unvisited fragments
        unvisited = {frag["uri"]: frag for frag in fragments}

        while unvisited:
            # Pop one fragment from the unvisited list
            _, fragment = unvisited.popitem()
            current_node = {"num_fragments": 0, "fragment_uris": []}

            # Initialize a stack for depth-first search
            stack = [fragment]
            while stack:
                frag = stack.pop()
                if frag["uri"] in visited:
                    continue

                visited.add(frag["uri"])
                current_node["fragment_uris"].append(frag["uri"])
                current_node["num_fragments"] += 1

                # Find overlapping fragments
                for uri, candidate in list(unvisited.items()):
                    if has_spatial_overlap(frag, candidate):
                        stack.append(candidate)
                        del unvisited[uri]  # Mark as visited by removing from unvisited

            # Assign the current node to the plan
            plan[node_id] = current_node
            node_id += 1

        return plan
