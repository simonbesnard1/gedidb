import os
import tiledb
from typing import Dict, List, Iterator


class SpatialConsolidationPlan:
    def __init__(self, plan_dict: Dict[int, Dict[str, List[str]]]):
        """
        Initialize the spatial consolidation plan.

        Parameters:
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
    @staticmethod
    def compute(array_uri: str, ctx: tiledb.Ctx) -> SpatialConsolidationPlan:
        """
        Generate a spatial consolidation plan for a TileDB array.

        Parameters:
        ----------
        array_uri : str
            URI of the TileDB array.
        ctx : tiledb.Ctx
            TileDB context.

        Returns:
        -------
        SpatialConsolidationPlan
            The spatial consolidation plan object.
        """
        # Retrieve fragment information
        fragment_info = tiledb.FragmentInfoList(array_uri, ctx=ctx)

        # Extract spatial domains
        fragments = SpatialConsolidationPlanner._extract_fragments(fragment_info)

        # Generate groups of spatially overlapping fragments
        plan = SpatialConsolidationPlanner._generate_plan(fragments)

        return SpatialConsolidationPlan(plan)

    @staticmethod
    def _extract_fragments(fragment_info: tiledb.FragmentInfoList) -> List[Dict[str, object]]:
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
            fragments.append({
                "uri": os.path.basename(fragment.uri),
                "latitude_range": nonempty_domain[0],
                "longitude_range": nonempty_domain[1],
            })
        return fragments

    @staticmethod
    def _generate_plan(fragments: List[Dict[str, object]]) -> Dict[int, Dict[str, List[str]]]:
        """
        Generate a plan by grouping overlapping fragments.

        Parameters:
        ----------
        fragments : List[Dict[str, object]]
            List of fragments with spatial domains.

        Returns:
        -------
        Dict[int, Dict[str, List[str]]]
            Consolidation plan grouped by spatial overlap.
        """
        def has_spatial_overlap(frag1: Dict[str, object], frag2: Dict[str, object]) -> bool:
            """Check if two fragments spatially overlap."""
            lat_overlap = frag1["latitude_range"][0] <= frag2["latitude_range"][1] and \
                          frag1["latitude_range"][1] >= frag2["latitude_range"][0]
            lon_overlap = frag1["longitude_range"][0] <= frag2["longitude_range"][1] and \
                          frag1["longitude_range"][1] >= frag2["longitude_range"][0]
            return lat_overlap and lon_overlap

        visited = set()
        plan = {}
        node_id = 0

        for fragment in fragments:
            if fragment["uri"] in visited:
                continue

            current_node = {
                "num_fragments": 0,
                "fragment_uris": []
            }

            stack = [fragment]
            while stack:
                frag = stack.pop()
                if frag["uri"] in visited:
                    continue

                visited.add(frag["uri"])
                current_node["fragment_uris"].append(frag["uri"])
                current_node["num_fragments"] += 1

                # Find all overlapping fragments
                for candidate in fragments:
                    if candidate["uri"] not in visited and has_spatial_overlap(frag, candidate):
                        stack.append(candidate)

            plan[node_id] = current_node
            node_id += 1

        return plan
