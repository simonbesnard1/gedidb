# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import logging
import os
from typing import Dict, Iterator, List, Tuple, Union

import tiledb

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
    def compute(
        array_uri: str,
        ctx: tiledb.Ctx,
        min_group_cells: int = 5_000,
        max_fragments_per_group: int = 200,
    ) -> SpatialConsolidationPlan:
        """
        Generate a spatial consolidation plan for a TileDB array.

        Parameters
        ----------
        array_uri : str
            URI of the TileDB array.
        ctx : tiledb.Ctx
            TileDB context.
        min_group_cells : int
            Groups with fewer total cells than this are absorbed into their
            nearest spatial neighbour.  Singleton groups (no overlap partner)
            are always absorbed.  Set to 0 to disable the threshold.
        max_fragments_per_group : int
            Maximum fragments allowed in a single consolidation call.
            Groups larger than this are split into spatially sorted batches.
            TileDB divides ``sm.memory_budget`` equally across all fragments
            in one call; capping the group size prevents per-fragment memory
            exhaustion on large arrays.  Set to 0 to disable the cap.

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

        plan = SpatialConsolidationPlanner._generate_plan(
            fragments,
            min_group_cells=min_group_cells,
            max_fragments_per_group=max_fragments_per_group,
        )
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
            List of fragments with spatial domains and cell counts.
        """
        fragments = []
        for fragment in fragment_info:
            nonempty_domain = fragment.nonempty_domain
            fragments.append(
                {
                    "uri": os.path.basename(fragment.uri),
                    "latitude_range": nonempty_domain[0],
                    "longitude_range": nonempty_domain[1],
                    "cell_num": fragment.cell_num,
                }
            )
        return fragments

    @staticmethod
    def _generate_plan(
        fragments: List[Dict[str, Union[str, Tuple[float, float]]]],
        min_group_cells: int = 5_000,
        max_fragments_per_group: int = 200,
    ) -> Dict[int, Dict[str, List[str]]]:
        """
        Generate a plan by grouping overlapping fragments, then absorbing
        singleton or under-populated groups into their nearest spatial neighbour,
        then splitting any group that still exceeds ``max_fragments_per_group``.

        Parameters
        ----------
        fragments : List[Dict[str, Union[str, Tuple[float, float]]]]
            List of fragments with spatial domains and cell counts.
        min_group_cells : int
            Groups whose total cell count is below this threshold are absorbed
            into the nearest group by centroid distance.  Singleton groups
            (one fragment, no overlap partner) are always absorbed regardless
            of cell count.  Set to 0 to disable the cell-count threshold.
        max_fragments_per_group : int
            Hard cap on the number of fragments in a single consolidation call.
            Oversized groups are sorted by latitude centroid and split into
            batches of at most this size.  Set to 0 to disable.

        Returns
        -------
        Dict[int, Dict[str, List[str]]]
            Consolidation plan grouped by spatial overlap.
        """

        def has_spatial_overlap(f1, f2) -> bool:
            return (
                f1["latitude_range"][0] <= f2["latitude_range"][1]
                and f1["latitude_range"][1] >= f2["latitude_range"][0]
                and f1["longitude_range"][0] <= f2["longitude_range"][1]
                and f1["longitude_range"][1] >= f2["longitude_range"][0]
            )

        # ── build URI → metadata lookup for centroid computation ──────────────
        meta = {f["uri"]: f for f in fragments}

        # ── DFS grouping by spatial overlap ───────────────────────────────────
        visited = set()
        plan = {}
        node_id = 0
        unvisited = dict(meta)  # mutable copy

        while unvisited:
            _, fragment = unvisited.popitem()
            current_node = {"num_fragments": 0, "fragment_uris": [], "cell_num": 0}

            stack = [fragment]
            while stack:
                frag = stack.pop()
                if frag["uri"] in visited:
                    continue
                visited.add(frag["uri"])
                current_node["fragment_uris"].append(frag["uri"])
                current_node["num_fragments"] += 1
                current_node["cell_num"] += frag["cell_num"]

                for uri, candidate in list(unvisited.items()):
                    if has_spatial_overlap(frag, candidate):
                        stack.append(candidate)
                        del unvisited[uri]

            plan[node_id] = current_node
            node_id += 1

        # ── absorb singleton / under-populated groups ─────────────────────────
        def _group_centroid(node):
            lats = [
                0.5 * (meta[u]["latitude_range"][0] + meta[u]["latitude_range"][1])
                for u in node["fragment_uris"]
            ]
            lons = [
                0.5 * (meta[u]["longitude_range"][0] + meta[u]["longitude_range"][1])
                for u in node["fragment_uris"]
            ]
            return sum(lats) / len(lats), sum(lons) / len(lons)

        def _centroid_distance(c1, c2):
            return ((c1[0] - c2[0]) ** 2 + (c1[1] - c2[1]) ** 2) ** 0.5

        changed = True
        while changed:
            changed = False
            small_ids = [
                nid
                for nid, node in plan.items()
                if min_group_cells > 0 and node["cell_num"] < min_group_cells
            ]
            for small_id in small_ids:
                if small_id not in plan:
                    continue
                small_node = plan[small_id]
                small_c = _group_centroid(small_node)

                # find nearest other group
                best_id, best_dist = None, float("inf")
                for nid, node in plan.items():
                    if nid == small_id:
                        continue
                    d = _centroid_distance(small_c, _group_centroid(node))
                    if d < best_dist:
                        best_dist, best_id = d, nid

                if best_id is None:
                    continue  # only one group left — nothing to absorb into

                # merge small_id into best_id
                plan[best_id]["fragment_uris"].extend(small_node["fragment_uris"])
                plan[best_id]["num_fragments"] += small_node["num_fragments"]
                plan[best_id]["cell_num"] += small_node["cell_num"]
                del plan[small_id]
                changed = True

        # ── split oversized groups ────────────────────────────────────────────
        # TileDB divides sm.memory_budget equally across all fragments in a
        # single consolidate() call.  If a group has too many fragments the
        # per-fragment budget drops below tile size and the call fails.  Split
        # large groups into spatially sorted batches so each call stays within
        # the budget.
        if max_fragments_per_group and max_fragments_per_group > 0:
            split_plan: Dict[int, Dict] = {}
            for node in plan.values():
                uris = node["fragment_uris"]
                if len(uris) <= max_fragments_per_group:
                    split_plan[len(split_plan)] = node
                else:
                    # Sort by latitude centroid for spatial locality
                    sorted_uris = sorted(
                        uris,
                        key=lambda u: 0.5
                        * (meta[u]["latitude_range"][0] + meta[u]["latitude_range"][1]),
                    )
                    for i in range(0, len(sorted_uris), max_fragments_per_group):
                        chunk = sorted_uris[i : i + max_fragments_per_group]
                        split_plan[len(split_plan)] = {
                            "num_fragments": len(chunk),
                            "fragment_uris": chunk,
                            "cell_num": sum(meta[u]["cell_num"] for u in chunk),
                        }
            plan = split_plan

        # ── renumber contiguously ─────────────────────────────────────────────
        return {i: node for i, node in enumerate(plan.values())}
