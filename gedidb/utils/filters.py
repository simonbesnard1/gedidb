# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import tiledb
from typing import Any, Dict, Optional


class TileDBFilterPolicy:
    """
    Encapsulates TileDB filter selection logic for attributes and dimensions.

    Uses:
    - Config to control Zstd levels / tuning.
    - Dtype-based rules to avoid per-variable special cases.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Parameters
        ----------
        cfg : dict, optional
            Sub-dictionary of your global config relevant to TileDB.
            Typically `config.get("tiledb", {})`.
        """
        self.cfg = cfg or {}

    # ---------- Dimension filters (if you want them here too) ----------

    def spatial_dim_filters(self, scale_factor: float = 1e-6) -> tiledb.FilterList:
        """
        Filters for latitude/longitude dimensions:
        - FloatScale (to int32)
        - DoubleDelta
        - BitWidthReduction
        - Zstd(level=3)
        """
        return tiledb.FilterList(
            [
                tiledb.FloatScaleFilter(
                    factor=scale_factor,
                    offset=0.0,
                    bytewidth=4,
                ),
                tiledb.DoubleDeltaFilter(),
                tiledb.BitWidthReductionFilter(),
                tiledb.ZstdFilter(level=3),
            ]
        )

    def time_dim_filters(self) -> tiledb.FilterList:
        """
        Filters for time dimension (int64, monotonic-ish).
        """
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=3),
            ]
        )

    # ---------- Attribute filters (dtype-based) ----------

    def _filters_float32(self) -> tiledb.FilterList:
        lvl = int(self.cfg.get("float32_zstd_level", 5))
        return tiledb.FilterList(
            [
                tiledb.ByteShuffleFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )

    def _filters_float64(self) -> tiledb.FilterList:
        lvl = int(self.cfg.get("float64_zstd_level", 4))
        return tiledb.FilterList(
            [
                tiledb.ByteShuffleFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )

    def _filters_flags_uint8(self) -> tiledb.FilterList:
        """
        Flags / enums:
        - BitWidthReduction (if available)
        - RLE
        - Zstd
        """
        lvl = int(self.cfg.get("flags_zstd_level", 3))
        fl = []
        try:
            fl.append(tiledb.BitWidthReductionFilter())
        except Exception:
            pass
        fl.extend(
            [
                tiledb.RleFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )
        return tiledb.FilterList(fl)

    def _filters_int_generic(self) -> tiledb.FilterList:
        """
        Generic ints:
        - BitWidthReduction (if available)
        - Zstd
        """
        lvl = int(self.cfg.get("int_zstd_level", 3))
        fl = []
        try:
            fl.append(tiledb.BitWidthReductionFilter())
        except Exception:
            pass
        fl.append(tiledb.ZstdFilter(level=lvl))
        return tiledb.FilterList(fl)

    def _filters_utf8(self) -> tiledb.FilterList:
        lvl = int(self.cfg.get("string_zstd_level", 3))
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def filters_for_dtype(self, dtype: Any) -> tiledb.FilterList:
        """
        Return a TileDB FilterList for a given dtype.

        Rules
        -----
        - float32 → ByteShuffle + Zstd (high-ish level)
        - float64 → ByteShuffle + Zstd (slightly lower)
        - uint8  → flags-style compression: (BitWidthReduction) + RLE + Zstd
        - other ints → (BitWidthReduction) + Zstd
        - unicode strings → Zstd
        - fallback → Zstd(level=3)
        """
        dt = np.dtype(dtype)
        kind = dt.kind  # 'f', 'i', 'u', 'b', 'U', ...

        if kind == "f":
            if dt == np.float32:
                return self._filters_float32()
            if dt == np.float64:
                return self._filters_float64()
            return self._filters_float64()

        if kind in ("i", "u"):
            if dt == np.uint8:
                return self._filters_flags_uint8()
            return self._filters_int_generic()

        if kind == "U":
            return self._filters_utf8()

        # bools, bytes, other oddballs
        return tiledb.FilterList([tiledb.ZstdFilter(level=3)])

    def timestamp_filters(self) -> tiledb.FilterList:
        """
        Filters for `timestamp_ns` attribute.
        """
        try:
            return tiledb.FilterList(
                [
                    tiledb.BitWidthReductionFilter(),
                    tiledb.ZstdFilter(level=2),
                ]
            )
        except Exception:
            return tiledb.FilterList([tiledb.ZstdFilter(level=2)])
