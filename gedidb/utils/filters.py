# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

import numpy as np
import tiledb
from typing import Any, Dict, Optional


class TileDBFilterPolicy:
    """
    Encapsulates TileDB filter selection for GEDI attributes and dimensions.

    Filter design principles
    ------------------------
    Each filter in a chain must earn its place. The rules applied here:

    1. One pre-processing transform + one entropy coder is usually optimal.
       Stacking multiple transforms rarely helps and always costs CPU time.

    2. Pre-processing transforms (ByteShuffle, DoubleDelta, FloatScale) make
       data more compressible for the entropy coder (Zstd). They don't compress
       by themselves — Zstd must follow them to realise the gain.

    3. ByteShuffle is the right pre-processor for floats: it separates the
       exponent bytes from the mantissa bytes, so Zstd sees better repetition.
       It is NOT useful for integers — Zstd already handles those well directly.

    4. DoubleDelta is the right pre-processor for monotonically increasing
       integers (timestamps, time dimensions). It stores second-order differences
       which are often near-zero, dramatically reducing entropy. It should NOT be
       applied to lat/lon after Hilbert sort — those are not monotonic.

    5. BitWidthReduction is only worth applying to narrow integers (≤ 4 bytes)
       that don't use their full range. Applying it to int64 timestamps or
       post-FloatScale values is a no-op or counterproductive.

    6. RLE is only worth applying before Zstd when long runs of identical values
       are expected. For GEDI quality flags this is unlikely — Zstd handles
       short repetitions directly and doesn't need RLE as a pre-pass.

    Zstd level guidance
    -------------------
    Level 1–2 : fastest writes, ~60-70 % of level-9 ratio. Good for timestamps.
    Level 3–4 : balanced. Recommended default for float attributes.
    Level 9+  : diminishing returns past level 5 for most geophysical data.
    """

    def __init__(self, cfg: Optional[Dict[str, Any]] = None) -> None:
        """
        Parameters
        ----------
        cfg : dict, optional
            TileDB sub-dictionary of the global config, i.e. config.get("tiledb", {}).

        Filter behaviour is controlled by ``use_filters`` in the config:

        ``use_filters: false`` (default)
            Write-optimised mode. Only Zstd(1) is applied to every column —
            no ByteShuffle, BitWidthReduction, or DoubleDelta pre-processors.
            Gives ~3–4× faster writes with a modest increase in storage size
            (~15–20 % larger than the full pipeline).  DoubleDelta is kept on
            the time dimension and ``timestamp_ns`` attribute because it is
            essentially free and reduces those columns by ~80 %.

        ``use_filters: true``
            Full compression pipeline: ByteShuffle+Zstd for floats,
            BitWidthReduction+Zstd for narrow ints, DoubleDelta+Zstd for
            time/timestamps.  Best for archival storage where read throughput
            and storage cost matter more than ingest speed.
        """
        self.cfg = cfg or {}
        self.use_filters = bool(self.cfg.get("use_filters", False))

    # ------------------------------------------------------------------ #
    # Dimension filters
    # ------------------------------------------------------------------ #

    def _fast_zstd(self, level_key: str, full_default: int) -> tiledb.FilterList:
        """Return a single Zstd filter, respecting the configured level."""
        lvl = int(self.cfg.get(level_key, 1 if not self.use_filters else full_default))
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def spatial_dim_filters(self, scale_factor: float = 1e-6) -> tiledb.FilterList:
        """
        Filters for latitude / longitude dimensions.

        Fast mode : Zstd(1)
        Full mode : Zstd(spatial_zstd_level, default 3)
        """
        return self._fast_zstd("spatial_zstd_level", 3)

    def time_dim_filters(self) -> tiledb.FilterList:
        """
        Filters for the time dimension (int64 days-since-epoch).

        DoubleDelta is kept in both fast and full modes because it is
        essentially free CPU-wise and reduces quasi-monotonic day-since-epoch
        values dramatically before Zstd.

        Fast mode : DoubleDelta → Zstd(1)
        Full mode : DoubleDelta → Zstd(time_zstd_level, default 3)
        """
        lvl = int(self.cfg.get("time_zstd_level", 1 if not self.use_filters else 3))
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )

    # ------------------------------------------------------------------ #
    # Attribute filters (dtype-based)
    # ------------------------------------------------------------------ #

    def filters_for_dtype(self, dtype: Any) -> tiledb.FilterList:
        """
        Return a FilterList appropriate for the given dtype.

        Fast mode (``use_filters: false``, default)
        -------------------------------------------
        Zstd(1) for every dtype — no pre-processors.  ~3–4× faster writes
        than full mode; ~15–20 % larger files.

        Full mode (``use_filters: true``)
        ----------------------------------
        float32 / float64
            ByteShuffle + Zstd(float_zstd_level, default 4).
        int8 / int16 / int32 / uint8 / uint16 / uint32
            BitWidthReduction + Zstd(int_zstd_level, default 3).
        int64 / uint64 / strings
            Zstd(default_zstd_level, default 3).
        """
        dt = np.dtype(dtype)

        if not self.use_filters:
            lvl = int(self.cfg.get("default_zstd_level", 1))
            return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

        if dt.kind == "f":
            lvl = int(self.cfg.get("float_zstd_level", 4))
            return tiledb.FilterList(
                [
                    tiledb.ByteShuffleFilter(),
                    tiledb.ZstdFilter(level=lvl),
                ]
            )

        if dt.kind in ("i", "u") and dt.itemsize <= 4:
            lvl = int(self.cfg.get("int_zstd_level", 3))
            return tiledb.FilterList(
                [
                    tiledb.BitWidthReductionFilter(),
                    tiledb.ZstdFilter(level=lvl),
                ]
            )

        # Wide integers (int64/uint64) and strings
        lvl = int(self.cfg.get("default_zstd_level", 3))
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def timestamp_filters(self) -> tiledb.FilterList:
        """
        Filters for the ``timestamp_ns`` attribute (int64 nanoseconds since epoch).

        DoubleDelta is kept in both fast and full modes — same reasoning as
        ``time_dim_filters``: quasi-monotonic int64 values compress to near-zero
        second differences essentially for free.

        Fast mode : DoubleDelta → Zstd(1)
        Full mode : DoubleDelta → Zstd(timestamp_zstd_level, default 2)
        """
        lvl = int(
            self.cfg.get("timestamp_zstd_level", 1 if not self.use_filters else 2)
        )
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )
