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
        """
        self.cfg = cfg or {}

    # ------------------------------------------------------------------ #
    # Dimension filters
    # ------------------------------------------------------------------ #

    def spatial_dim_filters(self, scale_factor: float = 1e-6) -> tiledb.FilterList:
        """
        Filters for latitude / longitude dimensions.

        Chain: Zstd
        """
        lvl = int(self.cfg.get("spatial_zstd_level", 3))
        return tiledb.FilterList([tiledb.ZstdFilter(level=lvl)])

    def time_dim_filters(self) -> tiledb.FilterList:
        """
        Filters for the time dimension (int64 days-since-epoch).

        GEDI granules are ingested roughly in chronological order, so time
        values within a fragment are quasi-monotonic. DoubleDelta reduces
        the stream to near-zero second differences before Zstd.
        BitWidthReduction is omitted: day-since-epoch values (~19 000 for
        2019+) need at least 15 bits, so headroom for bit reduction is minimal.

        Chain: DoubleDelta → Zstd
        """
        lvl = int(self.cfg.get("time_zstd_level", 3))
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

        Dispatch table
        --------------
        float32 / float64
            ByteShuffle + Zstd.
            ByteShuffle separates exponent and mantissa bytes so Zstd sees
            much better byte-level repetition — the same strategy validated
            by HDF5/NetCDF for geophysical floating-point data.

        int8 / int16 / int32 / uint8 / uint16 / uint32  (narrow integers)
            BitWidthReduction + Zstd.
            These are likely narrow-range values (quality flags, beam IDs,
            shot counts) where BitWidthReduction genuinely reclaims unused
            high bits before Zstd finalises the compression.
            Note: RLE is dropped — it only helps with long identical runs,
            which GEDI flag fields don't reliably produce.

        int64 / uint64  (wide integers)
            Zstd only.
            BitWidthReduction on 64-bit values with large absolute magnitudes
            (e.g. shot numbers ~10¹⁸) is a no-op. Zstd alone is correct here.

        unicode strings
            Zstd only. No useful byte-level pre-processor exists for
            variable-length UTF-8 strings.

        fallback
            Zstd(level=3).
        """
        dt = np.dtype(dtype)

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

        timestamp_ns values are large (~1.7 × 10¹⁸) and quasi-monotonic within
        a fragment. DoubleDelta reduces the stream to near-zero second differences
        before Zstd. BitWidthReduction (the old choice) cannot reduce 64-bit
        values with large absolute magnitudes and is replaced here.

        Chain: DoubleDelta → Zstd
        """
        lvl = int(self.cfg.get("timestamp_zstd_level", 2))
        return tiledb.FilterList(
            [
                tiledb.DoubleDeltaFilter(),
                tiledb.ZstdFilter(level=lvl),
            ]
        )
