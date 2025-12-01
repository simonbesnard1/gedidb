# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
# Optimized version

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Optional, Union

import numpy as np
from numba import njit, prange


# ============================================================
# Low-level helpers (Numba-jitted with parallel where beneficial)
# ============================================================


@njit(cache=True, fastmath=True, parallel=True)
def _derivative_variable_dz(y2d: np.ndarray, z2d: np.ndarray) -> np.ndarray:
    """
    Compute ∂y/∂z row-wise when z is per-row (variable, 2-D).
    Central difference where possible; fallback to forward/backward.
    NaNs propagate.

    """
    n, m = y2d.shape
    out = np.empty((n, m), dtype=np.float64)

    for i in prange(n):  # PARALLEL
        yi = y2d[i]
        zi = z2d[i]
        oi = out[i]

        for j in range(m):
            yj = yi[j]
            zj = zi[j]

            if not (np.isfinite(yj) and np.isfinite(zj)):
                oi[j] = np.nan
                continue

            # central difference
            if 0 < j < m - 1:
                y_l, z_l = yi[j - 1], zi[j - 1]
                y_r, z_r = yi[j + 1], zi[j + 1]
                if (
                    np.isfinite(y_l)
                    and np.isfinite(z_l)
                    and np.isfinite(y_r)
                    and np.isfinite(z_r)
                    and z_r != z_l
                ):
                    oi[j] = (y_r - y_l) / (z_r - z_l)
                    continue

            # forward difference
            if j < m - 1:
                y_r, z_r = yi[j + 1], zi[j + 1]
                if np.isfinite(y_r) and np.isfinite(z_r) and z_r != zj:
                    oi[j] = (y_r - yj) / (z_r - zj)
                    continue

            # backward difference
            if j > 0:
                y_l, z_l = yi[j - 1], zi[j - 1]
                if np.isfinite(y_l) and np.isfinite(z_l) and zj != z_l:
                    oi[j] = (yj - y_l) / (zj - z_l)
                    continue

            oi[j] = np.nan

    return out


@njit(cache=True, fastmath=True)
def _scatter_waveform_jit(
    start_offset: int, counts: np.ndarray, flat_values: np.ndarray, out_2d: np.ndarray
) -> None:
    """
    In-place scatter from a flat concatenation into out_2d rows
    [start_offset:start_offset+c, j].
    """
    cursor = 0
    n_shots = counts.shape[0]

    for j in range(n_shots):
        c = int(counts[j])
        if c <= 0:
            continue

        if cursor + c > flat_values.shape[0]:
            c = max(0, flat_values.shape[0] - cursor)

        for k in range(c):
            out_2d[start_offset + k, j] = flat_values[cursor + k]

        cursor += c
        if cursor >= flat_values.shape[0]:
            break


@njit(cache=True, fastmath=True, parallel=True)
def _finite_max_rowwise(h: np.ndarray) -> np.ndarray:
    """
    Rowwise max that returns NaN for all-NaN rows.

    """
    n, m = h.shape
    H = np.empty(n, dtype=np.float64)

    for i in prange(n):  # PARALLEL
        has_val = False
        mx = -np.inf
        for j in range(m):
            v = h[i, j]
            if np.isfinite(v):
                has_val = True
                if v > mx:
                    mx = v
        H[i] = mx if has_val else np.nan

    return H


@njit(cache=True, fastmath=True)
def _interp1d_monotonic_edgefill(
    xq: np.ndarray, x: np.ndarray, y: np.ndarray
) -> np.ndarray:
    """
    Simple linear interpolation for strictly increasing x with 'edge' fill.
    """
    nq = xq.shape[0]
    out = np.empty(nq, dtype=np.float64)

    if x.shape[0] == 1:
        v = y[0]
        for i in range(nq):
            out[i] = v
        return out

    for i in range(nq):
        q = xq[i]

        if q <= x[0]:
            out[i] = y[0]
            continue
        if q >= x[-1]:
            out[i] = y[-1]
            continue

        lo = 0
        hi = x.shape[0] - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if x[mid] <= q:
                lo = mid
            else:
                hi = mid

        dx = x[hi] - x[lo]
        if dx == 0.0:
            out[i] = 0.5 * (y[lo] + y[hi])
        else:
            t = (q - x[lo]) / dx
            out[i] = (1.0 - t) * y[lo] + t * y[hi]

    return out


@njit(cache=True, fastmath=True, parallel=True)
def _pavd_from_pai_variable_dz(pai: np.ndarray, z: np.ndarray) -> np.ndarray:
    """
    PAVD = -d(PAI)/dz with per-row non-uniform z (both (n, m)).

    """
    n, m = pai.shape
    out = np.empty((n, m), dtype=np.float64)

    for i in prange(n):  # PARALLEL
        # left edge
        dz0 = z[i, 1] - z[i, 0]
        if dz0 == 0.0 or not (
            np.isfinite(pai[i, 1]) and np.isfinite(pai[i, 0]) and np.isfinite(dz0)
        ):
            out[i, 0] = np.nan
        else:
            out[i, 0] = -(pai[i, 1] - pai[i, 0]) / dz0

        # interior
        for j in range(1, m - 1):
            dzc = z[i, j + 1] - z[i, j - 1]
            if dzc == 0.0 or not (
                np.isfinite(pai[i, j + 1])
                and np.isfinite(pai[i, j - 1])
                and np.isfinite(dzc)
            ):
                out[i, j] = np.nan
            else:
                out[i, j] = -(pai[i, j + 1] - pai[i, j - 1]) / dzc

        # right edge
        dzN = z[i, m - 1] - z[i, m - 2]
        if dzN == 0.0 or not (
            np.isfinite(pai[i, m - 1])
            and np.isfinite(pai[i, m - 2])
            and np.isfinite(dzN)
        ):
            out[i, m - 1] = np.nan
        else:
            out[i, m - 1] = -(pai[i, m - 1] - pai[i, m - 2]) / dzN

    return out


@njit(cache=True, fastmath=True, parallel=True)
def _compute_pai_and_pavd_fused(
    pgap: np.ndarray,
    mu: np.ndarray,
    G: np.ndarray,
    O: np.ndarray,
    z: np.ndarray,
    eps: float,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Fused computation of PAI and PAVD from pgap in a single pass.

    Assumes z is 2D (n_shots, nz).
    """
    n, m = pgap.shape
    pai = np.empty((n, m), dtype=np.float64)
    pavd = np.empty((n, m), dtype=np.float64)

    for i in prange(n):  # PARALLEL
        # Compute PAI for this row
        for j in range(m):
            p = pgap[i, j]

            # Clip
            if p < eps:
                p = eps
            elif p > 1.0 - eps:
                p = 1.0 - eps

            # PAI = -log(pgap) * mu / (G * O)
            denom = G[i, j] * O[i, j]
            pai[i, j] = -np.log(p) * mu[i, j] / denom

        # Compute PAVD = -d(PAI)/dz for this row
        # left edge
        dz0 = z[i, 1] - z[i, 0]
        if dz0 == 0.0 or not (
            np.isfinite(pai[i, 1]) and np.isfinite(pai[i, 0]) and np.isfinite(dz0)
        ):
            pavd[i, 0] = np.nan
        else:
            pavd[i, 0] = -(pai[i, 1] - pai[i, 0]) / dz0

        # interior
        for j in range(1, m - 1):
            dzc = z[i, j + 1] - z[i, j - 1]
            if dzc == 0.0 or not (
                np.isfinite(pai[i, j + 1])
                and np.isfinite(pai[i, j - 1])
                and np.isfinite(dzc)
            ):
                pavd[i, j] = np.nan
            else:
                pavd[i, j] = -(pai[i, j + 1] - pai[i, j - 1]) / dzc

        # right edge
        dzN = z[i, m - 1] - z[i, m - 2]
        if dzN == 0.0 or not (
            np.isfinite(pai[i, m - 1])
            and np.isfinite(pai[i, m - 2])
            and np.isfinite(dzN)
        ):
            pavd[i, m - 1] = np.nan
        else:
            pavd[i, m - 1] = -(pai[i, m - 1] - pai[i, m - 2]) / dzN

    return pai, pavd


@njit(cache=True, fastmath=True, parallel=True)
def _apply_masks_fused(
    pavd: np.ndarray,
    height: np.ndarray,
    nan_mask: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Apply multiple masks in a single fused kernel.

    Combines NaN propagation, fill value handling, and
    below-ground masking in one pass.
    """
    n, m = pavd.shape
    pavd_out = np.empty((n, m), dtype=np.float64)
    height_out = np.empty((n, m), dtype=np.float64)

    for i in prange(n):  # PARALLEL
        for j in range(m):
            h = height[i, j]
            p = pavd[i, j]

            # Check masks
            is_nan_input = nan_mask[i, j]
            is_fill = h < -1000.0
            is_below_ground = h < 0.0

            if is_nan_input or is_fill or is_below_ground:
                pavd_out[i, j] = np.nan
                height_out[i, j] = np.nan if is_fill else h
            else:
                pavd_out[i, j] = p
                height_out[i, j] = h

    return pavd_out, height_out


# ============================================================
# RH-resampling kernel (OPTIMIZED with parallel)
# ============================================================


@njit(cache=True, fastmath=True, parallel=True)
def _resample_pavd_height_to_rh101_jit(
    height_2d: np.ndarray,
    pavd_z: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Resample PAVD and height profiles from z-space to RH=0..100 (101 points).

    Each shot is processed independently.
    """
    n, m = height_2d.shape

    pavd_rh = np.empty((n, 101), dtype=np.float64)
    height_rh = np.empty((n, 101), dtype=np.float64)

    # Compute H first (parallel)
    H = np.empty(n, dtype=np.float64)
    for i in prange(n):
        has_val = False
        mx = -np.inf
        for j in range(m):
            v = height_2d[i, j]
            if np.isfinite(v):
                has_val = True
                if v > mx:
                    mx = v
        H[i] = mx if has_val else np.nan

    # RH axis 0..100 (shared across all shots)
    rh_axis = np.empty(101, dtype=np.float64)
    for k in range(101):
        rh_axis[k] = float(k)

    # Process each shot in parallel
    for i in prange(n):  # PARALLEL
        Hi = H[i]

        # init row with NaNs
        for k in range(101):
            pavd_rh[i, k] = np.nan
            height_rh[i, k] = np.nan

        # no valid canopy → leave as NaN
        if not (np.isfinite(Hi) and Hi > 0.0):
            continue

        zi = height_2d[i, :]
        pzi = pavd_z[i, :]

        # count finite z
        count = 0
        for j in range(m):
            if np.isfinite(zi[j]):
                count += 1

        if count == 0:
            continue

        # temp arrays (monotonic in RH)
        # Pre-allocate to max possible size
        r = np.empty(m, dtype=np.float64)
        ypav = np.empty(m, dtype=np.float64)

        q = 0
        invH = 100.0 / Hi

        for j in range(m):
            zval = zi[j]
            if np.isfinite(zval):
                rr = zval * invH
                if rr < 0.0:
                    rr = 0.0
                elif rr > 100.0:
                    rr = 100.0

                r[q] = rr
                ypav[q] = pzi[j]
                q += 1

        # Trim to actual size
        r = r[:q]
        ypav = ypav[:q]

        # sort by RH (using simple insertion sort for small arrays)
        # More cache-friendly than argsort for small n
        for j in range(1, q):
            key_r = r[j]
            key_y = ypav[j]
            k = j - 1
            while k >= 0 and r[k] > key_r:
                r[k + 1] = r[k]
                ypav[k + 1] = ypav[k]
                k -= 1
            r[k + 1] = key_r
            ypav[k + 1] = key_y

        # deduplicate r (keep first)
        w = 1
        for j in range(1, q):
            if r[j] != r[w - 1]:
                r[w] = r[j]
                ypav[w] = ypav[j]
                w += 1

        r = r[:w]
        ypav = ypav[:w]

        # interpolate to 0..100 RH
        pav_r = _interp1d_monotonic_edgefill(rh_axis, r, ypav)

        # convert pavd to per-meter via H/100
        scale = Hi / 100.0
        for k in range(101):
            pavd_rh[i, k] = pav_r[k] * scale
            height_rh[i, k] = Hi * (rh_axis[k] / 100.0)

        # enforce ground conditions
        pavd_rh[i, 0] = 0.0
        height_rh[i, 0] = 0.0

    return pavd_rh, height_rh, H


@njit(cache=True, fastmath=True, parallel=True)
def _fill_pgap_grid(
    pgap_grid: np.ndarray,
    pgap_theta: np.ndarray,
    rx_start: np.ndarray,
    rx_count: np.ndarray,
    flat_pgap: np.ndarray,
    start_offset: int,
) -> None:
    """
    Fill pgap_grid efficiently with Numba parallelization.

    """
    n_shots = pgap_grid.shape[0]
    nz = pgap_grid.shape[1]

    for j in prange(n_shots):  # PARALLEL
        # Fill baseline
        baseline = pgap_theta[j]
        for k in range(nz):
            pgap_grid[j, k] = baseline

        # Fill start_offset with 1.0
        if start_offset > 0:
            for k in range(min(start_offset, nz)):
                pgap_grid[j, k] = 1.0

        # Fill actual data
        s = rx_start[j]
        c = rx_count[j]
        e = s + c

        if c <= 0 or s < 0 or e > flat_pgap.shape[0]:
            continue  # leave shot as baseline/NaN

        n_valid = min(c, e - s, nz - start_offset)

        for k in range(n_valid):
            if s + k < flat_pgap.shape[0]:
                pgap_grid[j, start_offset + k] = flat_pgap[s + k]


@njit(cache=True, fastmath=True, parallel=True)
def _compute_height_grid(
    height_ag: np.ndarray,
    h0: np.ndarray,
    hL: np.ndarray,
    rx_count: np.ndarray,
    start_offset: int,
) -> None:
    """
    Compute height_above_ground grid efficiently.

    """
    n_shots = height_ag.shape[0]
    nz = height_ag.shape[1]

    for j in prange(n_shots):  # PARALLEL
        c = rx_count[j]
        if c <= 0:
            continue

        denom = max(c - 1, 1)
        v = (h0[j] - hL[j]) / denom

        for k in range(nz):
            height_ag[j, k] = h0[j] - k * v + start_offset * v


# ============================================================
# High-level profiler class
# ============================================================


@dataclass(slots=True)
class GEDIVerticalProfiler:
    """
    High-level interface to build GEDI-style RH profiles from pgap_theta_z.

    Methods:
      - read_pgap_theta_z(beam_obj, ...) → (pgap_theta_z, height)
      - compute_profiles(...) → cp_pavd, cp_height, H
    """

    out_dtype: np.dtype = np.float32
    gradient_edge_order: int = 2
    _eps: float = np.finfo(np.float64).eps

    def read_pgap_theta_z(
        self,
        beam_obj,
        start: int = 0,
        finish: Optional[int] = None,
        minlength: Optional[int] = None,
        start_offset: int = 0,
    ):
        """
        Robust pgap_theta_z reader that tolerates corrupted GEDI granules.
        Invalid shots are skipped (set to NaN) instead of raising errors.
        Output shapes: (n_shots, nz)

        """

        # Load SDS
        rx_start = np.asarray(beam_obj["rx_sample_start_index"][()], dtype=np.int64)
        rx_count = np.asarray(beam_obj["rx_sample_count"][()], dtype=np.int64)
        flat_pgap = np.asarray(beam_obj["pgap_theta_z"][()], dtype=self.out_dtype)

        h0 = np.asarray(beam_obj["geolocation/height_bin0"][()], dtype=np.float64)
        hL = np.asarray(beam_obj["geolocation/height_lastbin"][()], dtype=np.float64)
        pgap_theta = np.asarray(beam_obj["pgap_theta"][()], dtype=self.out_dtype)

        n_total = rx_start.shape[0]

        if finish is None:
            finish = n_total

        # Validate range
        if not (0 <= start < finish <= n_total):
            raise ValueError("Invalid start/finish in read_pgap_theta_z()")

        # Slice per user selection
        rx_start = rx_start[start:finish]
        rx_count = rx_count[start:finish]
        h0 = h0[start:finish]
        hL = hL[start:finish]
        pgap_theta = pgap_theta[start:finish]

        n_shots = len(rx_start)
        if n_shots == 0:
            return (
                np.zeros((0, 0), dtype=self.out_dtype),
                np.zeros((0, 0), dtype=self.out_dtype),
            )

        # Determine max bins across shots
        max_len = int(rx_count.max() + start_offset)
        if minlength is not None:
            max_len = max(max_len, int(minlength))

        nz = max_len

        # Initialize grids with NaN
        pgap_grid = np.full((n_shots, nz), np.nan, dtype=self.out_dtype)
        height_ag = np.full((n_shots, nz), np.nan, dtype=self.out_dtype)

        _fill_pgap_grid(
            pgap_grid,
            pgap_theta.astype(np.float32),
            rx_start,
            rx_count,
            flat_pgap,
            start_offset,
        )

        _compute_height_grid(
            height_ag,
            h0,
            hL,
            rx_count,
            start_offset,
        )

        return pgap_grid, height_ag

    # ----------------------------
    # z-space -> RH-space pipeline
    # ----------------------------
    def compute_profiles(
        self,
        pgap_theta_z: np.ndarray,  # (n_shots, nz)
        height: np.ndarray,  # (nz,) or (n_shots, nz)
        local_beam_elevation: Union[np.ndarray, float],
        rossg: Union[np.ndarray, float],
        omega: Union[np.ndarray, float],
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Main high-level profile computation.

        Returns
        -------
        pavd_rh   : (n_shots, 101)  # cp_pavd
        height_rh : (n_shots, 101)  # cp_height
        H         : (n_shots,)      # canopy height per shot
        """
        # ---- inputs & validation ----
        pgap = np.asarray(pgap_theta_z, dtype=np.float64)
        if pgap.ndim != 2:
            raise ValueError("pgap_theta_z must be 2D (n_shots, nz).")

        # Height array
        z_in = np.asarray(height, dtype=np.float64)

        # Broadcast μ = |sin(elevation)| to pgap shape
        elev = np.asarray(local_beam_elevation, dtype=np.float64)
        if elev.ndim == 0:
            elev = np.full(pgap.shape[0], elev, dtype=np.float64)
        elif elev.shape[0] != pgap.shape[0]:
            raise ValueError("local_beam_elevation length must match n_shots.")

        mu = np.abs(np.sin(elev))[:, None]
        mu = np.broadcast_to(mu, pgap.shape).copy()  # ensure contiguous

        # Broadcast G and Ω
        G = np.asarray(rossg, dtype=np.float64)
        O = np.asarray(omega, dtype=np.float64)

        if G.ndim == 0:
            G = np.full(pgap.shape[0], G, dtype=np.float64)
        if O.ndim == 0:
            O = np.full(pgap.shape[0], O, dtype=np.float64)

        if G.shape[0] != pgap.shape[0] or O.shape[0] != pgap.shape[0]:
            raise ValueError("rossg and omega must have length n_shots or be scalar.")

        G = np.broadcast_to(G[:, None], pgap.shape).copy()
        O = np.broadcast_to(O[:, None], pgap.shape).copy()

        # Validate denominator
        denom = G * O
        if not np.all(np.isfinite(denom)) or np.any(denom <= 0):
            raise ValueError("`rossg * omega` must be positive and finite everywhere.")

        # ---- Handle height dimensions ----
        if z_in.ndim == 1:
            height_2d = np.broadcast_to(
                z_in[None, :], (pgap.shape[0], z_in.shape[0])
            ).copy()
        elif z_in.ndim == 2:
            if z_in.shape != pgap.shape:
                raise ValueError(
                    "height (n_shots, nz) must match pgap shapes (n_shots, nz)."
                )
            height_2d = np.ascontiguousarray(z_in, dtype=np.float64)
        else:
            raise ValueError("height must be 1D or 2D.")

        # Fused PAI + PAVD computation ----
        pai_z, pavd_z = _compute_pai_and_pavd_fused(
            np.ascontiguousarray(pgap, dtype=np.float64),
            mu,
            G,
            O,
            height_2d,
            self._eps,
        )

        # Fused masking ----
        nan_mask = ~np.isfinite(pgap_theta_z)
        pavd_masked, height_masked = _apply_masks_fused(
            pavd_z,
            height_2d,
            nan_mask,
        )

        # ---- Resample to RH space ----
        pavd_rh64, height_rh64, H64 = _resample_pavd_height_to_rh101_jit(
            height_masked, pavd_masked
        )

        # Convert to output dtype
        pavd_rh = pavd_rh64.astype(self.out_dtype, copy=False)
        height_rh = height_rh64.astype(self.out_dtype, copy=False)
        H = H64.astype(self.out_dtype, copy=False)

        return pavd_rh, height_rh, H
