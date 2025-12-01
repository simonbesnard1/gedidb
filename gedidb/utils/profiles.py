# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple, Optional, Union

import numpy as np
from numba import njit


# ============================================================
# Low-level helpers (all Numba-jitted, no parallel=True)
# ============================================================


@njit(cache=True, fastmath=True)
def _derivative_variable_dz(y2d: np.ndarray, z2d: np.ndarray) -> np.ndarray:
    """
    Compute ∂y/∂z row-wise when z is per-row (variable, 2-D).
    Central difference where possible; fallback to forward/backward.
    NaNs propagate.
    """
    n, m = y2d.shape
    out = np.empty((n, m), dtype=np.float64)

    for i in range(n):
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


@njit(cache=True, fastmath=True)
def _finite_max_rowwise(h: np.ndarray) -> np.ndarray:
    """
    Rowwise max that returns NaN for all-NaN rows.
    """
    n, m = h.shape
    H = np.empty(n, dtype=np.float64)

    for i in range(n):
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


@njit(cache=True, fastmath=True)
def _pavd_from_pai_variable_dz(pai: np.ndarray, z: np.ndarray) -> np.ndarray:
    """
    PAVD = -d(PAI)/dz with per-row non-uniform z (both (n, m)).
    """
    n, m = pai.shape
    out = np.empty((n, m), dtype=np.float64)

    for i in range(n):
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


# ============================================================
# RH-resampling kernel (only PAVD + height + H)
# ============================================================


@njit(cache=True, fastmath=True)
def _resample_pavd_height_to_rh101_jit(
    height_2d: np.ndarray,
    pavd_z: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Resample PAVD and height profiles from z-space to RH=0..100 (101 points).

    Inputs
    ------
    height_2d : (n_shots, nz), height above ground (NaN where invalid)
    pavd_z    : (n_shots, nz), PAVD per meter at each z (NaN where invalid)

    Returns
    -------
    pavd_rh   : (n_shots, 101), PAVD vs RH, converted back to per-meter
    height_rh : (n_shots, 101), height vs RH
    H         : (n_shots,), max canopy height per shot
    """
    n, m = height_2d.shape

    pavd_rh = np.empty((n, 101), dtype=np.float64)
    height_rh = np.empty((n, 101), dtype=np.float64)
    H = _finite_max_rowwise(height_2d)

    # RH axis 0..100
    rh_axis = np.empty(101, dtype=np.float64)
    for k in range(101):
        rh_axis[k] = float(k)

    for i in range(n):
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

        # count finite z (only height, like original)
        count = 0
        for j in range(m):
            if np.isfinite(zi[j]):
                count += 1

        if count == 0:
            continue

        # temp arrays (monotonic in RH)
        r = np.empty(count, dtype=np.float64)
        ypav = np.empty(count, dtype=np.float64)

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
                ypav[q] = pzi[j]  # may be NaN; interpolation handles it
                q += 1
                if q >= count:
                    break

        # sort by RH
        order = np.argsort(r)
        r = r[order]
        ypav = ypav[order]

        # deduplicate r (keep first)
        w = 1
        for j in range(1, r.shape[0]):
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

        # Initialize pgap grid base with pgap_theta baseline fill
        pgap_grid = np.full((n_shots, nz), np.nan, dtype=self.out_dtype)
        for j in range(n_shots):
            pgap_grid[j, :] = pgap_theta[j]

        if start_offset > 0:
            pgap_grid[:, :start_offset] = 1.0

        # Output height grid
        height_ag = np.full((n_shots, nz), np.nan, dtype=self.out_dtype)

        # Fill per shot
        for j in range(n_shots):
            s = rx_start[j]
            c = rx_count[j]
            e = s + c

            # Validate shot
            if c <= 0 or s < 0 or e > flat_pgap.shape[0]:
                continue  # leave shot as NaN

            seg = flat_pgap[s:e]
            n_valid = min(len(seg), c)

            if n_valid > 0:
                pgap_grid[j, start_offset : start_offset + n_valid] = seg[:n_valid]

            # Compute height_above_ground for this shot
            denom = max(c - 1, 1)
            v = (h0[j] - hL[j]) / denom
            for k in range(nz):
                height_ag[j, k] = h0[j] - k * v + start_offset * v

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
        # ---- inputs & numerics ----
        pgap = np.asarray(pgap_theta_z, dtype=np.float64)
        if pgap.ndim != 2:
            raise ValueError("pgap_theta_z must be 2D (n_shots, nz).")

        pgap_clipped = np.clip(pgap, self._eps, 1.0 - self._eps)

        # Height array
        z_in = np.asarray(height, dtype=np.float64)

        # Broadcast μ = |sin(elevation)| to pgap shape
        elev = np.asarray(local_beam_elevation, dtype=np.float64)
        if elev.ndim == 0:
            elev = np.full(pgap.shape[0], elev, dtype=np.float64)
        elif elev.shape[0] != pgap.shape[0]:
            raise ValueError("local_beam_elevation length must match n_shots.")

        mu = np.abs(np.sin(elev))[:, None]
        mu = np.broadcast_to(mu, pgap.shape)

        # Broadcast G and Ω
        G = np.asarray(rossg, dtype=np.float64)
        O = np.asarray(omega, dtype=np.float64)

        if G.ndim == 0:
            G = np.full(pgap.shape[0], G, dtype=np.float64)
        if O.ndim == 0:
            O = np.full(pgap.shape[0], O, dtype=np.float64)

        if G.shape[0] != pgap.shape[0] or O.shape[0] != pgap.shape[0]:
            raise ValueError("rossg and omega must have length n_shots or be scalar.")

        G = np.broadcast_to(G[:, None], pgap.shape)
        O = np.broadcast_to(O[:, None], pgap.shape)

        denom = G * O
        if not np.all(np.isfinite(denom)) or np.any(denom <= 0):
            raise ValueError("`rossg * omega` must be positive and finite everywhere.")

        # ---- PAI in z-domain ----
        pai_z = (-np.log(pgap_clipped) * mu / denom).astype(np.float64, copy=False)

        # ---- PAVD = -d(PAI)/dz ----
        if z_in.ndim == 1:
            dPAI_dz = np.gradient(
                pai_z, z_in, axis=-1, edge_order=min(self.gradient_edge_order, 2)
            )
            pavd_z = (-dPAI_dz).astype(np.float64, copy=False)
            height_2d = np.broadcast_to(
                z_in[None, :], (pgap.shape[0], z_in.shape[0])
            ).astype(np.float64, copy=False)
        elif z_in.ndim == 2:
            if z_in.shape != pai_z.shape:
                raise ValueError(
                    "height (n_shots, nz) must match pgap/pai shapes (n_shots, nz)."
                )
            pai_c = np.ascontiguousarray(pai_z, dtype=np.float64)
            z_c = np.ascontiguousarray(z_in, dtype=np.float64)
            pavd_z = _pavd_from_pai_variable_dz(pai_c, z_c)
            height_2d = z_c
        else:
            raise ValueError("height must be 1D or 2D.")

        # ---- propagate NaNs from original pgap (pre-clip) ----
        nan_mask = ~np.isfinite(pgap_theta_z)
        if np.any(nan_mask):
            pavd_z = np.where(nan_mask, np.nan, pavd_z)

        # ---- treat GEDI fill values (e.g. -9999) as NaN ----
        # real heights will be in [-100, 100] range; anything << -100 is fill
        fill_mask = height_2d < -1000.0
        if np.any(fill_mask):
            height_2d[fill_mask] = np.nan
            pavd_z[fill_mask] = np.nan

        # ---- mask height < 0 (below-ground) ----
        h = height_2d.astype(np.float64, copy=False)
        mask = ~(h >= 0.0)

        h_masked = h.copy()
        h_masked[mask] = np.nan

        pavd_m = pavd_z.copy()
        pavd_m[mask] = np.nan

        # Ensure contiguous arrays for Numba
        h_masked = np.ascontiguousarray(h_masked, dtype=np.float64)
        pavd_m = np.ascontiguousarray(pavd_m, dtype=np.float64)

        pavd_rh64, height_rh64, H64 = _resample_pavd_height_to_rh101_jit(
            h_masked, pavd_m
        )

        # enforce ground conditions ONLY for valid shots
        for i in range(pavd_rh64.shape[0]):
            Hi = H64[i]
            if np.isfinite(Hi) and Hi > 0:
                pavd_rh64[i, 0] = 0.0
                height_rh64[i, 0] = 0.0
            # else leave NaN (fully invalid profile)

        pavd_rh = pavd_rh64.astype(self.out_dtype, copy=False)
        height_rh = height_rh64.astype(self.out_dtype, copy=False)
        H = H64.astype(self.out_dtype, copy=False)

        return pavd_rh, height_rh, H
