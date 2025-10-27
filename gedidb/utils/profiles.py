# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences

from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Optional
import numpy as np
from numba import njit, prange

# ----------------------------
# Numba helpers (JIT hot loops)
# ----------------------------


@njit(cache=True, fastmath=True, parallel=True)
def _derivative_variable_dz(y2d: np.ndarray, z2d: np.ndarray) -> np.ndarray:
    """
    Compute ∂y/∂z row-wise when z is per-row (variable, 2-D).
    Central difference where possible; fallback to forward/backward.
    NaNs propagate.
    """
    n, m = y2d.shape
    out = np.empty((n, m), dtype=np.float64)

    for i in prange(n):
        yi = y2d[i]
        zi = z2d[i]
        oi = out[i]

        for j in range(m):
            yj = yi[j]
            zj = zi[j]
            if not (np.isfinite(yj) and np.isfinite(zj)):
                oi[j] = np.nan
                continue

            # try central
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

            # forward
            if j < m - 1:
                y_r, z_r = yi[j + 1], zi[j + 1]
                if np.isfinite(y_r) and np.isfinite(z_r) and z_r != zj:
                    oi[j] = (y_r - yj) / (z_r - zj)
                    continue

            # backward
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
    In-place scatter from a flat concatenation into out_2d rows [start_offset:start_offset+c, j].
    Assumes flat_values is ordered by shots (segments concatenated in the same order as columns).
    """
    cursor = 0
    n_shots = counts.shape[0]
    for j in range(n_shots):
        c = int(counts[j])
        if c <= 0:
            continue
        seg = flat_values[cursor : cursor + c]
        # write
        for k in range(c):
            out_2d[start_offset + k, j] = seg[k]
        cursor += c


@njit(cache=True, fastmath=True)
def _finite_max_rowwise(h: np.ndarray) -> np.ndarray:
    """
    Rowwise max that returns NaN for all-NaN rows (np.nanmax would warn/propagate).
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
    Simple linear interpolation for increasing x with 'edge' fill (clamp to endpoints).
    Preconditions: len(x) == len(y) >= 1, x strictly increasing (deduped).
    """
    nq = xq.shape[0]
    out = np.empty(nq, dtype=np.float64)

    if x.shape[0] == 1:
        # constant fill with that single value
        v = y[0]
        for i in range(nq):
            out[i] = v
        return out

    # two-pointer search (binary search could be faster; linear is fine for 101 queries)
    for i in range(nq):
        q = xq[i]
        if q <= x[0]:
            out[i] = y[0]
            continue
        if q >= x[-1]:
            out[i] = y[-1]
            continue
        # locate interval [lo, hi]
        lo = 0
        hi = x.shape[0] - 1
        # binary search
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if x[mid] <= q:
                lo = mid
            else:
                hi = mid
        # linear interpolate
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
    Central differences for interior, forward/backward at edges.
    """
    n, m = pai.shape
    out = np.empty((n, m), dtype=np.float64)
    for i in prange(n):
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
def _resample_profiles_to_rh101_per_shot_jit(
    height_2d: np.ndarray,
    cover_z: np.ndarray,
    pai_z: np.ndarray,
    pavd_z: np.ndarray,
    waveform_z: np.ndarray,
    out_dtype_code: int,
) -> tuple:
    """
    Numba-parallel per-shot resampling to RH=0..100 (101 points).
    Returns (cov_rh, pai_rh, pavd_rh, height_rh, waveform_rh, H)

    Notes:
      - 'waveform_z' is assumed per-height (1/m), already in z-domain.
      - Waveform is normalized per shot so that ∫ w(z) dz = 1 (GEDI L1B-style).
        Integration is approximated on the RH grid using dz ≈ H/100.
      - If the integral is 0 or not finite, the shot's waveform is returned as NaN.
      - PAVD is converted from per-RH to per-meter via (Hi/100).
      - No minimum canopy-height gating is applied; only non-finite H is skipped.
      - Outputs are float64 internally; caller casts to desired dtype.
    """
    n, m = height_2d.shape

    cov_rh = np.empty((n, 101), dtype=np.float64)
    pai_rh = np.empty((n, 101), dtype=np.float64)
    pavd_rh = np.empty((n, 101), dtype=np.float64)
    height_rh = np.empty((n, 101), dtype=np.float64)
    waveform_rh = np.empty((n, 101), dtype=np.float64)
    H = np.empty(n, dtype=np.float64)

    # compute H (or use provided rh98)
    H = _finite_max_rowwise(height_2d)

    # RH axis 0..100
    rh_axis = np.empty(101, dtype=np.float64)
    for k in range(101):
        rh_axis[k] = float(k)

    for i in prange(n):
        Hi = H[i]

        # init row with NaNs
        for k in range(101):
            cov_rh[i, k] = np.nan
            pai_rh[i, k] = np.nan
            pavd_rh[i, k] = np.nan
            height_rh[i, k] = np.nan
            waveform_rh[i, k] = np.nan

        # require finite canopy height to define RH->z mapping
        if not np.isfinite(Hi):
            continue

        zi = height_2d[i, :]
        czi = cover_z[i, :]
        paii = pai_z[i, :]
        pzi = pavd_z[i, :]
        wzi = waveform_z[i, :]

        # count finite z
        count = 0
        for j in range(m):
            if np.isfinite(zi[j]):
                count += 1
        if count == 0:
            continue

        # temp arrays (monotonic in RH)
        r = np.empty(count, dtype=np.float64)
        yc = np.empty(count, dtype=np.float64)
        ypai = np.empty(count, dtype=np.float64)
        ypav = np.empty(count, dtype=np.float64)
        ywav = np.empty(count, dtype=np.float64)

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
                yc[q] = czi[j]
                ypai[q] = paii[j]
                ypav[q] = pzi[j]
                ywav[q] = wzi[j]  # per-meter waveform
                q += 1

        # sort by r
        order = np.argsort(r)
        r = r[order]
        yc = yc[order]
        ypai = ypai[order]
        ypav = ypav[order]
        ywav = ywav[order]

        # deduplicate r (keep-first)
        w = 1
        for j in range(1, r.shape[0]):
            if r[j] != r[w - 1]:
                r[w] = r[j]
                yc[w] = yc[j]
                ypai[w] = ypai[j]
                ypav[w] = ypav[j]
                ywav[w] = ywav[j]
                w += 1
        r = r[:w]
        yc = yc[:w]
        ypai = ypai[:w]
        ypav = ypav[:w]
        ywav = ywav[:w]

        # interpolate to integer RH grid
        cov_r = _interp1d_monotonic_edgefill(rh_axis, r, yc)
        pai_r = _interp1d_monotonic_edgefill(rh_axis, r, ypai)
        pav_r = _interp1d_monotonic_edgefill(rh_axis, r, ypav)
        wav_r = _interp1d_monotonic_edgefill(rh_axis, r, ywav)  # still per-meter

        # fill outputs
        # pavd: convert to per-meter using dz/dr = H/100
        # waveform: no rescale (already per-meter)
        for k in range(101):
            cov_rh[i, k] = cov_r[k]
            pai_rh[i, k] = pai_r[k]
            pavd_rh[i, k] = pav_r[k] * (Hi / 100.0)
            height_rh[i, k] = Hi * (rh_axis[k] / 100.0)

        # --- L1B-style normalization of waveform to unit area ---
        pos_sum = 0.0
        for k in range(101):
            wk = wav_r[k]
            if np.isfinite(wk) and wk > 0.0:
                pos_sum += wk
            else:
                # clamp negatives / NaNs to zero for normalization
                wav_r[k] = 0.0

        # area ≈ sum_k w(z_k) * dz, with dz ≈ Hi/100 per RH bin
        area = pos_sum * (Hi / 100.0)

        if np.isfinite(area) and area > 0.0:
            inv_area = 1.0 / area
            for k in range(101):
                # wav_r is now ≥ 0.0 and finite
                waveform_rh[i, k] = wav_r[k] * inv_area  # unitless, ∫=1
        else:
            # degenerate shot → all NaN (as requested)
            for k in range(101):
                waveform_rh[i, k] = np.nan

    return cov_rh, pai_rh, pavd_rh, height_rh, waveform_rh, H


@dataclass(slots=True)
class GEDIVerticalProfiler:
    out_dtype: np.dtype = np.float32
    gradient_edge_order: int = 2  # kept for API; grad is now JIT special-cased
    _eps: float = np.finfo(np.float64).eps

    # ----------------------------
    # I/O -> 2D expansion
    # ----------------------------
    def read_pgap_theta_z(
        self,
        beam_obj,
        start: int = 0,
        finish: Optional[int] = None,
        minlength: Optional[int] = None,
        start_offset: int = 0,
    ) -> Tuple[np.ndarray, np.ndarray]:
        rx_start_idx = np.asarray(beam_obj["rx_sample_start_index"][()], dtype=np.int64)
        rx_count = np.asarray(beam_obj["rx_sample_count"][()], dtype=np.int64)
        n_total = rx_start_idx.shape[0]

        if finish is None:
            finish = n_total
        if not (0 <= start < finish <= n_total):
            raise ValueError("Invalid start/finish slice.")

        start_indices = rx_start_idx[start:finish] - 1  # 0-based
        counts = rx_count[start:finish]
        n_shots = counts.shape[0]

        # Flat concatenation from first shot start to last shot end
        first = int(start_indices[0])
        last_end = int(start_indices[-1] + counts[-1])
        flat_pgap_prof = np.asarray(
            beam_obj["pgap_theta_z"][first:last_end], dtype=self.out_dtype
        )

        max_count = int(np.max(counts) + start_offset)
        if minlength is not None:
            max_count = max(int(minlength), max_count)

        # Base fill with per-shot pgap_theta; pad head with ones if requested
        pgap_theta = np.asarray(
            beam_obj["pgap_theta"][start:finish], dtype=self.out_dtype
        )  # (N,)
        out_shape = (max_count, n_shots)  # (M, N) -> transpose at end
        out_pgap_profile = np.broadcast_to(pgap_theta, out_shape).copy()
        if start_offset > 0:
            out_pgap_profile[:start_offset, :] = 1.0

        # JIT scatter (uses counts ordering; local_start_indices not required)
        _scatter_waveform_jit(
            start_offset,
            counts.astype(np.int64),
            flat_pgap_prof.astype(np.float32, copy=False),
            out_pgap_profile,  # already float32, no `.view`
        )

        # Height-above-ground per bin (M, N)
        h0 = np.asarray(
            beam_obj["geolocation/height_bin0"][start:finish], dtype=np.float64
        )  # top
        hL = np.asarray(
            beam_obj["geolocation/height_lastbin"][start:finish], dtype=np.float64
        )  # bottom
        denom = np.maximum(counts - 1, 1)  # guard c==1
        v = (h0 - hL) / denom  # per-shot spacing

        bin_idx = np.arange(max_count, dtype=np.float64)[:, None]  # (M,1)
        out_height = (
            h0[None, :] - bin_idx * v[None, :] + start_offset * v[None, :]
        ).astype(self.out_dtype, copy=False)

        return out_pgap_profile.T, out_height.T  # (n_shots, nz)

    # ----------------------------
    # z-space -> RH-space pipeline
    # ----------------------------
    def compute_profiles(
        self,
        pgap_theta_z: np.ndarray,  # (n_shots, nz) or (..., nz)
        height: np.ndarray,  # (nz,) or (n_shots, nz) height above ground (m)
        local_beam_elevation: np.ndarray | float,
        rossg: np.ndarray | float,
        omega: np.ndarray | float,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:

        # ---- inputs & numerics ----
        pgap = np.asarray(pgap_theta_z, dtype=np.float64)
        z_in = np.asarray(height, dtype=np.float64)

        # Clip pgap for numeric stability
        pgap = np.clip(pgap, self._eps, 1.0 - self._eps)

        # Broadcast μ = |sin(elevation)| to pgap shape
        elev = np.asarray(local_beam_elevation, dtype=np.float64)
        while elev.ndim < pgap.ndim - 1:
            elev = elev[None, ...]
        mu = np.abs(np.sin(elev))
        mu = np.broadcast_to(mu[..., None], pgap.shape)

        # Broadcast G and Ω to pgap shape
        G = np.asarray(rossg, dtype=np.float64)
        O = np.asarray(omega, dtype=np.float64)
        if G.ndim == 0:
            G = G[None]
        if O.ndim == 0:
            O = O[None]
        while G.ndim < pgap.ndim - 1:
            G = G[None, ...]
        while O.ndim < pgap.ndim - 1:
            O = O[None, ...]
        G = np.broadcast_to(G[..., None], pgap.shape)
        O = np.broadcast_to(O[..., None], pgap.shape)

        denom = G * O
        if not np.all(np.isfinite(denom)) or np.any(denom <= 0):
            raise ValueError("`rossg * omega` must be positive and finite everywhere.")

        # ---- z-domain cover & PAI ----
        cover_z = (mu * (1.0 - pgap)).astype(np.float64, copy=False)
        pai_z = (-np.log(pgap) * mu / denom).astype(np.float64, copy=False)

        # ---- PAVD = - d(PAI)/dz (JIT for per-shot z grids) ----
        if z_in.ndim == 1:
            # global 1D height axis
            dPAI_dz = np.gradient(
                pai_z, z_in, axis=-1, edge_order=min(self.gradient_edge_order, 2)
            )
            pavd_z = (-dPAI_dz).astype(np.float64, copy=False)
            height_2d = np.broadcast_to(z_in, (pgap.shape[0], z_in.shape[0])).astype(
                np.float64, copy=False
            )
        elif z_in.ndim == 2:
            if z_in.shape != pai_z.shape:
                raise ValueError(
                    "height (n_shots, nz) must match pgap/pai shapes (n_shots, nz)."
                )
            pavd_z = _pavd_from_pai_variable_dz(pai_z, z_in)
            height_2d = z_in
        else:
            raise ValueError("height must be 1D or 2D.")

        # ---- propagate NaNs from original pgap (pre-clip) ----
        nan_mask = ~np.isfinite(pgap_theta_z)
        if np.any(nan_mask):
            cover_z = np.where(nan_mask, np.nan, cover_z)
            pai_z = np.where(nan_mask, np.nan, pai_z)
            pavd_z = np.where(nan_mask, np.nan, pavd_z)

        # ---- construct GEDI-like waveform BEFORE masking ----
        dpgap_dz = _derivative_variable_dz(pgap, z_in)
        waveform_z = (dpgap_dz * mu / denom).astype(np.float64, copy=False)
        waveform_z = np.where(waveform_z > 0.0, waveform_z, 0.0)

        # ---- 1) mask away height < 0 ----
        h = height_2d.astype(np.float64, copy=False)
        mask = ~(h >= 0.0)
        # masked copies (float64 to keep JIT paths happy)
        h_masked = h.copy()
        h_masked[mask] = np.nan
        waveform_m = waveform_z.copy()
        waveform_m[mask] = np.nan
        cover_m = cover_z.copy()
        cover_m[mask] = np.nan
        pai_m = pai_z.copy()
        pai_m[mask] = np.nan
        pavd_m = pavd_z.copy()
        pavd_m[mask] = np.nan

        # ---- 2) resample to 101-point RH grid (vegetation-only) ----
        # map dtype to tiny code for JIT
        out_dtype_code = 0 if self.out_dtype == np.float32 else 1

        cov_rh64, pai_rh64, pavd_rh64, height_rh64, waveform_rh64, H64 = (
            _resample_profiles_to_rh101_per_shot_jit(
                h_masked,
                cover_m,
                pai_m,
                pavd_m,
                waveform_m,
                out_dtype_code=out_dtype_code,
            )
        )

        # At ground contact: no cover, PAI=0, no vertical density, waveform=0.
        nshots = cov_rh64.shape[0]
        if nshots > 0:
            cov_rh64[:, 0] = 0.0
            pai_rh64[:, 0] = 0.0
            pavd_rh64[:, 0] = 0.0
            waveform_rh64[:, 0] = 0.0
            height_rh64[:, 0] = 0.0

        # final cast once
        cov_rh = cov_rh64.astype(self.out_dtype, copy=False)
        pai_rh = pai_rh64.astype(self.out_dtype, copy=False)
        pavd_rh = pavd_rh64.astype(self.out_dtype, copy=False)
        height_rh = height_rh64.astype(self.out_dtype, copy=False)
        waveform_rh = waveform_rh64.astype(self.out_dtype, copy=False)
        H = H64.astype(self.out_dtype, copy=False)

        return cov_rh, pai_rh, pavd_rh, height_rh, waveform_rh, H
