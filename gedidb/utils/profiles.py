# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

from __future__ import annotations
from dataclasses import dataclass
from typing import Tuple, Optional
import numpy as np


@dataclass(slots=True)
class GEDIVerticalProfiler:
    out_dtype: np.dtype = np.float32
    gradient_edge_order: int = 2
    _eps: float = np.finfo(np.float64).eps

    def read_pgap_theta_z(
        self,
        beam_obj,  # L2BBeam-like: supports beam_obj["/path"][()]
        start: int = 0,
        finish: Optional[int] = None,
        minlength: Optional[int] = None,
        start_offset: int = 0,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Returns:
            pgap_2d  : (n_shots, nz)
            height_2d: (n_shots, nz)  height-above-ground per bin
        """
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
        out_shape = (max_count, n_shots)  # (M, N) -> transpose at end to (N, M)
        out_pgap_profile = np.broadcast_to(pgap_theta, out_shape).copy()
        if start_offset > 0:
            out_pgap_profile[:start_offset, :] = 1.0

        # Normalize indices to local flat buffer and scatter into correct rows
        local_start_indices = (start_indices - start_indices.min()).astype(np.int64)
        self._waveform_1d_to_2d(
            local_start_indices,
            counts,
            flat_pgap_prof,
            out_pgap_profile,
            start_offset=start_offset,
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
        ).astype(self.out_dtype)

        # Return as (n_shots, nz)
        return out_pgap_profile.T, out_height.T

    @staticmethod
    def _waveform_1d_to_2d(
        start_indices: np.ndarray,
        counts: np.ndarray,
        flat_values: np.ndarray,
        out_2d: np.ndarray,
        *,
        start_offset: int = 0,
    ) -> None:
        """In-place scatter from flat concatenation into columns of out_2d."""
        cursor = 0
        for j, (s, c) in enumerate(zip(start_indices, counts)):
            if c <= 0:
                continue
            seg = flat_values[cursor : cursor + c]
            stop = start_offset + c
            out_2d[start_offset:stop, j] = seg
            cursor += c

    def compute_profiles(
        self,
        pgap_theta_z: np.ndarray,  # (n_shots, nz) or (..., nz)
        height: np.ndarray,  # (nz,) or (n_shots, nz), height above ground (m)
        local_beam_elevation: np.ndarray | float,  # scalar or (n_shots,)
        rossg: np.ndarray | float,  # scalar or (n_shots,)
        omega: np.ndarray | float,  # scalar or (n_shots,)
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Compute z-domain cover/PAI/PAVD from Pgap, then:
          1) mask bins where height < 0
          2) resample to a 101-point RH grid (0..100), preserving PAI for PAVD

        Returns
        -------
        cover_rh  : (n_shots, 101)
        pai_rh    : (n_shots, 101)
        pavd_rh   : (n_shots, 101)   [scaled by H/100]
        height_rh : (n_shots, 101)   height above ground (m) at each RH bin
        H         : (n_shots,)       canopy height used for normalization (m)

        Notes
        -----
        - `height` is expected to be height ABOVE GROUND; if you pass absolute
          elevation, convert to above-ground before calling (e.g., z - elev_lowestmode).
        - For per-shot height (2D), dz is handled row-wise with safe guards.
        """
        # ---- inputs & numerics ----
        pgap = np.asarray(pgap_theta_z, dtype=np.float64)
        z_in = np.asarray(height, dtype=np.float64)

        # Clip pgap for numeric stability (avoid log(0) / log(>1))
        pgap = np.clip(pgap, self._eps, 1.0 - self._eps)

        # Broadcast μ = |sin(elevation)| to pgap shape (no z-dim in elev)
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
        cover_z = (mu * (1.0 - pgap)).astype(self.out_dtype, copy=False)
        pai_z = (-np.log(pgap) * mu / denom).astype(self.out_dtype, copy=False)

        # ---- dz-aware derivative: PAVD = - d(PAI)/dz ----
        if z_in.ndim == 1:
            # global 1D height axis
            dPAI_dz = np.gradient(
                pai_z.astype(np.float64, copy=False),
                z_in,
                axis=-1,
                edge_order=self.gradient_edge_order,
            )
        elif z_in.ndim == 2:
            # per-shot height grids (n_shots, nz)
            if z_in.shape != pai_z.shape:
                raise ValueError(
                    "height (n_shots, nz) must match pgap/pai shapes (n_shots, nz)."
                )
            pai64 = pai_z.astype(np.float64, copy=False)
            dPAI_dz = np.full_like(pai64, np.nan)

            # edges
            dz0 = z_in[:, 1] - z_in[:, 0]
            dzN = z_in[:, -1] - z_in[:, -2]
            bad0 = ~np.isfinite(dz0) | (dz0 == 0)
            badN = ~np.isfinite(dzN) | (dzN == 0)
            with np.errstate(divide="ignore", invalid="ignore"):
                d_left = (pai64[:, 1] - pai64[:, 0]) / dz0
                d_right = (pai64[:, -1] - pai64[:, -2]) / dzN
            d_left[bad0] = np.nan
            d_right[badN] = np.nan
            dPAI_dz[:, 0] = d_left
            dPAI_dz[:, -1] = d_right

            # central differences for interior
            dzc = z_in[:, 2:] - z_in[:, :-2]
            badc = ~np.isfinite(dzc) | (dzc == 0)
            with np.errstate(divide="ignore", invalid="ignore"):
                d_center = (pai64[:, 2:] - pai64[:, :-2]) / dzc
            d_center[badc] = np.nan
            dPAI_dz[:, 1:-1] = d_center
        else:
            raise ValueError("height must be 1D or 2D.")

        pavd_z = (-dPAI_dz).astype(self.out_dtype, copy=False)

        # ---- propagate NaNs from original pgap (pre-clip) ----
        nan_mask = ~np.isfinite(pgap_theta_z)
        if np.any(nan_mask):
            cover_z = np.where(nan_mask, np.nan, cover_z)
            pai_z = np.where(nan_mask, np.nan, pai_z)
            pavd_z = np.where(nan_mask, np.nan, pavd_z)

        # ---- ensure height is (n_shots, nz) for RH step ----
        if z_in.ndim == 1:
            # broadcast 1D height to all shots
            n_shots = pgap.shape[0]
            height_2d = np.broadcast_to(z_in, (n_shots, z_in.shape[0])).astype(
                np.float64, copy=False
            )
        else:
            height_2d = z_in

        # ---- 1) mask away height < 0 ----
        height_masked, cover_m, pai_m, pavd_m = self._mask_negative_heights(
            height_2d, cover_z, pai_z, pavd_z
        )

        # ---- 2) resample to 101-point RH grid (vegetation-only); H returned too ----
        cov_rh, pai_rh, pavd_rh, height_rh, H = (
            self.resample_profiles_to_rh101_per_shot(
                height_masked,
                cover_m,
                pai_m,
                pavd_m,
                rh98=None,  # or pass per-shot rh98 above ground if you have it
                min_canopy_height=2.0,
                out_dtype=self.out_dtype,
            )
        )

        return cov_rh, pai_rh, pavd_rh, height_rh, H

    @staticmethod
    def _mask_negative_heights(
        height_2d: np.ndarray, *profiles: np.ndarray
    ) -> Tuple[np.ndarray, ...]:
        """
        Mask (set to NaN) all bins where height < 0, applied consistently to
        the provided profile arrays (same shape). Returns masked copies.
        """
        h = np.asarray(height_2d, dtype=np.float64)
        mask = ~(h >= 0.0)  # True where height < 0 or NaN
        out = []
        for prof in profiles:
            p = np.asarray(prof, dtype=np.float64).copy()
            p[mask] = np.nan
            out.append(p)
        # also return the masked height for convenience
        h_masked = h.copy()
        h_masked[mask] = np.nan
        return (h_masked, *out)

    @staticmethod
    def resample_profiles_to_rh101_per_shot(
        height_2d: np.ndarray,  # (n_shots, nz), height above ground (m)
        cover_z: np.ndarray,  # (n_shots, nz)
        pai_z: np.ndarray,  # (n_shots, nz)
        pavd_z: np.ndarray,  # (n_shots, nz)
        rh98: Optional[
            np.ndarray
        ] = None,  # (n_shots,), *height above ground* if provided
        *,
        min_canopy_height: float = 2.0,
        out_dtype=np.float32,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """
        Resample per-shot z-domain profiles to a fixed RH grid [0..100] (101 points).

        Returns
        -------
        cover_rh  : (n_shots, 101)
        pai_rh    : (n_shots, 101)
        pavd_rh   : (n_shots, 101)  [scaled by H/100 to conserve ∫PAVD dz = PAI]
        height_rh : (n_shots, 101)  height above ground (m) at each RH bin
        H         : (n_shots,)      canopy height used for normalization (m)
        """

        def _interp_to_rh(rh_axis, r, y, *, fill="edge", dtype=np.float64):
            """Robust interp: accepts descending r, NaNs, duplicates; configurable tail fill."""
            r = np.asarray(r, np.float64)
            y = np.asarray(y, np.float64)
            m = np.isfinite(r) & np.isfinite(y)
            if not np.any(m):
                return np.full_like(rh_axis, np.nan, dtype=dtype)
            r = r[m]
            y = y[m]
            # make increasing
            if r[0] > r[-1]:
                r = r[::-1]
                y = y[::-1]
            # sort & dedup
            order = np.argsort(r)
            r = r[order]
            y = y[order]
            r, idx = np.unique(r, return_index=True)
            y = y[idx]
            if r.size == 1:
                return np.full_like(
                    rh_axis, y[0] if fill == "edge" else np.nan, dtype=dtype
                )
            left_val, right_val = (y[0], y[-1]) if fill == "edge" else (np.nan, np.nan)
            return np.interp(rh_axis, r, y, left=left_val, right=right_val).astype(
                dtype, copy=False
            )

        h = np.asarray(height_2d, dtype=np.float64)
        cov = np.asarray(cover_z, dtype=np.float64)
        pai = np.asarray(pai_z, dtype=np.float64)
        pav = np.asarray(pavd_z, dtype=np.float64)

        if h.shape != cov.shape or h.shape != pai.shape or h.shape != pav.shape:
            raise ValueError("height_2d and profiles must share shape (n_shots, nz).")

        n_shots, _ = h.shape
        rh_axis = np.arange(101, dtype=np.float64)

        # ---- Canopy height per shot (height above ground) ----
        if rh98 is not None:
            H = np.asarray(rh98, dtype=np.float64).reshape(-1)
            if H.shape[0] != n_shots:
                raise ValueError("rh98 must be (n_shots,).")
        else:
            # No-warning rowwise max even when all-NaN
            finite = np.isfinite(h)
            any_finite = finite.any(axis=1)
            h_no_nan = np.where(finite, h, -np.inf)
            H = h_no_nan.max(axis=1)
            # rows that were all-NaN: set H to NaN explicitly
            H[~any_finite] = np.nan

        # Valid shots: finite H and tall enough
        valid = np.isfinite(H) & (H >= min_canopy_height)

        # ---- Allocate outputs ----
        cov_rh = np.full((n_shots, 101), np.nan, dtype=out_dtype)
        pai_rh = np.full((n_shots, 101), np.nan, dtype=out_dtype)
        pavd_rh = np.full((n_shots, 101), np.nan, dtype=out_dtype)
        height_rh = np.full((n_shots, 101), np.nan, dtype=out_dtype)

        # Precompute height_rh for valid shots: z(r) = H * r/100
        height_rh[valid] = (H[valid, None] * (rh_axis[None, :] / 100.0)).astype(
            out_dtype, copy=False
        )

        # ---- Per-shot interpolation ----
        for i in range(n_shots):
            Hi = H[i]
            if not valid[i]:
                continue

            zi = h[i]  # height above ground
            czi = cov[i]
            paii = pai[i]
            pzi = pav[i]

            # RH support for this shot; clip to [0,100] to avoid extrapolation warnings
            r = np.clip(100.0 * (zi / Hi), 0.0, 100.0)

            cov_r = _interp_to_rh(rh_axis, r, czi, fill="edge", dtype=np.float64)
            pai_r = _interp_to_rh(rh_axis, r, paii, fill="edge", dtype=np.float64)
            pav_r = _interp_to_rh(rh_axis, r, pzi, fill="edge", dtype=np.float64)

            cov_rh[i] = cov_r.astype(out_dtype, copy=False)
            pai_rh[i] = pai_r.astype(out_dtype, copy=False)
            # Conserve PAI under variable change: PAVD_r = PAVD_z * (dz/dr) = PAVD_z * (H/100)
            pavd_rh[i] = (pav_r * (Hi / 100.0)).astype(out_dtype, copy=False)

        return cov_rh, pai_rh, pavd_rh, height_rh, H
