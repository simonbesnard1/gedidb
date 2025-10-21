# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
from typing import Tuple


def pgap_to_vertical_profiles(
    pgap_theta_z: np.ndarray,
    height: np.ndarray,
    local_beam_elevation: np.ndarray | float,
    rossg: float,
    omega: float,
    *,
    out_dtype=np.float32,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Compute canopy cover(z), PAI(z), and PAVD(z) from GEDI-style Pgap(theta, z).

    Parameters
    ----------
    pgap_theta_z : array_like
        Gap probability as a function of height z (and optionally shots).
        Shape (..., nz). Values should be in [0,1]. We'll clip for stability.
    height : array_like
        Height bin centers (m). Shape (nz,) or broadcastable to pgap's last dim.
        Can be ascending or descending; gradient sign is handled automatically.
    local_beam_elevation : float or array_like
        Beam elevation angle above the horizon, in radians (GEDI convention).
        cos(zenith) = |sin(elevation)|.
        Can be scalar or broadcastable to pgap_theta_z[..., None] without the z dim.
    rossg : float
        Projection function G (a.k.a. “Ross-G”) term. Typical ~0.5 for random leaf angles.
    omega : float
        Clumping factor Ω (<=1 for clumped canopies). If you don’t model clumping, use 1.0.
    out_dtype : numpy dtype, optional
        Output dtype (default float32 to save memory).

    Returns
    -------
    cover_z : ndarray
        Canopy cover profile, same shape as pgap_theta_z (… , nz).
        cover(z) = μ * (1 - Pgap), where μ = cos(zenith) = |sin(elevation)|.
    pai_z : ndarray
        Plant area index profile, same shape as pgap_theta_z.
        PAI(z) = - μ * ln(Pgap) / (G * Ω).
    pavd_z : ndarray
        Plant area volume density (dPAI/dz), numerically differentiated along z.

    Notes
    -----
    - Numerical safety: we clip Pgap into [eps, 1 - eps] to avoid log(0) and
      to keep cover well-behaved at the top/bottom of profiles.
    - Height monotonicity: if `height` is descending, the gradient sign is
      corrected automatically by using np.gradient with the provided axis.
    - Broadcasting: `local_beam_elevation` can be per-shot. We expand/broadcast
      it across the z dimension.
    """
    pgap = np.asarray(pgap_theta_z, dtype=np.float64)
    z = np.asarray(height, dtype=np.float64)

    # Ensure last axis is z
    if z.ndim != 1:
        raise ValueError("`height` must be 1D (nz,).")
    if pgap.shape[-1] != z.shape[0]:
        raise ValueError("Last dimension of `pgap_theta_z` must match `height` length.")

    # Broadcast elevation to match pgap without the z dimension
    elev = np.asarray(local_beam_elevation, dtype=np.float64)
    # Shape prep: make elevation broadcastable to pgap[..., None] then drop the trailing None later
    while elev.ndim < pgap.ndim - 1:
        elev = np.expand_dims(elev, axis=0)

    # μ = cos(zenith) = |sin(elevation)|
    mu = np.abs(np.sin(elev))
    # Expand μ to have a z dimension for clean broadcasting
    mu = np.broadcast_to(mu[..., None], pgap.shape)

    # Stabilize Pgap for log/cover
    eps = np.finfo(np.float64).eps
    pgap_safe = np.clip(pgap, eps, 1.0 - eps)

    # cover(z) = μ * (1 - Pgap)
    cover_z = (mu * (1.0 - pgap_safe)).astype(out_dtype, copy=False)

    # PAI(z) = - μ * ln(Pgap) / (G * Ω)
    denom = rossg * omega
    if not np.isfinite(denom) or denom <= 0:
        raise ValueError("`rossg * omega` must be positive and finite.")
    pai_z = (-np.log(pgap_safe) * mu / denom).astype(out_dtype, copy=False)

    # PAVD = - d(PAI) / dz  (your original sign convention)
    # np.gradient handles non-uniform z spacing if z is passed.
    # edge_order=2 tends to be smoother for well-behaved profiles.
    # Note: np.gradient differentiates along the last axis by default when passed the dx vector.
    dPAI_dz = np.gradient(
        pai_z.astype(np.float64, copy=False), z, axis=-1, edge_order=2
    )
    pavd_z = (-dPAI_dz).astype(out_dtype, copy=False)

    # Mask out where input pgap was nan
    nan_mask = ~np.isfinite(pgap_theta_z)
    if np.any(nan_mask):
        cover_z = np.where(nan_mask, np.nan, cover_z)
        pai_z = np.where(nan_mask, np.nan, pai_z)
        pavd_z = np.where(nan_mask, np.nan, pavd_z)

    return cover_z, pai_z, pavd_z
