# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np

from gedidb.utils.profiles import (
    _scatter_waveform_jit,
    _interp1d_monotonic_edgefill,
    _pavd_from_pai_variable_dz,
    _resample_profiles_to_rh101_per_shot_jit,
    GEDIVerticalProfiler,
)

# ---------------------------
# JIT helpers: unit coverage
# ---------------------------


def test_scatter_waveform_jit_basic():
    # Two shots, counts=[3,0,2], start_offset=1
    counts = np.array([3, 0, 2], dtype=np.int64)[:2]  # just [3,0]
    flat = np.array([10, 11, 12], dtype=np.float32)
    out = np.full((5, 2), -1.0, dtype=np.float32)
    _scatter_waveform_jit(1, counts, flat, out)
    # Column 0 gets values at rows 1..3 (start_offset + count)
    assert np.allclose(out[1:4, 0], [10, 11, 12])
    # Column 1 unchanged because count=0
    assert np.all(out[:, 1] == -1.0)


def test_interp1d_monotonic_edgefill_edges_and_singleton():
    x = np.array([0.0, 5.0, 10.0], dtype=np.float64)
    y = np.array([0.0, 50.0, 100.0], dtype=np.float64)
    q = np.array([-3.0, 0.0, 2.5, 10.0, 12.0], dtype=np.float64)
    r = _interp1d_monotonic_edgefill(q, x, y)
    # edge clamping & linear in the middle
    assert np.allclose(r, [0.0, 0.0, 25.0, 100.0, 100.0])

    # singleton x → constant fill
    x1 = np.array([7.0])
    y1 = np.array([42.0])
    q1 = np.array([0.0, 7.0, 10.0])
    r1 = _interp1d_monotonic_edgefill(q1, x1, y1)
    assert np.allclose(r1, [42.0, 42.0, 42.0])


def test_pavd_from_pai_variable_dz_linear_profile():
    # For PAI = a*z + b, dPAI/dz = a, so PAVD = -(dPAI/dz) = -a
    a = 0.2
    b = 1.0
    z = np.array([[0.0, 1.0, 1.5, 3.0], [0.0, 2.0, 3.0, 4.0]], dtype=np.float64)
    pai = a * z + b
    pavd = _pavd_from_pai_variable_dz(pai, z)
    # Non-uniform spacing: edges use forward/backward, interior uses central difference
    # All should be close to -a
    assert np.allclose(pavd, -a, atol=1e-12, equal_nan=False)


# ---------------------------------------------------------
# RH resampling core + pipeline: behavioral/unit assertions
# ---------------------------------------------------------


def test_resample_rh_min_height_gate():
    # n shots=2, m bins=4. Shot 0: max height (H) = 1.0 (< 2.0 min) → all NaN rows
    # Shot 1: heights up to 10 → should produce values
    h2d = np.array(
        [[0.0, 0.5, 1.0, np.nan], [0.0, 5.0, 10.0, np.nan]], dtype=np.float64
    )
    # cover/pai/pavd values arbitrary finite
    c = np.where(np.isfinite(h2d), 0.5, np.nan)
    pai = np.where(np.isfinite(h2d), 1.0, np.nan)
    pav = np.where(np.isfinite(h2d), 0.1, np.nan)
    rh98 = np.empty(0, dtype=np.float64)  # not provided; use finite rowwise max
    cov_rh, pai_rh, pavd_rh, height_rh, H = _resample_profiles_to_rh101_per_shot_jit(
        h2d, c, pai, pav, rh98, min_canopy_height=2.0, out_dtype_code=0
    )
    # H is derived from finite max of each row
    assert np.allclose(H, [1.0, 10.0])
    # First row is below threshold → all NaNs
    assert np.all(np.isnan(cov_rh[0]))
    assert np.all(np.isnan(pai_rh[0]))
    assert np.all(np.isnan(pavd_rh[0]))
    assert np.all(np.isnan(height_rh[0]))
    # Second row: height_rh spans 0..H
    assert np.isclose(height_rh[1, 0], 0.0)
    assert np.isclose(height_rh[1, 100], 10.0)


def test_resample_rh_height_mapping_and_types():
    # One shot, monotonic heights 0..10; constant cover/pai/pavd
    h2d = np.array([[0.0, 2.5, 5.0, 7.5, 10.0]], dtype=np.float64)
    c = np.array([[0.25, 0.25, 0.25, 0.25, 0.25]], dtype=np.float64)
    pai = np.array([[1.0, 1.2, 1.4, 1.6, 1.8]], dtype=np.float64)
    pav = np.array([[0.1, 0.1, 0.1, 0.1, 0.1]], dtype=np.float64)
    rh98 = np.empty(0, dtype=np.float64)
    cov_rh, pai_rh, pavd_rh, height_rh, H = _resample_profiles_to_rh101_per_shot_jit(
        h2d, c, pai, pav, rh98, min_canopy_height=2.0, out_dtype_code=0  # float32
    )
    # Types (returned float64 inside JIT; caller casts later—here we assert numerical content)
    assert cov_rh.shape == (1, 101)
    assert pai_rh.shape == (1, 101)
    assert pavd_rh.shape == (1, 101)
    assert height_rh.shape == (1, 101)
    # RH height mapping 0..H
    assert np.isclose(H[0], 10.0)
    assert np.isclose(height_rh[0, 0], 0.0)
    assert np.isclose(height_rh[0, -1], 10.0)
    # Interpolation keeps values within range
    assert np.nanmin(cov_rh) >= 0.25 - 1e-6 and np.nanmax(cov_rh) <= 0.25 + 1e-6


def test_read_pgap_theta_z_reconstruction():
    # Simulate two shots: counts = [3, 2], start indices are 1-based in file
    rx_start = np.array([1, 4], dtype=np.int64)  # 1-based
    rx_count = np.array([3, 2], dtype=np.int64)
    pgap_theta_z = np.array([0.2, 0.3, 0.4, 0.9, 1.0], dtype=np.float32)  # flat
    pgap_theta = np.array([0.7, 0.8], dtype=np.float32)  # per-shot base fill
    h0 = np.array([30.0, 20.0], dtype=np.float64)
    hL = np.array([0.0, 0.0], dtype=np.float64)

    beam = {
        "rx_sample_start_index": rx_start,
        "rx_sample_count": rx_count,
        "pgap_theta_z": pgap_theta_z,
        "pgap_theta": pgap_theta,
        "geolocation/height_bin0": h0,
        "geolocation/height_lastbin": hL,
    }

    vp = GEDIVerticalProfiler(out_dtype=np.float32)
    P, Z = vp.read_pgap_theta_z(beam, start=0, finish=2, minlength=None, start_offset=0)
    # Shapes: (n_shots, nz) where nz=max(count)=3
    assert P.shape == (2, 3)
    assert Z.shape == (2, 3)
    # Shot 0 gets [0.2,0.3,0.4], shot 1 has count 2 then last column remains base-filled (0.8)
    assert np.allclose(P[0], [0.2, 0.3, 0.4])
    assert np.allclose(P[1], [0.9, 1.0, 0.8], rtol=0, atol=1e-7)
    # Heights descend by equal spacing per shot
    dz0 = (h0[0] - hL[0]) / (rx_count[0] - 1)
    dz1 = (h0[1] - hL[1]) / (rx_count[1] - 1)
    assert np.allclose(Z[0], [30.0, 30.0 - dz0, 30.0 - 2 * dz0])
    assert np.allclose(Z[1, :2], [20.0, 20.0 - dz1])


def test_compute_profiles_simple_pipeline():
    # One shot, global z axis, pgap increases with height (just synthetic)
    nz = 6
    z = np.linspace(0.0, 10.0, nz)
    pgap = np.linspace(0.2, 0.8, nz)[None, :]  # (1,nz)
    vp = GEDIVerticalProfiler(out_dtype=np.float32)

    # Choose elevation=pi/2 → mu=1, and G*Omega=1 → pai=-ln(pgap)
    elev = np.pi / 2
    rossg = 1.0
    omega = 1.0

    cov_rh, pai_rh, pavd_rh, height_rh, H = vp.compute_profiles(
        pgap_theta_z=pgap,
        height=z,  # 1D axis
        local_beam_elevation=elev,
        rossg=rossg,
        omega=omega,
    )

    # Shapes and basic properties
    assert cov_rh.shape == (1, 101)
    assert pai_rh.shape == (1, 101)
    assert pavd_rh.shape == (1, 101)
    assert height_rh.shape == (1, 101)
    assert H.shape == (1,)
    # height_rh is 0..max(z)
    assert np.isclose(H[0], z.max(), atol=1e-12)
    assert np.isclose(height_rh[0, 0], 0.0)
    assert np.isclose(height_rh[0, -1], z.max())
    # cover_z ≈ (1 - pgap) * mu; after RH resampling it should be bounded in [0,1]
    assert float(np.nanmin(cov_rh)) >= 0.0 - 1e-6
    assert float(np.nanmax(cov_rh)) <= 1.0 + 1e-6


def test_compute_profiles_nan_mask_and_negative_heights_masking():
    pgap = np.array([[0.5, np.nan, 0.7, 0.9], [0.6, 0.6, 0.6, 0.6]], dtype=np.float64)
    # shot0 has a negative height at first bin; shot1 stays below the 2 m gate (H=1.5)
    z2d = np.array([[-1.0, 0.0, 1.0, 2.0], [0.0, 0.5, 1.0, 1.5]], dtype=np.float64)

    vp = GEDIVerticalProfiler(out_dtype=np.float32)
    cov_rh, pai_rh, pavd_rh, height_rh, H = vp.compute_profiles(
        pgap_theta_z=pgap,
        height=z2d,
        local_beam_elevation=np.pi / 2,
        rossg=1.0,
        omega=1.0,
    )

    # H is finite (derived from finite max heights)
    assert np.all(np.isfinite(H))
    assert cov_rh.shape == (2, 101)

    # Shot 0: has some finite bins (H=2.0 meets the ≥2.0 requirement), but NaNs propagate
    assert np.isnan(cov_rh[0]).any()
    assert np.isfinite(cov_rh[0]).any()

    # Shot 1: below 2 m -> fully masked (all NaN)
    assert np.isnan(cov_rh[1]).all()
