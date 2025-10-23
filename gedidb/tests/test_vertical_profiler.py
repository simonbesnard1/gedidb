# SPDX-License-Identifier: EUPL-1.2
# Contact: besnard@gfz.de, felix.dombrowski@uni-potsdam.de and ah2174@cam.ac.uk
# SPDX-FileCopyrightText: 2025 Amelia Holcomb
# SPDX-FileCopyrightText: 2025 Felix Dombrowski
# SPDX-FileCopyrightText: 2025 Simon Besnard
# SPDX-FileCopyrightText: 2025 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
#

import numpy as np
import pytest

from gedidb.utils.profiles import (
    _scatter_waveform_jit,
    _interp1d_monotonic_edgefill,
    _pavd_from_pai_variable_dz,
    _resample_profiles_to_rh101_per_shot_jit,
    GEDIVerticalProfiler,
    _finite_max_rowwise,  # add this import if not already
)

# ---------------------------
# Extra coverage: helpers
# ---------------------------


def test_finite_max_rowwise_covers_all_nan_and_neginf():
    h = np.array(
        [
            [np.nan, np.nan, np.nan],  # all NaN
            [-np.inf, 2.0, -np.inf],  # finite values present -> 2.0
            [0.0, -5.0, -np.inf],  # finite max 0.0
        ],
        dtype=np.float64,
    )
    out = _finite_max_rowwise(h)
    # Accept NaN or -inf for all-NaN row (Numba+fastmath can differ)
    assert np.isnan(out[0]) or np.isneginf(out[0])
    assert np.isclose(out[1], 2.0)
    assert np.isclose(out[2], 0.0)


def test_pavd_from_pai_variable_dz_zero_spacing_nan_edges():
    # Create identical consecutive z at edges to force dz0==0 and dzN==0 -> NaNs there
    z = np.array([[1.0, 1.0, 2.0, 3.0], [0.0, 1.0, 2.0, 2.0]], dtype=np.float64)
    pai = z.copy()  # any finite values
    pavd = _pavd_from_pai_variable_dz(pai, z)
    assert np.isnan(pavd[0, 0]) and np.isnan(pavd[1, -1])
    # interior must still be finite where spacing is valid
    assert np.isfinite(pavd[0, 1:-1]).all()
    assert np.isfinite(pavd[1, 1:-1]).all()


# ---------------------------
# Extra coverage: RH resampling
# ---------------------------


def test_resample_rh_uses_provided_rh98_and_handles_all_nan_row():
    # Two shots; second has all-NaN heights -> all NaNs out
    h2d = np.array([[0.0, 5.0, 10.0], [np.nan, np.nan, np.nan]], dtype=np.float64)
    c = np.where(np.isfinite(h2d), 0.5, np.nan).astype(np.float64)
    pai = np.where(np.isfinite(h2d), 1.0, np.nan).astype(np.float64)
    pav = np.where(np.isfinite(h2d), 0.2, np.nan).astype(np.float64)

    # Provide rh98 (length n=2) to take the provided H path
    rh98 = np.array([9.0, np.nan], dtype=np.float64)
    cov_rh, pai_rh, pavd_rh, height_rh, H = _resample_profiles_to_rh101_per_shot_jit(
        h2d, c, pai, pav, rh98, min_canopy_height=2.0, out_dtype_code=1
    )
    # H matches provided vector
    assert np.allclose(H, [9.0, np.nan], equal_nan=True)
    # Row 2: count==0 (no finite heights) -> remains all NaN
    assert np.isnan(cov_rh[1]).all()
    # Row 1: height_rh spans 0..H[0]
    assert np.isclose(height_rh[0, 0], 0.0) and np.isclose(height_rh[0, -1], 9.0)


def test_resample_rh_duplicate_heights_get_deduped():
    # Duplicate heights within a shot ⇒ r gets deduplicated
    h2d = np.array([[0.0, 5.0, 5.0, 10.0]], dtype=np.float64)
    c = np.array([[0.1, 0.2, 0.8, 0.9]], dtype=np.float64)  # two values at same z
    pai = np.ones_like(c, dtype=np.float64)
    pav = np.ones_like(c, dtype=np.float64) * 0.3
    rh98 = np.empty(0, dtype=np.float64)
    cov_rh, *_ = _resample_profiles_to_rh101_per_shot_jit(
        h2d, c, pai, pav, rh98, min_canopy_height=2.0, out_dtype_code=0
    )
    # Just sanity: interpolation gives values within [min,max], no crashes
    assert np.nanmin(cov_rh) >= 0.1 - 1e-6 and np.nanmax(cov_rh) <= 0.9 + 1e-6


# ---------------------------
# Extra coverage: read_pgap_theta_z
# ---------------------------


def test_read_pgap_theta_z_minlength_and_start_offset_and_invalid_slice():
    # One shot with 2 samples; padding to minlength=5 with start_offset=2
    rx_start = np.array([1], dtype=np.int64)
    rx_count = np.array([2], dtype=np.int64)
    pgap_theta_z = np.array([0.4, 0.5], dtype=np.float32)
    pgap_theta = np.array([0.8], dtype=np.float32)
    h0 = np.array([10.0], dtype=np.float64)
    hL = np.array([0.0], dtype=np.float64)
    beam = {
        "rx_sample_start_index": rx_start,
        "rx_sample_count": rx_count,
        "pgap_theta_z": pgap_theta_z,
        "pgap_theta": pgap_theta,
        "geolocation/height_bin0": h0,
        "geolocation/height_lastbin": hL,
    }
    vp = GEDIVerticalProfiler(out_dtype=np.float32)
    P, Z = vp.read_pgap_theta_z(beam, start=0, finish=1, minlength=5, start_offset=2)
    # shape (n_shots, nz) => (1, 5)
    assert P.shape == (1, 5)
    # first start_offset columns set to 1.0 (pad)
    assert np.allclose(P[0, :2], 1.0)
    # then scattered data at positions 2 and 3
    assert np.allclose(P[0, 2:4], [0.4, 0.5])
    # trailing fill stays at base pgap_theta
    assert np.isclose(P[0, 4], 0.8)

    # invalid slice: start >= finish
    with pytest.raises(ValueError):
        vp.read_pgap_theta_z(beam, start=1, finish=1)


# ---------------------------
# Extra coverage: compute_profiles
# ---------------------------


def test_compute_profiles_raises_on_bad_denom_and_shape_and_rank():
    nz = 4
    z = np.linspace(0.0, 3.0, nz)
    pgap = np.linspace(0.2, 0.8, nz)[None, :]

    vp = GEDIVerticalProfiler(out_dtype=np.float32)

    # Non-positive denom (G*Ω) -> ValueError
    with pytest.raises(ValueError):
        vp.compute_profiles(
            pgap, z, local_beam_elevation=np.pi / 4, rossg=1.0, omega=0.0
        )

    # 2D height with mismatched shape -> ValueError
    z2 = np.broadcast_to(z, (2, nz))  # (2,n) but pgap is (1,n)
    with pytest.raises(ValueError):
        vp.compute_profiles(
            pgap, z2, local_beam_elevation=np.pi / 4, rossg=1.0, omega=1.0
        )

    # Wrong rank (3D height) -> ValueError
    z3 = np.zeros((1, nz, 1), dtype=np.float64)
    with pytest.raises(ValueError):
        vp.compute_profiles(
            pgap, z3, local_beam_elevation=np.pi / 4, rossg=1.0, omega=1.0
        )


def test_compute_profiles_float64_output_and_broadcasting():
    # 2 shots, per-shot 2D heights; broadcast elevation/G/Ω from scalars & 1D arrays
    pgap = np.array([[0.3, 0.4, 0.5], [0.2, 0.6, 0.8]], dtype=np.float64)
    z2d = np.array([[0.0, 1.0, 2.0], [0.0, 2.0, 4.0]], dtype=np.float64)

    # elevation as 1D per-shot vector; G scalar; omega scalar
    elev = np.array([np.pi / 6, np.pi / 3], dtype=np.float64)  # mu>0 for both
    vp = GEDIVerticalProfiler(out_dtype=np.float64)

    cov_rh, pai_rh, pavd_rh, height_rh, H = vp.compute_profiles(
        pgap_theta_z=pgap,
        height=z2d,
        local_beam_elevation=elev,
        rossg=0.5,  # scalar
        omega=2.0,  # scalar → denom=1.0 so pai=-ln(pgap)*mu
    )

    # dtypes are float64 as requested
    assert cov_rh.dtype == np.float64
    assert pai_rh.dtype == np.float64
    assert pavd_rh.dtype == np.float64
    assert height_rh.dtype == np.float64
    assert H.dtype == np.float64

    # height_rh should span 0..H per shot
    assert np.isclose(height_rh[0, 0], 0.0) and np.isclose(height_rh[0, -1], H[0])
    assert np.isclose(height_rh[1, 0], 0.0) and np.isclose(height_rh[1, -1], H[1])


def _scatter_ref(start_offset, counts, flat, out):
    """Pure-Python reference for expected behavior."""
    cursor = 0
    n_shots = counts.shape[0]
    for j in range(n_shots):
        c = int(counts[j])
        if c <= 0:
            continue
        seg = flat[cursor : cursor + c]
        # mimic the JIT code's assumptions: seg length must be c
        assert seg.shape[0] == c, "flat_values too short for provided counts"
        for k in range(c):
            out[start_offset + k, j] = seg[k]
        cursor += c
    return out


def test_scatter_waveform_jit_all_zero_or_negative_counts_noop():
    # counts <= 0 must be skipped; matrix remains unchanged
    counts = np.array([0, -2, 0, -1], dtype=np.int64)
    flat = np.array([], dtype=np.float32)
    out = np.full((4, 4), -7.0, dtype=np.float32)
    before = out.copy()
    _scatter_waveform_jit(0, counts, flat, out)
    assert np.array_equal(out, before)


def test_scatter_waveform_jit_varied_counts_with_offset():
    # Three shots, middle shot has zero count; start_offset shifts writes downward
    counts = np.array([2, 0, 3], dtype=np.int64)
    flat = np.array([10, 11, 20, 21, 22], dtype=np.float32)  # 2 + 0 + 3 = 5
    out = np.full((8, 3), np.nan, dtype=np.float32)

    _scatter_waveform_jit(2, counts, flat, out)

    # Shot 0 → rows 2..3
    assert np.allclose(out[2:4, 0], [10, 11], equal_nan=False)
    # Shot 1 skipped (count=0) → entire column still NaN
    assert np.isnan(out[:, 1]).all()
    # Shot 2 → rows 2..4
    assert np.allclose(out[2:5, 2], [20, 21, 22], equal_nan=False)
    # Above and below written regions remain NaN
    assert np.isnan(out[:2, 0]).all() and np.isnan(out[4:, 0]).all()
    assert np.isnan(out[:2, 2]).all() and np.isnan(out[5:, 2]).all()


def test_scatter_waveform_jit_matches_python_reference_multiple_shots():
    rng = np.random.default_rng(42)
    # Random counts including zeros; ensure sum(counts>0) == len(flat)
    counts = rng.integers(low=0, high=5, size=6, dtype=np.int64)
    total = int(counts.sum())
    flat = rng.integers(1, 100, size=total).astype(np.float32)
    start_offset = 1
    n_rows = int(counts.max() + start_offset + 2)
    out_jit = np.full((n_rows, counts.shape[0]), -1.0, dtype=np.float32)
    out_ref = out_jit.copy()

    _scatter_waveform_jit(start_offset, counts, flat, out_jit)
    out_ref = _scatter_ref(start_offset, counts, flat, out_ref)

    assert np.array_equal(out_jit, out_ref)
