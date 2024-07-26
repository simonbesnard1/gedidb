"""This file contains the mapping between DB columns and GEDI product fields.

    The field names are suffixed with the product from which they derive.
    (e.g. lon_lowestmode_level2A is the "lon_lowestmode" field from the L2A product)

    Some fields were created for user convenience but are not
    not explicitly present in any raw product.
    For example, "absolute_time_level2A" is GEDI_TIME_START + delta_time_level2A.
    To see how these fields were created,
    refer to the relevant GediGranule subclass (e.g. gedidb/granule/gedi_level2A.py)

    When fields are duplicated between products, we generally select from the
    lowest level product that contains the field.

    The basic quality flags for L2 (quality_flag, algorithm_run_flag, etc.) are not
    included in the DB, as the data is pre-filtered to contain only shots where
    these flags equal 1.

    Database types are not included here, but can be found in
      gedidb/database/gedidb_schema.py
    These types may not be the same as the types in the GEDI products, as
     SQL typing does not exactly match HDF5 typing.
"""

# Key: DB column name
# Value: GEDI field name + _product
COLUMN_TO_FIELD = {
    # Primary key and metadata (no suffixes)
    "shot_number": "shot_number",
    "granule": "granule",
    "geometry": "geometry",
    # General identifiable data
    "beam_type": "beam_type_level2A",
    "beam_name": "beam_name_level2A",
    "delta_time": "delta_time_level2A",
    "absolute_time": "absolute_time_level2A",
    # Quality data
    "degrade_flag": "degrade_flag_level2A",
    "sensitivity_a0": "sensitivity_a0_level2A",
    "sensitivity_a1": "sensitivity_a1_level2A",
    "sensitivity_a2": "sensitivity_a2_level2A",
    "sensitivity_a3": "sensitivity_a3_level2A",
    "sensitivity_a4": "sensitivity_a4_level2A",
    "sensitivity_a5": "sensitivity_a5_level2A",
    "sensitivity_a6": "sensitivity_a6_level2A",
    "sensitivity_a10": "sensitivity_a10_level4A",
    "solar_elevation": "solar_elevation_level2A",
    "solar_azimuth": "solar_azimuth_level2A",
    "energy_total": "energy_total_level2A",
    "elevation_difference_tdx": "elevation_difference_tdx_level2A",
    "l4_quality_flag": "l4_quality_flag_level4A",
    "l4_algorithm_run_flag": "algorithm_run_flag_level4A",
    "l4_predictor_limit_flag": "predictor_limit_flag_level4A",
    "l4_response_limit_flag": "response_limit_flag_level4A",
    "stale_return_flag": "stale_return_flag_level2B",
    # DEM
    "dem_tandemx": "digital_elevation_model_level2A",
    "dem_srtm": "digital_elevation_model_srtm_level2A",
    # Processing data
    "selected_l2a_algorithm": "selected_algorithm_level2A",
    "selected_mode": "selected_mode_level2A",
    "selected_rg_algorithm": "selected_rg_algorithm_level2B",
    "dz": "dz_level2B",
    # Geolocation data
    "lon_lowestmode": "lon_lowestmode_level2A",
    "longitude_bin0": "longitude_bin0_level2B",
    "longitude_bin0_error": "longitude_bin0_error_level2B",
    "lat_lowestmode": "lat_lowestmode_level2A",
    "latitude_bin0": "latitude_bin0_level2B",
    "latitude_bin0_error": "latitude_bin0_error_level2B",
    "elev_lowestmode": "elev_lowestmode_level2A",
    "elevation_bin0": "elevation_bin0_level2B",
    "elevation_bin0_error": "elevation_bin0_error_level2B",
    "lon_highestreturn": "lon_highestreturn_level2A",
    "lat_highestreturn": "lat_highestreturn_level2A",
    "elev_highestreturn": "elev_highestreturn_level2A",
    # Land cover data: NOTE this is gridded and/or derived data
    "gridded_pft_class": "pft_class_level4A",
    "gridded_region_class": "region_class_level4A",
    "gridded_urban_proportion": "urban_proportion_level4A",
    "gridded_water_persistence": "landsat_water_persistence_level4A",
    "gridded_leaf_off_flag": "leaf_off_flag_level2B",
    "gridded_leaf_on_doy": "leaf_on_doy_level2B",
    "gridded_leaf_on_cycle": "leaf_on_cycle_level2B",
    "interpolated_modis_nonvegetated": "modis_nonvegetated_level2B",
    "interpolated_modis_treecover": "modis_treecover_level2B",
    # Scientific data
    "cover": "cover_level2B",
    "cover_z": "cover_z_level2B",
    "fhd_normal": "fhd_normal_level2B",
    "num_detectedmodes": "num_detectedmodes_level2B",
    "omega": "omega_level2B",
    "pai": "pai_level2B",
    "pai_z": "pai_z_level2B",
    "pavd_z": "pavd_z_level2B",
    "pgap_theta": "pgap_theta_level2B",
    "pgap_theta_error": "pgap_theta_error_level2B",
    "rg": "rg_level2B",
    "rhog": "rhog_level2B",
    "rhog_error": "rhog_error_level2B",
    "rhov": "rhov_level2B",
    "rhov_error": "rhov_error_level2B",
    "rossg": "rossg_level2B",
    "rv": "rv_level2B",
    "rx_range_highestreturn": "rx_range_highestreturn_level2B",
    "agbd": "agbd_level4A",
    "agbd_pi_lower": "agbd_pi_lower_level4A",
    "agbd_pi_upper": "agbd_pi_upper_level4A",
    "agbd_se": "agbd_se_level4A",
    "agbd_t": "agbd_t_level4A",
    "agbd_t_se": "agbd_t_se_level4A",
    "rh_98": "rh_98_level2A",
} | {f"rh_{i}": f"rh_{i}_level2A" for i in range(0, 101, 5)}

FIELD_TO_COLUMN = {v: k for k, v in COLUMN_TO_FIELD.items()}
