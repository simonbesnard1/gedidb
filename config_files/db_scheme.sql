-- Create the granule table
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_GRANULE_TABLE} (
   granule_name VARCHAR(60) PRIMARY KEY,
   status VARCHAR(10),
   created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create the metadata table
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_METADATA_TABLE} (
   SDS_Name VARCHAR(255) PRIMARY KEY,
   Description TEXT,
   units VARCHAR(100),
   product VARCHAR(100),
   source_table VARCHAR(255),
   created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE} (
    shot_number BIGINT PRIMARY KEY,
    granule VARCHAR(60),
    version VARCHAR(60),
    beam_type VARCHAR(20),
    beam_name VARCHAR(9),
    delta_time FLOAT,
    absolute_time TIMESTAMP,
    degrade_flag SMALLINT,
    leaf_off_flag SMALLINT,
    water_persistence SMALLINT,
    modis_nonvegetated FLOAT,
    modis_treecover FLOAT,
    sensitivity_a0 FLOAT,
    sensitivity_a1 FLOAT,
    sensitivity_a2 FLOAT,
    sensitivity_a3 FLOAT,
    sensitivity_a4 FLOAT,
    sensitivity_a5 FLOAT,
    sensitivity_a6 FLOAT,
    sensitivity_a10 FLOAT,
    solar_elevation FLOAT,
    solar_azimuth FLOAT,
    energy_total FLOAT,
    selected_algorithm SMALLINT,
    pft_class SMALLINT,
    region_class SMALLINT,
    urban_proportion SMALLINT,
    landsat_water_persistence SMALLINT,
    quality_flag SMALLINT,
    l2a_quality_flag SMALLINT,
    l2_quality_flag SMALLINT,
    l2b_quality_flag SMALLINT,
    l4_quality_flag SMALLINT,
    stale_return_flag SMALLINT,
    algorithmrun_flag SMALLINT,
    surface_flag SMALLINT,
    algorithm_run_flag SMALLINT,
    selected_l2a_algorithm SMALLINT,
    selected_rg_algorithm SMALLINT,
    selected_mode SMALLINT,
    dz FLOAT,
    digital_elevation_model_srtm FLOAT,
    waveform_count SMALLINT,
    waveform_start INT,
    sensitivity FLOAT,
    lon_lowestmode FLOAT,
    longitude_bin0 FLOAT,
    longitude_bin0_error FLOAT,
    lat_lowestmode FLOAT,
    latitude_bin0 FLOAT,
    latitude_bin0_error FLOAT,
    elev_lowestmode FLOAT,
    elevation_bin0 FLOAT,
    elevation_bin0_error FLOAT,
    lon_highestreturn FLOAT,
    lat_highestreturn FLOAT,
    elev_highestreturn FLOAT,
    predictor_limit_flag SMALLINT,
    response_limit_flag SMALLINT,
    digital_elevation_model FLOAT,
    agbd_t_se FLOAT,
    leaf_on_doy SMALLINT,
    leaf_on_cycle SMALLINT,
    rh FLOAT[],
    rh100 SMALLINT,
    cover FLOAT,
    cover_z FLOAT[],
    fhd_normal FLOAT,
    num_detectedmodes SMALLINT,
    omega FLOAT,
    pai FLOAT,
    pai_z FLOAT[],
    pavd_z FLOAT[],
    pgap_theta FLOAT,
    pgap_theta_error FLOAT,
    rg FLOAT,
    rhog FLOAT,
    rhog_error FLOAT,
    rhov FLOAT,
    rhov_error FLOAT,
    rossg FLOAT,
    rv FLOAT,
    rx_range_highestreturn FLOAT,
    agbd FLOAT,
    agbd_pi_lower FLOAT,
    agbd_pi_upper FLOAT,
    agbd_se FLOAT,
    agbd_t FLOAT,
    predict_stratum VARCHAR(60),
    wsci FLOAT,
    wsci_pi_lower FLOAT,
    wsci_pi_upper FLOAT,
    wsci_quality_flag SMALLINT,
    wsci_xy FLOAT,
    wsci_xy_pi_lower FLOAT, 
    wsci_xy_pi_upper FLOAT,
    master_frac FLOAT, 
    master_int INTEGER,
    geometry geometry(Point,4326)
) PARTITION BY RANGE ((ST_X(geometry), ST_Y(geometry)));  -- Partition by longitude and latitude

-- Western Hemisphere: Longitude -180° to 0°

-- Northern Polar Region (-180° to 0° longitude, 60° to 90° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (-180, 60) ) TO ( (0, 90) );

-- Northern Temperate Zone (-180° to 0° longitude, 30° to 60° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (-180, 30) ) TO ( (0, 60) );

-- Tropical Zone (-180° to 0° longitude, 0° to 30° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_tropical
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (-180, 0) ) TO ( (0, 30) );

-- Southern Temperate Zone (-180° to 0° longitude, -30° to 0° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (-180, -30) ) TO ( (0, 0) );

-- Southern Polar Region (-180° to 0° longitude, -90° to -30° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (-180, -90) ) TO ( (0, -30) );


-- Eastern Hemisphere: Longitude 0° to 180°

-- Northern Polar Region (0° to 180° longitude, 60° to 90° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (0, 60) ) TO ( (180, 90) );

-- Northern Temperate Zone (0° to 180° longitude, 30° to 60° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (0, 30) ) TO ( (180, 60) );

-- Tropical Zone (0° to 180° longitude, 0° to 30° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_tropical
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (0, 0) ) TO ( (180, 30) );

-- Southern Temperate Zone (0° to 180° longitude, -30° to 0° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (0, -30) ) TO ( (180, 0) );

-- Southern Polar Region (0° to 180° longitude, -90° to -30° latitude)
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_partitioned
FOR VALUES FROM ( (0, -90) ) TO ( (180, -30) );


-- Create spatial indexes for each partition
CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_north_polar ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_polar USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_north_temperate ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_temperate USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_tropical ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_tropical USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_south_temperate ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_temperate USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_south_polar ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_polar USING GIST (geometry);

CREATE INDEX IF NOT EXISTS idx_shot_geometry_eh_north_polar ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_polar USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_eh_north_temperate ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_temperate USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_eh_tropical ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_tropical USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_eh_south_temperate ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_temperate USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_shot_geometry_eh_south_polar ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_polar USING GIST (geometry);



