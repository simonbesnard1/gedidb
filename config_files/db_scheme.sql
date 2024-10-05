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
    shot_number BIGINT,
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
    geometry geometry(Point,4326),  
    zone VARCHAR(50),
    PRIMARY KEY (zone, shot_number)

) PARTITION BY LIST (zone);  -- Partition by zone


-- Create the trigger function to calculate the zone
CREATE OR REPLACE FUNCTION {DEFAULT_SCHEMA}.calculate_zone()
RETURNS trigger AS $$
BEGIN
    IF NEW.lon_lowestmode >= -180 AND NEW.lon_lowestmode < 0 THEN
        -- Western Hemisphere
        IF NEW.lat_lowestmode >= 60 AND NEW.lat_lowestmode <= 90 THEN
            NEW.zone := 'wh_north_polar';  -- Zone name
        ELSIF NEW.lat_lowestmode >= 30 AND NEW.lat_lowestmode < 60 THEN
            NEW.zone := 'wh_north_temperate';
        ELSIF NEW.lat_lowestmode >= 0 AND NEW.lat_lowestmode < 30 THEN
            NEW.zone := 'wh_tropical';
        ELSIF NEW.lat_lowestmode >= -30 AND NEW.lat_lowestmode < 0 THEN
            NEW.zone := 'wh_south_temperate';
        ELSIF NEW.lat_lowestmode >= -90 AND NEW.lat_lowestmode < -30 THEN
            NEW.zone := 'wh_south_polar';
        ELSE
            RAISE EXCEPTION 'Invalid lat_lowestmode for Western Hemisphere: %', NEW.lat_lowestmode;
        END IF;
    ELSIF NEW.lon_lowestmode >= 0 AND NEW.lon_lowestmode <= 180 THEN
        -- Eastern Hemisphere
        IF NEW.lat_lowestmode >= 60 AND NEW.lat_lowestmode <= 90 THEN
            NEW.zone := 'eh_north_polar';
        ELSIF NEW.lat_lowestmode >= 30 AND NEW.lat_lowestmode < 60 THEN
            NEW.zone := 'eh_north_temperate';
        ELSIF NEW.lat_lowestmode >= 0 AND NEW.lat_lowestmode < 30 THEN
            NEW.zone := 'eh_tropical';
        ELSIF NEW.lat_lowestmode >= -30 AND NEW.lat_lowestmode < 0 THEN
            NEW.zone := 'eh_south_temperate';
        ELSIF NEW.lat_lowestmode >= -90 AND NEW.lat_lowestmode < -30 THEN
            NEW.zone := 'eh_south_polar';
        ELSE
            RAISE EXCEPTION 'Invalid lat_lowestmode for Eastern Hemisphere: %', NEW.lat_lowestmode;
        END IF;
    ELSE
        -- Handle edge cases, e.g., if lon_lowestmode is outside expected range
        RAISE EXCEPTION 'Invalid lon_lowestmode: %', NEW.lon_lowestmode;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Drop the trigger if it exists
DROP TRIGGER IF EXISTS calculate_zone_trigger ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE};

-- Create the trigger to invoke the function
CREATE TRIGGER calculate_zone_trigger
BEFORE INSERT OR UPDATE ON {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR EACH ROW EXECUTE FUNCTION {DEFAULT_SCHEMA}.calculate_zone();

-- Create partitions for each zone

-- Zone: wh_north_polar
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('wh_north_polar');

-- Zone: wh_north_temperate
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_north_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('wh_north_temperate');

-- Zone: wh_tropical
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_tropical
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('wh_tropical');

-- Zone: wh_south_temperate
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('wh_south_temperate');

-- Zone: wh_south_polar
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_wh_south_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('wh_south_polar');

-- Zone: eh_north_polar
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('eh_north_polar');

-- Zone: eh_north_temperate
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_north_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('eh_north_temperate');

-- Zone: eh_tropical
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_tropical
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('eh_tropical');

-- Zone: eh_south_temperate
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_temperate
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('eh_south_temperate');

-- Zone: eh_south_polar
CREATE TABLE IF NOT EXISTS {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}_eh_south_polar
PARTITION OF {DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}
FOR VALUES IN ('eh_south_polar');

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



