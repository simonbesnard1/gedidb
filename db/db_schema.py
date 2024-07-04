from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import mapped_column
from sqlalchemy import (
    String,
    SmallInteger,
    BigInteger,
    Float,
    DateTime,
    ARRAY,
)
from geoalchemy2 import Geometry


class Base(DeclarativeBase):
    pass


class Shots(Base):
    __tablename__ = "filtered_l2ab_l4a_shots"

    # General data
    shot_number = mapped_column(
        BigInteger, nullable=False, primary_key=True, autoincrement=False
    )
    granule = mapped_column(String(60), nullable=False)
    beam_type = mapped_column(String(20), nullable=False)
    beam_name = mapped_column(String(8), nullable=False)
    delta_time = mapped_column(Float, nullable=False)
    absolute_time = mapped_column(DateTime, nullable=False)

    # Quality data
    degrade_flag = mapped_column(SmallInteger, nullable=False)
    sensitivity_a0 = mapped_column(Float, nullable=False)
    sensitivity_a1 = mapped_column(Float, nullable=False)
    sensitivity_a2 = mapped_column(Float, nullable=False)
    sensitivity_a3 = mapped_column(Float, nullable=False)
    sensitivity_a4 = mapped_column(Float, nullable=False)
    sensitivity_a5 = mapped_column(Float, nullable=False)
    sensitivity_a6 = mapped_column(Float, nullable=False)
    sensitivity_a10 = mapped_column(Float, nullable=False)
    solar_elevation = mapped_column(Float, nullable=False)
    solar_azimuth = mapped_column(Float, nullable=False)
    energy_total = mapped_column(Float, nullable=False)
    elevation_difference_tdx = mapped_column(Float, nullable=False)
    l4_quality_flag = mapped_column(SmallInteger, nullable=False)
    l4_algorithm_run_flag = mapped_column(SmallInteger, nullable=False)
    l4_predictor_limit_flag = mapped_column(SmallInteger, nullable=False)
    l4_response_limit_flag = mapped_column(SmallInteger, nullable=False)
    stale_return_flag = mapped_column(SmallInteger, nullable=False)

    # DEM
    dem_tandemx = mapped_column(Float, nullable=False)
    dem_srtm = mapped_column(Float, nullable=False)

    # Processing data
    selected_l2a_algorithm = mapped_column(SmallInteger, nullable=False)
    selected_rg_algorithm = mapped_column(SmallInteger, nullable=False)
    selected_mode = mapped_column(SmallInteger, nullable=False)
    dz = mapped_column(Float, nullable=False)

    # Geolocation data
    lon_lowestmode = mapped_column(Float, nullable=False)
    longitude_bin0 = mapped_column(Float, nullable=False)
    longitude_bin0_error = mapped_column(Float, nullable=False)
    lat_lowestmode = mapped_column(Float, nullable=False)
    latitude_bin0 = mapped_column(Float, nullable=False)
    latitude_bin0_error = mapped_column(Float, nullable=False)
    elev_lowestmode = mapped_column(Float, nullable=False)
    elevation_bin0 = mapped_column(Float, nullable=False)
    elevation_bin0_error = mapped_column(Float, nullable=False)
    lon_highestreturn = mapped_column(Float, nullable=False)
    lat_highestreturn = mapped_column(Float, nullable=False)
    elev_highestreturn = mapped_column(Float, nullable=False)

    # Land cover data: NOTE this is gridded and/or derived data
    gridded_pft_class = mapped_column(SmallInteger, nullable=False)
    gridded_region_class = mapped_column(SmallInteger, nullable=False)
    gridded_urban_proportion = mapped_column(Float, nullable=False)
    gridded_water_persistence = mapped_column(Float, nullable=False)
    gridded_leaf_off_flag = mapped_column(SmallInteger, nullable=False)
    # is this one a smallinteger?
    gridded_leaf_on_doy = mapped_column(SmallInteger, nullable=False)
    gridded_leaf_on_cycle = mapped_column(SmallInteger, nullable=False)
    interpolated_modis_nonvegetated = mapped_column(Float, nullable=False)
    interpolated_modis_treecover = mapped_column(Float, nullable=False)

    # Scientific data
    rh_0 = mapped_column(Float, nullable=False)
    rh_5 = mapped_column(Float, nullable=False)
    rh_10 = mapped_column(Float, nullable=False)
    rh_15 = mapped_column(Float, nullable=False)
    rh_20 = mapped_column(Float, nullable=False)
    rh_25 = mapped_column(Float, nullable=False)
    rh_30 = mapped_column(Float, nullable=False)
    rh_35 = mapped_column(Float, nullable=False)
    rh_40 = mapped_column(Float, nullable=False)
    rh_45 = mapped_column(Float, nullable=False)
    rh_50 = mapped_column(Float, nullable=False)
    rh_55 = mapped_column(Float, nullable=False)
    rh_60 = mapped_column(Float, nullable=False)
    rh_65 = mapped_column(Float, nullable=False)
    rh_70 = mapped_column(Float, nullable=False)
    rh_75 = mapped_column(Float, nullable=False)
    rh_80 = mapped_column(Float, nullable=False)
    rh_85 = mapped_column(Float, nullable=False)
    rh_90 = mapped_column(Float, nullable=False)
    rh_95 = mapped_column(Float, nullable=False)
    rh_98 = mapped_column(Float, nullable=False)
    rh_100 = mapped_column(Float, nullable=False)
    cover = mapped_column(Float, nullable=False)
    cover_z = mapped_column(ARRAY(Float), nullable=False)
    fhd_normal = mapped_column(Float, nullable=False)
    num_detectedmodes = mapped_column(SmallInteger, nullable=False)
    omega = mapped_column(Float, nullable=False)
    pai = mapped_column(Float, nullable=False)
    pai_z = mapped_column(ARRAY(Float), nullable=False)
    pavd_z = mapped_column(ARRAY(Float), nullable=False)
    pgap_theta = mapped_column(Float, nullable=False)
    pgap_theta_error = mapped_column(Float, nullable=False)
    rg = mapped_column(Float, nullable=False)
    rhog = mapped_column(Float, nullable=False)
    rhog_error = mapped_column(Float, nullable=False)
    rhov = mapped_column(Float, nullable=False)
    rhov_error = mapped_column(Float, nullable=False)
    rossg = mapped_column(Float, nullable=False)
    rv = mapped_column(Float, nullable=False)
    rx_range_highestreturn = mapped_column(Float, nullable=False)
    agbd = mapped_column(Float, nullable=False)
    agbd_pi_lower = mapped_column(Float, nullable=False)
    agbd_pi_upper = mapped_column(Float, nullable=False)
    agbd_se = mapped_column(Float, nullable=False)
    agbd_t = mapped_column(Float, nullable=False)
    agbd_t_se = mapped_column(Float, nullable=False)

    # Geometry data
    geometry = mapped_column(Geometry("POINT", srid=4326), nullable=False)


class Granules(Base):
    __tablename__ = "gedi_granules"

    granule_name = mapped_column(String(60), nullable=False, primary_key=True)
    granule_hash = mapped_column(String(60), nullable=False)
    granule_file = mapped_column(String(100), nullable=False)
    l2a_file = mapped_column(String(100), nullable=False)
    l2b_file = mapped_column(String(100), nullable=False)
    l4a_file = mapped_column(String(100), nullable=False)
    # TODO(amelia): how do deal with versions across products?
    # major_version = mapped_column(SmallInteger, nullable=False)
    created_date = mapped_column(DateTime, nullable=False)
