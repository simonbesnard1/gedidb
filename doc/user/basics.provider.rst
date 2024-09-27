.. for doctest:
    >>> import gedidb as gdb

.. _basics.provider:

#############
Data provider
#############

The data provider module is crucial for retrieving and delivering this structured metadata from the PostGIS database.
It allows the system to query the spatial and non-spatial data stored in the database,
efficiently fetching relevant variables.
By interfacing with the PostGIS database, the module ensures that this data is accurately retrieved,
formatted, and made available for further processing or analysis,
supporting spatial queries and handling complex geospatial information.

Default data to retrieve
------------------------

The provided list contains the default values and their associated properties that are stored in the database.
Each entry includes a description of the variable, its unit of measurement (where applicable),
and the dataset level (L2A, L2B, L4A and L4C) it pertains to.

These variables encompass various attributes, ranging from spatial coordinates and elevation data to vegetation
and biomass metrics, as well as quality flags and algorithm-related information.
Additionally, this metadata, which defines the variables and their characteristics,
is stored in its own dedicated table within the database,
ensuring that users have access to detailed information about the dataset structure and contents.

.. csv-table::
   :header: "Variable Name", "Description", "Units", "Product"
   :widths: 20, 50, 15, 10

   "shot_number", "Shot number", "N/A", "L2A"
   "delta_time", "Time delta since Jan 1 00:00 2018", "Seconds", "L2A"
   "degrade_flag", "Flag indicating degraded state of pointing and/or positioning information", "N/A", "L2A"
   "leaf_off_flag", "GEDI 1 km EASE 2.0 grid flag", "N/A", "L2A"
   "modis_nonvegetated", "Percent non-vegetated from MODIS MOD44B V6 data", "Percent", "L2A"
   "modis_treecover", "Percent tree cover from MODIS MOD44B V6 data", "Percent", "L2A"
   "solar_elevation", "Solar elevation", "N/A", "L2A"
   "solar_azimuth", "Solar azimuth", "N/A", "L2A"
   "energy_total", "Integrated counts in the return waveform relative to the mean noise level", "Number", "L2A"
   "selected_algorithm", "Identifier of algorithm selected as identifying the lowest non-noise mode", "N/A", "L2A"
   "pft_class", "GEDI 1 km EASE 2.0 grid Plant Functional Type (PFT)", "N/A", "L2A"
   "region_class", "GEDI 1 km EASE 2.0 grid world continental regions", "N/A", "L2A"
   "urban_proportion", "The percentage proportion of land area within a focal area surrounding each shot that is urban land cover.", "Select Units", "L2A"
   "landsat_water_persistence", "Percent UMD GLAD Landsat observations with classified surface water", "N/A", "L2A"
   "quality_flag", "Flag simplifying selection of most useful data", "Quality Flag", "L2A"
   "stale_return_flag", "Flag indicating return signal above detection threshold was not detected", "Class Flag", "L2A"
   "surface_flag", "Indicates elev_lowestmode is within 300 m of DEM or MSS", "N/A", "L2A"
   "selected_mode", "Identifier of mode selected as lowest non-noise mode", "N/A", "L2A"
   "digital_elevation_model_srtm", "STRM elevation at GEDI footprint location", "Meters", "L2A"
   "sensitivity", "Maxmimum canopy cover that can be penetrated", "N/A", "L2A"
   "lon_lowestmode", "Longitude of center of lowest mode", "Degree", "L2A"
   "longitude_bin0_error", "Error on longitude_bin0", "Degree", "L2A"
   "lat_lowestmode", "Latitude of center of lowest mode", "Degree", "L2A"
   "latitude_bin0_error", "Error in latitude of bin 0", "Degree", "L2A"
   "elev_lowestmode", "Elevation of center of lowest mode relative to reference ellipsoid", "Meters", "L2A"
   "elevation_bin0_error", "Error in elevation of bin 0", "Meters", "L2A"
   "lon_highestreturn", "Longitude of highest detected return", "Degree", "L2A"
   "lat_highestreturn", "Latitude of highest detected return", "Degree", "L2A"
   "elev_highestreturn", "Elevation of highest detected return relative to reference ellipsoid", "Meters", "L2A"
   "digital_elevation_model", "TanDEM-X elevation at GEDI footprint location", "Meters", "L2A"
   "leaf_on_doy", "GEDI 1 km EASE 2.0 grid leaf-on start day-of-year", "N/A", "L2A"
   "leaf_on_cycle", "Flag that indicates the vegetation growing cycle for leaf-on observations", "N/A", "L2A"
   "rh", "Relative height metrics at 1% interval", "Meters", "L2A"
   "num_detectedmodes", "Number of detected modes in rxwaveform", "N/A", "L2A"
   "l2a_quality_flag", "L2A quality flag", "Quality Flag", "L2B"
   "l2b_quality_flag", "L2B quality flag", "Quality Flag", "L2B"
   "algorithmrun_flag", "The L2B algorithm is run if this flag is set to 1 indicating data have sufficient waveform fidelity for L2B to run", "N/A", "L2B"
   "selected_l2a_algorithm", "Selected L2A algorithm setting", "N/A", "L2B"
   "selected_rg_algorithm", "Selected R (ground) algorithm", "N/A", "L2B"
   "dz", "Vertical step size of foliage profile", "Meters", "L2B"
   "longitude_bin0", "Longitude of first bin of the pgap_theta_z, interpolated from L1B waveform coordinate", "Degree", "L2B"
   "latitude_bin0", "Latitude of first bin of the pgap_theta_z, interpolated from L1B waveform coordinate", "Degree", "L2B"
   "elevation_bin0", "Elevation of first bin of the pgap_theta_z, interpolated from L1B waveform coordinate", "Meters", "L2B"
   "rh100", "Height above ground of the received waveform signal start (rh[101] from L2A)", "cm", "L2B"
   "cover", "Total canopy cover", "Percent", "L2B"
   "cover_z", "Cumulative canopy cover vertical profile", "Percent", "L2B"
   "fhd_normal", "Foliage Height Diversity", "N/A", "L2B"
   "omega", "Foliage Clumping Index", "N/A", "L2B"
   "pai", "Total Plant Area Index", "m²/m²", "L2B"
   "pai_z", "Plant Area Index profile", "m²/m²", "L2B"
   "pavd_z", "Plant Area Volume Density profile", "m²/m³", "L2B"
   "pgap_theta", "Total Gap Probability (theta)", "N/A", "L2B"
   "pgap_theta_error", "Total Pgap (theta) error", "N/A", "L2B"
   "rg", "Integral of the ground component in the RX waveform for the selected L2A processing version", "Number", "L2B"
   "rhog", "Volumetric scattering coefficient (rho) of the ground", "Number", "L2B"
   "rhog_error", "Rho (ground) error", "Number", "L2B"
   "rhov", "Volumetric scattering coefficient (rho) of the canopy", "Number", "L2B"
   "rhov_error", "Rho (canopy) error", "Number", "L2B"
   "rossg", "Ross-G function", "N/A", "L2B"
   "rv", "Integral of the vegetation component in the RX waveform for the selected L2A processing version", "Number", "L2B"
   "rx_range_highestreturn", "Range to signal start", "Meters", "L2B"
   "l2_quality_flag", "Flag identifying the most useful L2 data for biomass predictions", "-", "L4A"
   "l4_quality_flag", "Flag simplifying selection of most useful biomass predictions", "-", "L4A"
   "algorithm_run_flag", "The L4A algorithm is run if this flag is set to 1. This flag selects data that have sufficient waveform fidelity for AGBD estimation.", "-", "L4A"
   "predictor_limit_flag", "Prediction stratum identifier. Predictor value is outside the bounds of the training data (0=in bounds; 1=lower bound; 2=upper bound)", "-", "L4A"
   "response_limit_flag", "Prediction value is outside the bounds of the training data (0=in bounds; 1=lower bound; 2=upper bound)", "-", "L4A"
   "agbd_t_se", "Model prediction standard error in fit units (needed for calculation of custom prediction intervals)", "-", "L4A"
   "agbd", "Aboveground biomass density (Mg/ha)", "Mg/ha", "L4A"
   "agbd_pi_lower", "Lower prediction interval (see alpha attribute for the level)", "Mg/ha", "L4A"
   "agbd_pi_upper", "Upper prediction interval (see alpha attribute for the level)", "Mg/ha", "L4A"
   "agbd_se", "Aboveground biomass density (Mg/ha) prediction standard error", "Mg/ha", "L4A"
   "agbd_t", "Model prediction in fit units", "-", "L4A"
   "wsci", "Waveform Structural Complexity Index", "FLOAT32MT", "L4C"
   "wsci_pi_lower", "Waveform Structural Complexity Index lower prediction interval", "FLOAT32MT", "L4C"
   "wsci_pi_upper", "Waveform Structural Complexity Index upper prediction interval", "FLOAT32MT", "L4C"

Reading GEDI data from the database
-----------------------------------

This pre-written code can be used and adapted. ::

    from gedidb.providers.gedi_provider import GEDIProvider

    #%% Instantiate the GEDIProvider
    provider = GEDIProvider(
        config_file='./config_files/data_config.yml',
        table_name='filtered_l2ab_l4ac_shots'
    )

    #%% Define the columns to query and additional parameters
    vars_selected = ['rh', 'pavd_z', 'pai']
    dataset = provider.get_dataset(
        variables=vars_selected, geometry=None,
        start_time='2018-01-01', end_time='2023-12-31',
        limit=100, force=True, order_by=['-shot_number'],
        return_type='xarray'
    )



The method ``.get_dataset()`` can be used to retrieve GEDI data from the database.
the ``return_type`` can either be a
`xarray.DataArray <https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html#xarray.DataArray>`_
or a `pandas.Dataframe <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_.
