.. _tuning-config:

#################################
Configuring gediDB for custom use
#################################

This section provides guidance on modifying the **data configuration file** (`data_config.yml`). These files control how GEDI data is processed, stored, and queried, and can be customized to meet your project's unique requirements.

Why customize configuration?
----------------------------

Customizing the `data_config.yml` file allows you to:

- **Include new variables** or **exclude unnecessary ones** as data needs change.
- **Adjust spatial or temporal filters** to refine the region of interest.

Customizing the data configuration file
---------------------------------------

The `data_config.yml` file manages which variables are extracted from GEDI `.h5` files, sets data filtering criteria, and defines spatial and temporal parameters. This flexibility allows you to customize data processing to suit your research needs.

**Example: Adding new variables**

To add a new variable (e.g., "sensitivity") to the `L2A` product, open `data_config.yml` and locate the relevant section (in this case, `level_2a`). Then add the variable you want to extract:

.. code-block:: yaml

    level_2a:
      variables:
        shot_number:
          SDS_Name: "shot_number"
          ...
        beam_type:
          SDS_Name: "beam_type"
          ...
        sensitivity:                            # New variable added
          SDS_Name: "sensitivity"
          description: "Maxmimum canopy cover that can be penetrated"
          units: "adimensional"
          dtype: "float32"
          valid_range: "0, 1"
          product_level: 'L2A'

This configuration adds `sensitivity` to the variables processed from the `L2A` GEDI product.

**Example: Modifying spatial or temporal filters**

To change the region or time range for data extraction, update the `data_config.yml` file as follows:

.. code-block:: yaml

    region_of_interest: './path/to/new_region.geojson'  # Updated spatial area
    start_date: '2020-01-01'                            # Updated start date
    end_date: '2023-01-01'                              # Updated end date

Ensure that `region_of_interest` points to a valid `.geojson` file with the desired geographic boundaries.

By customizing these configuration files, you can adapt gediDB to handle a wide range of data needs while ensuring data consistency and efficiency.
