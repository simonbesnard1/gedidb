.. for doctest:
    >>> import gedidb as gdb

.. _tuning-config:

#############################
Modifying configuration files
#############################

This section will guide you through the steps needed to modify both the **data configuration file** (`data_config.yml`) and the **database schema file** (`db_scheme.sql`). These two files control how GEDI data is processed, stored, and queried.

Why Modify?
-----------
As your project evolves, you may need to adapt the settings in `data_config.yml` to include new variables or exclude unnecessary ones. Similarly, the database schema (`db_scheme.sql`) may need to be extended to accommodate additional data types or to optimize queries.

Modifying the Data Configuration File
-------------------------------------
The `data_config.yml` file is the central hub for controlling which variables are extracted from GEDI `.h5` files, how data is filtered, and the spatial/temporal parameters. It is designed to be flexible so that you can tailor it to your research needs.

### Example: Adding New Variables

To add a new variable (e.g., "sensitivity") to be processed from the `L2A` product, open the `data_config.yml` file and locate the section for `level_2a`. Then add a new entry for the variable you want to extract:

.. code-block:: yaml

    level_2a:
      variables:
        shot_number:
          SDS_Name: "shot_number"
        beam_type:
          SDS_Name: "beam_type"
        sensitivity:
          SDS_Name: "sensitivity"  # New variable added here

In this example, the `sensitivity` variable from the `L2A` product will now be processed and stored in the database.

### Example: Modifying Spatial or Temporal Parameters

If you want to change the region of interest or the date range for the data, modify the relevant section of the `data_config.yml` file:

.. code-block:: yaml

    region_of_interest: './path/to/new_region.geojson'  # New spatial area
    start_date: '2020-01-01'  # New start date
    end_date: '2023-01-01'    # New end date

Make sure the `region_of_interest` points to a valid `.geojson` file containing the desired polygon or multipolygon.

Advanced Customization
######################

The data configuration file can also include more advanced settings like filtering for quality flags, beam types, or environmental conditions. You can modify or add to the existing filters to better fit your analysis.

For more information, you can download the template configuration file: :download:`Download data_config.yml <../_static/test_files/data_config.yml>`

Modifying the Database Schema File
----------------------------------
The `db_scheme.sql` file defines how the processed GEDI data will be stored in your PostgreSQL/PostGIS database. If you're adding new variables or changing the structure of the data, you will likely need to modify this file as well.

### Example: Adding a New Column

If you are adding a new variable (e.g., "sensitivity") to the database, you'll need to add a new column to the corresponding table in the schema. Open `db_scheme.sql` and locate the table where the data will be stored, such as `filtered_l2ab_l4ac_shots`.

Add a new column for the variable:

.. code-block:: sql

    CREATE TABLE filtered_l2ab_l4ac_shots (
        shot_number BIGINT PRIMARY KEY,
        beam_name TEXT,
        sensitivity FLOAT,  -- New column added here
        ...
    );

After modifying the schema, run the updated SQL file to apply the changes to your database:

.. code-block:: bash

    psql -d gedi_db -U gedi_user -f path_to_schema/db_scheme.sql


Advanced Schema Customization
#############################

In addition to adding columns, you can also modify data types, create new tables, or adjust relationships between existing tables. Just make sure that any changes to the schema reflect the modifications made in the `data_config.yml` file.

For more details, you can download the template database schema file: :download:`Download db_scheme.sql <../_static/test_files/db_scheme.sql>`



