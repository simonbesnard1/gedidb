.. _tuning-config:

#################################
Configuring gediDB for custom use
#################################

This section provides guidance on modifying both the **data configuration file** (`data_config.yml`) and the **database schema file** (`db_scheme.sql`). These files control how GEDI data is processed, stored, and queried, and can be customized to meet your project's unique requirements.

Why customize configuration?
----------------------------

Customizing the `data_config.yml` and `db_scheme.sql` files allows you to:

- **Include new variables** or **exclude unnecessary ones** as data needs change.
- **Adjust spatial or temporal filters** to refine the region of interest.
- **Optimize the database schema** to store additional data types or improve query efficiency.

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
        beam_type:
          SDS_Name: "beam_type"
        sensitivity:
          SDS_Name: "sensitivity"  # New variable added here

This configuration adds `sensitivity` to the variables processed from the `L2A` GEDI product.

**Example: Modifying spatial or temporal filters**

To change the region or time range for data extraction, update the `data_config.yml` file as follows:

.. code-block:: yaml

    region_of_interest: './path/to/new_region.geojson'  # Updated spatial area
    start_date: '2020-01-01'  # Updated start date
    end_date: '2023-01-01'    # Updated end date

Ensure that `region_of_interest` points to a valid `.geojson` file with the desired geographic boundaries.

**Advanced filtering and customization**

The `data_config.yml` file also supports advanced options, such as filtering by quality flags, beam types, and environmental parameters. Adjust these filters to fine-tune your dataset for specific analysis requirements.

You can download a sample configuration file here: :download:`Download data_config.yml <../_static/test_files/data_config.yml>`

Customizing the database schema
-------------------------------

The `db_scheme.sql` file defines the database structure for storing processed GEDI data. If you add new variables or change the data structure, update this file to ensure consistency between the data and schema.

**Example: Adding a new column**

If adding a new variable (e.g., "sensitivity"), add a corresponding column in the database schema. Open `db_scheme.sql` and locate the table where the data will be stored, such as `filtered_l2ab_l4ac_shots`.

Add the column:

.. code-block:: sql

    CREATE TABLE filtered_l2ab_l4ac_shots (
        shot_number BIGINT PRIMARY KEY,
        beam_name TEXT,
        sensitivity FLOAT,  -- New column for the "sensitivity" variable
        ...
    );

**Advanced schema customization**

The `db_scheme.sql` file allows for advanced customization, including:

- **Adding or modifying data types**: Define appropriate data types for each variable.
- **Creating additional tables**: Structure the database to accommodate other types of data if needed.
- **Establishing relationships between tables**: Set foreign keys or unique constraints for better data integrity.

Ensure any schema changes in `db_scheme.sql` align with the variables and structure defined in `data_config.yml`.

For a sample schema file, download here: :download:`Download db_scheme.sql <../_static/test_files/db_scheme.sql>`

---

By customizing these configuration files, you can adapt gediDB to handle a wide range of data needs while ensuring data consistency and efficiency.
