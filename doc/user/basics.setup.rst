.. _basics-setup:

*******************
Configuration files
*******************

To maximize the functionality of **gediDB**, it’s essential to configure key settings using the `data_config.yml` and `db_scheme.sql` files. These files specify important parameters, ensuring efficient data handling, database connection, and alignment with your processing needs.

=======================
Data configuration file
=======================

The `data_config.yml` file is the main configuration file for settings related to data retrieval, database connectivity, and file management. Key configurations include:

 - **Database Connection Details**: Define database connection variables like `host`, `port`, `username`, `password`, and `database name`.
 - **File Paths**: Specify directories for storing downloaded GEDI data, processed files, and metadata.
 - **Environment Settings**: Configure parameters for parallel processing and resource allocation.
 - **Data Extraction Settings**: Control which variables to extract from GEDI `.h5` files to streamline storage and improve processing efficiency.

A default data configuration file (`data_config.yml`) can be downloaded here:

:download:`Download data_config.yml <../_static/test_files/data_config.yml>`

**Extracted data from .h5 Files**

GEDI `.h5` files contain extensive data, but **gediDB** allows you to specify only the essential variables you need. This configuration not only reduces storage requirements but also speeds up data processing.

For instance, each GEDI product, like **Level 2A**, can have a dedicated configuration section, allowing tailored data extraction. Below is an example specifying selected variables for **Level 2A**:

.. code-block:: yaml

    level_2a:
      variables:
        shot_number:
          SDS_Name: "shot_number"
        beam_type:
          SDS_Name: "beam_type"
        beam_name:
          SDS_Name: "name"
        delta_time:
          SDS_Name: "delta_time"

**Spatial and Temporal Parameters**

Define **spatial** and **temporal** parameters to set boundaries for the data queries. These settings specify which GEDI granules to retrieve, based on the region and time range of interest.

.. code-block:: yaml

    region_of_interest: './path/to/my_geojson.geojson'
    start_date: '2019-01-01'
    end_date: '2022-01-01'

- **`region_of_interest`**: Path to a GeoJSON file defining the spatial area of interest, such as a polygon or multipolygon.
- **`start_date`** and **`end_date`**: Define the time range for data retrieval.

**Example GeoJSON polygon**

Here is an example of a GeoJSON polygon file that could be used for the `region_of_interest`:

.. code-block:: json

    {
      "type": "FeatureCollection",
      "features": [
        {
          "type": "Feature",
          "properties": {},
          "geometry": {
            "type": "Polygon",
            "coordinates": [
              [
                [30.256673359035123, -15.85375449790373],
                [30.422423359035125, -15.85375449790373],
                [30.422423359035125, -15.62525449790373],
                [30.256673359035123, -15.62525449790373],
                [30.256673359035123, -15.85375449790373]
              ]
            ]
          }
        }
      ]
    }

Download an example `test.geojson` file here:

:download:`Download test.geojson <../_static/test_files/test.geojson>`

====================
Database scheme file
====================

The database scheme file (`db_scheme.sql`) file defines the structure of key tables in the PostgreSQL database, tailored to efficiently store and retrieve GEDI data. The schema includes three main tables—`granule`, `shot`, and `metadata`—each with specific roles in organizing data, maintaining relationships, and enabling efficient querying.

Key configurations in the database scheme include:

 - **Table Definitions**: Structures tables for GEDI data storage, including main tables for granules, shots, and metadata.
 - **Primary Keys**: Establishes unique identifiers for each table, ensuring data integrity and quick retrieval.
 - **Foreign Key Relationships**: Links tables, such as associating shot records with corresponding granule entries, to preserve relational data structure.
 - **Indexes**: Adds indexes on commonly queried fields, like spatial coordinates and timestamps, to optimize query performance.

A sample `db_scheme.sql` file with recommended schema configurations can be downloaded here:

:download:`Download db_scheme.sql <../_static/test_files/db_scheme.sql>`

**Database table structure**

The tables are:

1. **Granule Table** (`{DEFAULT_SCHEMA}.{DEFAULT_GRANULE_TABLE}`)
   - This table maintains information about each granule, which is a discrete data unit in the GEDI dataset.
  
     - `granule_name`: (Primary Key) A unique identifier for each granule.
     - `created_date`: A timestamp indicating when the granule record was created.

   This table allows quick access to granule-level data and is useful for identifying the source of each shot record in the `shot` table.

2. **Shot Table** (`{DEFAULT_SCHEMA}.{DEFAULT_SHOT_TABLE}`)
   - The `shot` table is the core storage for GEDI data points (shots), where each shot represents a single laser measurement from the GEDI instrument. This table is structured to support a variety of scientific analyses on forest structure, biomass, and other ecological metrics.
   
     - `shot_number`: (Primary Key) A unique identifier for each GEDI shot.
     - `granule`: The associated granule name, linked to the `granule_name` in the `granule` table.
     - **Geolocation**: 
       - `lon_lowestmode`, `lat_lowestmode`, `lon_highestreturn`, `lat_highestreturn`: Coordinates of the lowest mode and highest return points.
       - `geometry`: Stores geolocation as a Point geometry with SRID 4326 for efficient spatial querying.
     - **Time & Versioning**: 
       - `absolute_time`: Timestamp of the GEDI measurement.
       - `version`: Version information for the GEDI dataset.
     - **Beam Information**:
       - `beam_type`, `beam_name`, `delta_time`: Beam type and timing details.
     - **Quality & Environmental Flags**:
       - `degrade_flag`, `leaf_off_flag`, `water_persistence`, `modis_nonvegetated`, and others: Quality and environmental indicators for filtering reliable data.
     - **Forest and Biomass Metrics**:
       - `agbd`, `pai`, `pavd_z`, `cover`, `cover_z`: Aboveground biomass density, plant area index, plant area volume density, and other forest structure metrics.
       - `rh`, `rh100`: Relative height metrics, useful for vegetation canopy studies.
       - `wsci`: Wood specific canopy index, useful in biomass and forest structure studies.
     - **Radiometric and Solar Information**:
       - `solar_elevation`, `solar_azimuth`: Solar angles for understanding lighting conditions during data capture.
     - **Sensitivity and Algorithm Information**:
       - `sensitivity`, `selected_algorithm`, `algorithmrun_flag`: Sensitivity measures and algorithm choices that impact data processing.

   This table’s extensive columns capture a wide range of GEDI metrics, enabling complex querying based on both geospatial and attribute data. Indexing on the geometry and commonly used fields can further enhance query performance.

3. **Metadata Table** (`{DEFAULT_SCHEMA}.{DEFAULT_METADATA_TABLE}`)
   - The `metadata` table contains descriptive information for each dataset variable, facilitating easier interpretation of data fields.
   
     - `SDS_Name`: (Primary Key) The variable name as it appears in GEDI data files.
     - `Description`: A textual description of the variable’s purpose.
     - `units`: Units of measurement for the variable.
     - `product`: The GEDI product associated with the variable (e.g., L2A, L2B).
     - `source_table`: Specifies the source table where the variable is located.
     - `created_date`: Timestamp indicating when the metadata entry was created.

**Example database schema**

Here’s an overview of the main tables and columns in `db_scheme.sql`:

.. code-block:: sql

    -- Granule Table
    CREATE TABLE IF NOT EXISTS DEFAULT_SCHEMA.DEFAULT_GRANULE_TABLE (
       granule_name VARCHAR(60) PRIMARY KEY,
       created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Shot Table
    CREATE TABLE IF NOT EXISTS DEFAULT_SCHEMA.DEFAULT_SHOT_TABLE (
        shot_number BIGINT PRIMARY KEY,
        granule VARCHAR(60),
        version VARCHAR(60),
        beam_type VARCHAR(20),
        beam_name VARCHAR(9),
        delta_time FLOAT,
        absolute_time TIMESTAMP,
        degrade_flag SMALLINT,
        ... -- other columns listed above
        geometry geometry(Point,4326)
    );

    -- Metadata Table
    CREATE TABLE IF NOT EXISTS DEFAULT_SCHEMA.DEFAULT_METADATA_TABLE (
        SDS_Name VARCHAR(255) PRIMARY KEY,
        Description TEXT,
        units VARCHAR(100),
        product VARCHAR(100),
        source_table VARCHAR(255),
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
