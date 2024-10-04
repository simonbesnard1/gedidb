.. _basics.setup:

#############
Configuration
#############

To make the most out of **gediDB**, it's important to configure key variables before starting. The core configuration is bundled in the ``data_config.yml`` file, which specifies essential parameters for the package's functionality, ensuring that your data and processing align with your requirements.

Data configuration file
-----------------------

The data configuration file (``data_config.yml``) is the central hub for all necessary settings related to data retrieval, database connection, and file management. It contains essential variables like:

- **Database connection details**: host, port, username, password, and database name.
- **File paths**: locations where downloaded GEDI data, processed files, and metadata will be stored.
- **Environment settings**: settings that manage parallel processing and resource allocation.
- **Data extraction settings**: controls which variables to extract from GEDI ``.h5`` files.

A default `data_config.yml` file can be downloaded here:

:download:`Download data_config.yml <../_static/test_files/data_config.yml>`

Extracted data from .h5 files
-----------------------------

GEDI ``.h5`` files contain large amounts of data, but for most use cases, only a subset is relevant. The ``data_config.yml`` file allows you to specify exactly which variables to extract and process, ensuring efficient data handling and avoiding unnecessary storage of irrelevant data.

Each GEDI product, such as **Level 2A**, can have its own configuration section. This enables you to customize data extraction based on your research needs. Below is an example of how to specify data variables for **Level 2A**:

::

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

By carefully selecting only the necessary variables in the ``data_config.yml`` file, you ensure optimal storage efficiency and faster processing.

Spatial and temporal parameters
-------------------------------

Another crucial configuration involves defining **spatial** and **temporal** parameters for querying the data. These parameters determine which granules are retrieved based on the region and time period of interest.

::

  region_of_interest: './path/to/my_geojson.geojson'
  start_date: '2019-01-01'
  end_date: '2022-01-01'

- **`region_of_interest`**: Path to a GeoJSON file that defines the spatial area of interest (e.g., a polygon or multipolygon).
- **`start_date`** and **`end_date`**: Specify the time range over which GEDI data should be retrieved.

Example GeoJSON Polygon
-----------------------

Here is an example of a GeoJSON polygon that could be used for the ``region_of_interest``:

::

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

An example `test.geojson` file can be downloaded here:

:download:`Download test.geojson <../_static/test_files/test.geojson>`


Efficient data handling
-----------------------

By properly configuring the ``data_config.yml`` file, you ensure that **gediDB** processes GEDI data with optimal performance. Whether you need to filter data based on spatial regions, temporal ranges, or specific variables, this configuration step is key to making your workflow efficient and scalable.
