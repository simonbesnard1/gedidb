.. for doctest:
    >>> import gedidb as gdb

.. _basics.setup:

#############
Configuration
#############

To effectively utilize the package, several key variables must be configured beforehand.
First, you'll need to establish the necessary parameters that dictate the package's functionality,
ensuring that they align with your specific requirements. All these are bundled in the ``data_config.yml`` file.

data_config.yml
---------------

The ``data_config.yml`` file serves as the central configuration hub for the script and database setup.
It contains all critical variables needed for smooth execution, including database connection details
(such as host, port, username, and password), file paths, environment settings, and other script-specific parameters.

Extracted data from .h5 files
-----------------------------

``.h5`` files often contain large volumes of data, but not all of it is necessary for every application.
In practice, only a subset of this data is typically useful or relevant to specific tasks.
The ``data_config.yml`` file defines exactly which portions of the data we choose to retain and process.

By reviewing the configuration settings, you can see the specific data fields and attributes that are kept for
each product, ensuring that only the essential information is used.
More advanced users can also feel free to edit the settings on their own, if needed.

This allows for more efficient data handling and analysis, while filtering out unnecessary details.

Below is a section of the product ``level_2a`` data:

::

    # Variables list for each GEDI level product - SDS_Name is science dataset name the native h5 file retrieved from the NASA API
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
        ...


Spatial and temporal parameters
-------------------------------

The user needs to define the spatial and temporal parameters used in the
`CMR query <basics.authenticate.html#cmr-login-credentials>`_ to retrieve granules.

::

  region_of_interest: './path/to/my_geojson.geojson'
  start_date: '2019-01-01'
  end_date: '2022-01-01'

The ``region_of_interest`` parameter expects the path to a ``.geojson`` file, which should hold a ``FeatureCollection``
of either a polygon or a multipolygon.

An example of a ``.geojson`` polygon:

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
                [
                  30.256673359035123,
                  -15.85375449790373
                ],
                [
                  30.422423359035125,
                  -15.85375449790373
                ],
                [
                  30.422423359035125,
                  -15.62525449790373
                ],
                [
                  30.256673359035123,
                  -15.62525449790373
                ],
                [
                  30.256673359035123,
                  -15.85375449790373
                ]
              ]
            ]
          }
        }
      ]
    }
