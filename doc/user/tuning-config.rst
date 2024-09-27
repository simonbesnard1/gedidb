.. for doctest:
    >>> import gedidb as gdb

.. _tuning-config:

#####################
Data configurattion
#####################

The getting started guide aims to get you using the GEDI toolbox as quickly as possible.
It is designed as an entry point for new users.

Modifying the data configuration file
-------------------------------------

The data_config.yml file serves as the central configuration hub for the script and database setup.
It contains all critical variables needed for smooth execution, including database connection details
(such as host, port, username, and password), file paths, environment settings, and other script-specific parameters.

Modifying the database scheme file
----------------------------------

The package automatically creates, writes and reads data to and from [PostGIS](https://postgis.net/) tables.
The database itself needs to be provided by the user. For a guide on how to set up a spatial database,
look [here](https://postgis.net/workshops/postgis-intro/creating_db.html).
The user needs to replace the default values for the database url. The table and schema names are also predefined
and can be edited if needed. ::

  database:
    url: 'postgresql://your_username:your_password@server'
    tables:
      shots: 'filtered_l2ab_l4ac_shots'
      granules: 'gedi_granules'
      metadata: 'variable_metadata'
    schema: 'public'