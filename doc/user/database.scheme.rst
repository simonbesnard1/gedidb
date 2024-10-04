.. for doctest:
    >>> import gedidb as gdb

.. _database.scheme:

###############
Database Schema
###############

Setting Up the Database Schema
------------------------------

To work with the **gediDB** package, you need to define a database schema that creates the necessary tables and relationships for storing and querying GEDI data. The schema file for this setup is provided as part of the **gediDB** package.

### Downloading the Schema

You can download the database schema SQL file here:

:download:`Download db_scheme.sql <../_static/test_files/db_scheme.sql>`

Applying the Schema
###################

Once the `db_scheme.sql` file is downloaded, follow these steps to execute the schema on your PostgreSQL database:

1. **Access the database**:

   Use your PostgreSQL user credentials to connect to the **gedi_db** database that you created earlier. If you're not already connected, you can do so by running:

.. code-block:: bash

    psql -d gedi_db -U gedi_user

2. **Apply the schema**:

   To set up the tables and relationships, execute the schema SQL file (replace `path_to_schema` with the actual path to where you downloaded the `db_scheme.sql` file):

.. code-block:: bash

    psql -d gedi_db -U gedi_user -f path_to_schema/db_scheme.sql

This will create the tables, indexes, and any necessary relationships for storing GEDI L2A, L2B, L4A, and L4C products in your PostgreSQL database.

What the Schema Does?
#####################

Executing the schema file will create a set of tables and relationships required for:

- **Storing GEDI shots**: Each shot is stored with relevant metadata like timestamp, geolocation, and various sensor measurements.
- **Handling spatial data**: The schema includes PostGIS spatial data types to enable efficient querying of the GEDI shots based on their geospatial properties.
- **Ensuring data consistency**: Foreign key relationships between tables ensure that the data is linked correctly and queries can join across multiple tables efficiently.

Next Steps
##########

Once the schema has been successfully applied, your database is ready to receive and store GEDI data. You can now use the `gedidb` package to load GEDI data into the database, query it using spatial and temporal filters, and perform advanced geospatial analyses.

If you encounter any issues while applying the schema, ensure that your user has the correct privileges to create tables and that the database connection is correctly configured.
