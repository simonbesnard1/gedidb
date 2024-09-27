.. for doctest:
    >>> import gedidb as gdb

.. _database.scheme:

###############
Database scheme
###############

Setting Up Database Schema
---------------------------

To work with the `gedidb` package, you must define the appropriate database schema. The schema file is provided in the `gedidb` package.

1. **Navigate to your schema file** (replace `path_to_schema` with the actual path to the `.sql` schema file):

.. code-block:: bash

    psql -d gedi_db -U gedi_user -f path_to_schema/db_schema.sql

This will execute the schema file and create the necessary tables and relationships required for GEDI data storage.
