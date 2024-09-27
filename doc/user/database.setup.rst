.. for doctest:
    >>> import gedidb as gdb

.. _database.setup:

###################
PostgreSQL database
###################

This guide will walk you through the steps required to set up a PostgreSQL database with PostGIS, which is essential for working with GEDI data in `gedidb`.

Prerequisites
-------------

Before proceeding, ensure that the following tools are installed on your system:

1. **PostgreSQL** (version 12 or higher)
2. **PostGIS** (a spatial extension for PostgreSQL)

If not installed, you can install these packages using your system’s package manager.

For Ubuntu/Debian-based systems, run:

.. code-block:: bash

    sudo apt update
    sudo apt install postgresql postgis

For RedHat/CentOS-based systems, run:

.. code-block:: bash

    sudo yum install postgresql postgis

Create a PostgreSQL Database
----------------------------

Once PostgreSQL and PostGIS are installed, you need to create a new PostgreSQL database.

1. **Switch to the `postgres` user** (this user is created by default during PostgreSQL installation):

.. code-block:: bash

    sudo -i -u postgres

2. **Create a new database** (replace `gedi_db` with your preferred database name):

.. code-block:: bash

    createdb gedi_db

3. **Create a user** (replace `gedi_user` with your desired username, and `your_password` with a secure password):

.. code-block:: bash

    createuser --interactive --pwprompt gedi_user

4. **Grant privileges** to the newly created user:

.. code-block:: bash

    psql -c "GRANT ALL PRIVILEGES ON DATABASE gedi_db TO gedi_user;"

Enable PostGIS Extension
------------------------

Once the database is created, you need to enable PostGIS to allow spatial queries. Follow these steps:

1. **Connect to the database**:

.. code-block:: bash

    psql -d gedi_db -U gedi_user

2. **Enable PostGIS extension**:

.. code-block:: sql

    CREATE EXTENSION postgis;

You can verify that the PostGIS extension is enabled by running:

.. code-block:: sql

    SELECT PostGIS_Version();

Conclusion
----------

You should now have a fully functional PostgreSQL database with PostGIS enabled and ready for use with `gedidb`. If you encounter any issues, ensure that PostgreSQL and PostGIS are correctly installed and that your database user has the appropriate privileges.


