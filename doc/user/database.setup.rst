.. for doctest:
    >>> import gedidb as gdb

.. _database.setup:

#####################
Setting up PostgreSQL
#####################

This guide provides step-by-step instructions to help you set up a **PostgreSQL** database with **PostGIS**, which is essential for handling geospatial GEDI data using **gediDB**.

Prerequisites
-------------

Before starting, make sure you have the following installed on your system:

1. **PostgreSQL** (version 12 or higher): The powerful, open-source database system.
2. **PostGIS**: A spatial extension that adds geospatial capabilities to PostgreSQL, allowing you to store and query spatial data like GEDI shots and forest structure.

If these tools are not installed, you can use your system's package manager to install them.

For Ubuntu/Debian-based systems, run the following commands in your terminal:

.. code-block:: bash

    sudo apt update
    sudo apt install postgresql postgis

For RedHat/CentOS-based systems, run:

.. code-block:: bash

    sudo yum install postgresql postgis

Setting Up the Database
-----------------------

Once PostgreSQL and PostGIS are installed, follow the steps below to create your database:

1. **Switch to the PostgreSQL user**:

   PostgreSQL creates a default administrative user called `postgres` during installation. Switch to this user by running:

.. code-block:: bash

    sudo -i -u postgres

2. **Create a new PostgreSQL database**:

   Create your database (replace `gedi_db` with your preferred database name):

.. code-block:: bash

    createdb gedi_db

3. **Create a new user**:

   Create a dedicated user for the database (replace `gedi_user` with your desired username, and set a secure password):

.. code-block:: bash

    createuser --interactive --pwprompt gedi_user

   You will be prompted to assign a password and confirm other user options.

4. **Grant user privileges**:

   Give your new user full control over the database:

.. code-block:: bash

    psql -c "GRANT ALL PRIVILEGES ON DATABASE gedi_db TO gedi_user;"

At this point, your PostgreSQL database and user are ready. Now we need to enable **PostGIS** to handle geospatial data.

Enabling PostGIS
----------------

PostGIS is a vital extension for working with GEDI data, as it allows spatial queries and operations. Here’s how to enable it:

1. **Connect to the database**:

   Use the newly created user to connect to the database:

.. code-block:: bash

    psql -d gedi_db -U gedi_user

2. **Enable the PostGIS extension**:

   Once connected, enable the PostGIS extension by running the following SQL command:

.. code-block:: sql

    CREATE EXTENSION postgis;

   This command activates the spatial capabilities of the database.

3. **Verify PostGIS installation**:

   Confirm that PostGIS has been successfully enabled by checking the version:

.. code-block:: sql

    SELECT PostGIS_Version();

   The output will display the installed PostGIS version, indicating that the extension is active and ready.

What's Next?
------------

Now that your PostgreSQL database is set up with PostGIS enabled, it’s ready to be connected to **gediDB** for storing and querying GEDI data. You can proceed to the next step, which involves configuring the database schema and establishing a connection with **gediDB**.

If you encounter issues during the setup, ensure that PostgreSQL and PostGIS are correctly installed, and that your user has the necessary privileges to modify the database.

For further customization or troubleshooting, refer to the PostgreSQL and PostGIS official documentation.

