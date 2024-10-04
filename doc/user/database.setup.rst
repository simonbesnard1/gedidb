.. for doctest:
    >>> import gedidb as gdb

.. _database.setup:

#####################
Setting up PostgreSQL
#####################

This guide provides advanced instructions to set up a **PostgreSQL** database with **PostGIS** for handling large-scale geospatial GEDI data using **gediDB**. It includes robust security measures, connection management for over 100 users, and techniques to optimize performance for large datasets.

Prerequisites
-------------

Before starting, make sure the following tools are installed on your system:

1. **PostgreSQL** (version 12 or higher): The powerful, open-source database system.
2. **PostGIS**: A spatial extension that adds geospatial capabilities to PostgreSQL, allowing you to store and query spatial data like GEDI shots and forest structure.

For Ubuntu/Debian-based systems, run the following commands in your terminal:

.. code-block:: bash

    sudo apt update
    sudo apt install postgresql postgis

For RedHat/CentOS-based systems, run:

.. code-block:: bash

    sudo yum install postgresql postgis

Setting Up a Highly Secure and Scalable Database
------------------------------------------------

### 1. Create a PostgreSQL Database and User with Limited Privileges

1. **Switch to the PostgreSQL user**:

.. code-block:: bash

    sudo -i -u postgres

2. **Create a new PostgreSQL database**:

.. code-block:: bash

    createdb gedi_db

3. **Create a user with limited permissions**:

It is essential to create a user for database access that has read-only access, ensuring no user can modify the data.

.. code-block:: bash

    createuser gedi_readonly_user --pwprompt

4. **Grant limited permissions to the user**:

This ensures that users can only read data but not modify, delete, or insert data.

.. code-block:: bash

    psql -c "GRANT CONNECT ON DATABASE gedi_db TO gedi_readonly_user;"
    psql -c "GRANT SELECT ON ALL TABLES IN SCHEMA public TO gedi_readonly_user;"

You can also configure the user’s permissions to be applied automatically for any future tables created:

.. code-block:: bash

    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO gedi_readonly_user;

### 2. Enabling PostGIS for Spatial Queries

1. **Enable the PostGIS extension** for spatial capabilities:

.. code-block:: sql

    CREATE EXTENSION postgis;

2. **Verify the PostGIS installation**:

.. code-block:: sql

    SELECT PostGIS_Version();

### 3. Configure Database Security

For a public-facing database, you need strong security measures to prevent unauthorized access and SQL injections.

#### a. Enforce SSL/TLS Encryption

To encrypt communication between the database and users:

1. Enable SSL in your `postgresql.conf` file:

.. code-block:: bash

    ssl = on
    ssl_cert_file = '/path/to/server.crt'
    ssl_key_file = '/path/to/server.key'

2. Ensure clients connect using SSL by configuring your `pg_hba.conf` file:

.. code-block:: bash

    hostssl all all 0.0.0.0/0 md5

#### b. Harden Authentication with SCRAM-SHA-256

Enable SCRAM-SHA-256 for password hashing by updating the `pg_hba.conf` file:

.. code-block:: bash

    host all all 0.0.0.0/0 scram-sha-256

This method provides stronger password security compared to MD5 hashing.

#### c. Set Up Connection Rate Limiting

Limit the number of connection attempts to prevent brute-force attacks:

- Configure connection limits in `postgresql.conf`:

.. code-block:: bash

    max_connections = 500  # Total number of connections
    superuser_reserved_connections = 10  # Reserved for admin users

- Use connection pooling tools like **pgBouncer** to manage hundreds of simultaneous connections efficiently:

.. code-block:: bash

    sudo apt install pgbouncer

Configure the `pgbouncer.ini` file to manage user connections:

.. code-block:: ini

    [databases]
    gedi_db = host=localhost dbname=gedi_db

    [pgbouncer]
    listen_addr = *
    listen_port = 6432
    auth_type = md5
    pool_mode = session
    max_client_conn = 1000
    default_pool_size = 100

### 4. Partition Tables for Performance Optimization

Partition your tables by geographic regions (10x10-degree tiles) to improve query performance when working with large geospatial datasets.

1. **Create a partitioned table** based on the geometry (longitude-latitude):

.. code-block:: sql

    CREATE TABLE shots (
        shot_number BIGINT PRIMARY KEY,
        granule VARCHAR(60),
        version VARCHAR(60),
        beam_type VARCHAR(20),
        beam_name VARCHAR(9),
        geometry geometry(Point, 4326)
    ) PARTITION BY RANGE (ST_X(geometry), ST_Y(geometry));

2. **Create partitions** for each geographic region:

.. code-block:: sql

    CREATE TABLE shots_partition_1 PARTITION OF shots
    FOR VALUES FROM (0, 0) TO (10, 10);

    CREATE TABLE shots_partition_2 PARTITION OF shots
    FOR VALUES FROM (10, 10) TO (20, 20);

    -- Add more partitions as needed
