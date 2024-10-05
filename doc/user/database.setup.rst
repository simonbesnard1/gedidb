Setting Up a Secure and Scalable PostgreSQL Database with PostGIS for GEDI Data
===============================================================================

This guide provides advanced instructions for setting up a **PostgreSQL** database with **PostGIS** to handle large-scale geospatial GEDI data using **gediDB**. It covers robust security measures, connection management for over 100 users, and techniques to optimize performance for large datasets. This guide is intended for database administrators and developers who require a high level of security and scalability.

.. contents:: Table of Contents
   :depth: 3

Prerequisites
-------------

Before you begin, ensure that the following tools are installed on your system:

1. **PostgreSQL** (version 12 or higher): A powerful, open-source object-relational database system.
2. **PostGIS**: A spatial database extender for PostgreSQL, adding support for geographic objects.

For Ubuntu/Debian-based systems, run the following commands in your terminal:

.. code-block:: bash

   sudo apt update
   sudo apt install postgresql postgis

For RedHat/CentOS-based systems, run:

.. code-block:: bash

   sudo yum install postgresql postgis

Setting Up a Highly Secure and Scalable Database
------------------------------------------------

### 1. Create a PostgreSQL Database and Users with Appropriate Permissions

#### a. Admin User Setup

1. **Switch to the PostgreSQL user**:

   .. code-block:: bash

      sudo -i -u postgres

2. **Create a new PostgreSQL database**:

   .. code-block:: bash

      createdb gedi_db

3. **Create an admin user with full privileges**:

   .. code-block:: sql

      CREATE USER admin_user WITH PASSWORD 'your_secure_password';
      ALTER USER admin_user WITH SUPERUSER;  -- Grant full admin privileges

   *Note:* Replace `'your_secure_password'` with a strong, secure password.

#### b. Public Read-Only User Setup

1. **Create a public user with read-only access**:

   .. code-block:: sql

      CREATE USER public_readonly WITH PASSWORD 'readonly_secure_password';
      ALTER USER public_readonly SET default_transaction_read_only = true;

   *Note:* Replace `'readonly_secure_password'` with a strong, secure password.

2. **Grant read-only permissions**:

   Ensure that the public user can only **read** data and not modify it.

   .. code-block:: sql

      GRANT CONNECT ON DATABASE gedi_db TO public_readonly;
      GRANT USAGE ON SCHEMA public TO public_readonly;
      GRANT SELECT ON ALL TABLES IN SCHEMA public TO public_readonly;

3. **Set default privileges for future tables**:

   Automatically grant read-only access to any tables created in the future.

   .. code-block:: sql

      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO public_readonly;

### 2. Enabling PostGIS for Spatial Queries

1. **Connect to the `gedi_db` database**:

   .. code-block:: bash

      psql -d gedi_db -U admin_user

2. **Enable the PostGIS extension** for spatial capabilities:

   .. code-block:: sql

      CREATE EXTENSION IF NOT EXISTS postgis;

3. **Verify the PostGIS installation**:

   .. code-block:: sql

      SELECT PostGIS_Version();

### 3. Secure the Database

#### a. Enforce SSL/TLS Encryption

To ensure encrypted communication between users and the database:

1. **Enable SSL** in your `postgresql.conf` file:

   Locate the `postgresql.conf` file, typically found in `/etc/postgresql/<version>/main/` or `/var/lib/pgsql/data/`.

   .. code-block:: ini

      ssl = on
      ssl_cert_file = '/path/to/server.crt'
      ssl_key_file = '/path/to/server.key'

   *Note:* Replace `/path/to/server.crt` and `/path/to/server.key` with the paths to your SSL certificate and key files.

2. **Configure `pg_hba.conf` to require SSL connections**:

   Locate the `pg_hba.conf` file, usually in the same directory as `postgresql.conf`.

   Add the following line to enforce SSL connections:

   .. code-block:: ini

      hostssl   all   all   0.0.0.0/0   md5

   *Note:* Adjust the IP range (`0.0.0.0/0`) to match your network requirements.

#### b. Harden Authentication with SCRAM-SHA-256

Enable stronger password hashing by updating the authentication method:

1. **Modify `pg_hba.conf` to use SCRAM-SHA-256**:

   .. code-block:: ini

      host   all   all   0.0.0.0/0   scram-sha-256

2. **Set the password encryption method in `postgresql.conf`**:

   .. code-block:: ini

      password_encryption = scram-sha-256

3. **Reload the PostgreSQL configuration**:

   .. code-block:: bash

      sudo systemctl reload postgresql

*Note:* Existing users may need to reset their passwords to use the new encryption method.

#### c. Limit Connection Attempts and Use Connection Pooling

Prevent brute-force attacks and efficiently manage multiple connections:

1. **Set connection limits in `postgresql.conf`**:

   .. code-block:: ini

      max_connections = 500                # Total number of connections
      superuser_reserved_connections = 10  # Reserved for admin users

2. **Install and configure `pgBouncer` for connection pooling**:

   .. code-block:: bash

      sudo apt install pgbouncer

   Configure the `pgbouncer.ini` file, typically located at `/etc/pgbouncer/pgbouncer.ini`:

   .. code-block:: ini

      [databases]
      gedi_db = host=localhost port=5432 dbname=gedi_db

      [pgbouncer]
      listen_addr = *
      listen_port = 6432
      auth_type = md5
      auth_file = /etc/pgbouncer/userlist.txt
      pool_mode = session
      max_client_conn = 1000
      default_pool_size = 100

   Create the `userlist.txt` file with the users:

   .. code-block:: text

      "admin_user" "md5<md5_hash_of_password>"
      "public_readonly" "md5<md5_hash_of_password>"

   *Note:* Replace `<md5_hash_of_password>` with the actual MD5 hash of the user's password.

3. **Update client connection settings**:

   Clients should connect to `pgBouncer` instead of connecting directly to PostgreSQL. Update the connection parameters to use `port=6432`.

### 4. Partition Tables for Performance

To optimize reading efficiency and speed, partition the data based on geographic regions (e.g., 10x10-degree latitude/longitude tiles):

1. **Create a partitioned table** based on spatially derived attributes:

   Since PostgreSQL does not support partitioning directly on functions like `ST_X(geometry)`, create generated columns for longitude and latitude.

   .. code-block:: sql

      CREATE TABLE shots (
         shot_number BIGINT PRIMARY KEY,
         granule VARCHAR(60),
         version VARCHAR(60),
         beam_type VARCHAR(20),
         beam_name VARCHAR(9),
         geometry geometry(Point, 4326),
         longitude DOUBLE PRECISION GENERATED ALWAYS AS (ST_X(geometry)) STORED,
         latitude DOUBLE PRECISION GENERATED ALWAYS AS (ST_Y(geometry)) STORED
      ) PARTITION BY RANGE (longitude);

2. **Create partitions** based on longitude ranges (e.g., every 10 degrees):

   .. code-block:: sql

      CREATE TABLE shots_p1 PARTITION OF shots
      FOR VALUES FROM (-180) TO (-170);

      CREATE TABLE shots_p2 PARTITION OF shots
      FOR VALUES FROM (-170) TO (-160);

      -- Continue creating partitions covering the full longitude range

3. **Optionally, sub-partition by latitude**:

   If needed, further partition each longitude partition by latitude.

   .. code-block:: sql

      ALTER TABLE shots_p1 PARTITION BY RANGE (latitude);

      CREATE TABLE shots_p1_1 PARTITION OF shots_p1
      FOR VALUES FROM (-90) TO (-80);

      -- Continue for other latitude ranges

### 5. Monitor and Log Database Activity

Enable detailed logging in `postgresql.conf` to track activity:

.. code-block:: ini

   log_connections = on
   log_disconnections = on
   log_duration = on
   log_min_duration_statement = 1000  # Log statements longer than 1 second
   log_line_prefix = '%m [%p] %d %u %h '

This configuration allows you to monitor queries, connections, and potential issues in real-time without overwhelming the logs.

### 6. Regular Maintenance and Indexing

1. **Create Indexes on Spatial Columns**:

   .. code-block:: sql

      CREATE INDEX idx_shots_geometry ON shots USING GIST (geometry);

2. **Vacuum and Analyze Regularly**:

   Schedule regular maintenance tasks to optimize database performance.

   .. code-block:: bash

      vacuumdb -d gedi_db -U admin_user -z

   Or set up autovacuum in `postgresql.conf`:

   .. code-block:: ini

      autovacuum = on
      autovacuum_max_workers = 3

### Summary

- **Admin User (`admin_user`)**: Has full access to the database and can manage data.
- **Public Read-Only User (`public_readonly`)**: Can query the database without making changes.
- **Security Enhancements**: SSL/TLS encryption, SCRAM-SHA-256 password hashing, and connection pooling with `pgBouncer`.
- **Performance Optimizations**: Use partitioning, indexing, and regular maintenance to handle large datasets efficiently.
- **Monitoring**: Detailed logging to track database activity and performance.

