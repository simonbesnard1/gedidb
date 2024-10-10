Setting up PostgreSQL and PostGIS
=================================

This guide provides comprehensive instructions for setting up a **PostgreSQL** database with **PostGIS** to handle large-scale GEDI geospatial data using gediDB. It covers security measures, multi-user connection management, and optimization techniques for handling large datasets, particularly with high concurrency. The instructions are for database administrators and developers requiring security, scalability, and high-performance geospatial querying.

Prerequisites
-------------

Ensure the following tools are installed:

- **PostgreSQL** (version 12 or higher): An advanced, open-source object-relational database.
- **PostGIS**: A PostgreSQL extension that adds support for geographic objects.

**Installation**:

For Ubuntu/Debian-based systems:

.. code-block:: bash

   sudo apt update
   sudo apt install postgresql postgis

For RedHat/CentOS-based systems:

.. code-block:: bash

   sudo yum install postgresql postgis

Setting up a secure and scalable database
-----------------------------------------

Database and user roles
~~~~~~~~~~~~~~~~~~~~~~~

Creating dedicated user roles ensures appropriate permissions and secure access.

**Admin user setup**:

1. **Switch to the PostgreSQL user**:

   .. code-block:: bash

      sudo -i -u postgres

2. **Create the PostgreSQL database**:

   .. code-block:: bash

      createdb gedi_db
      psql -d gedi_db 

3. **Create an admin user with full privileges**:

   .. code-block:: sql

      CREATE USER admin_user WITH PASSWORD 'your_secure_password';
      ALTER USER admin_user WITH SUPERUSER;

   Replace `'your_secure_password'` with a strong password.

**Read-Only public user setup**:

1. **Create a public read-only user**:

   .. code-block:: sql

      CREATE USER public_readonly WITH PASSWORD 'readonly_secure_password';
      ALTER USER public_readonly SET default_transaction_read_only = true;

   Replace `'readonly_secure_password'` with a secure password.

2. **Grant read-only permissions**:

   .. code-block:: sql

      GRANT CONNECT ON DATABASE gedi_db TO public_readonly;
      GRANT USAGE ON SCHEMA public TO public_readonly;
      GRANT SELECT ON ALL TABLES IN SCHEMA public TO public_readonly;

3. **Set default privileges for future tables**:

   .. code-block:: sql

      ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO public_readonly;

Enabling PostGIS for spatial queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

1. **Connect to the `gedi_db` database**:

   .. code-block:: bash

      psql -d gedi_db -U admin_user

2. **Enable the PostGIS extension**:

   .. code-block:: sql

      CREATE EXTENSION IF NOT EXISTS postgis;

3. **Verify the installation**:

   .. code-block:: sql

      SELECT PostGIS_Version();

Securing the database
~~~~~~~~~~~~~~~~~~~~~

**SSL/TLS encryption**:

1. **Enable SSL in `postgresql.conf`**:

   .. code-block:: ini

      ssl = on
      ssl_cert_file = '/path/to/server.crt'
      ssl_key_file = '/path/to/server.key'

   Replace `/path/to/server.crt` and `/path/to/server.key` with the paths to your SSL certificate and key files.

2. **Require SSL in `pg_hba.conf`**:

   .. code-block:: ini

      hostssl all all <YOUR_NETWORK_OR_IP> md5

**Enhanced authentication with SCRAM-SHA-256**:

1. **Update `pg_hba.conf` to use SCRAM-SHA-256 authentication**:

   .. code-block:: ini

      host all all <YOUR_NETWORK_OR_IP> scram-sha-256

2. **Set password encryption in `postgresql.conf`**:

   .. code-block:: ini

      password_encryption = scram-sha-256

3. **Reload configuration**:

   .. code-block:: bash

      sudo systemctl reload postgresql

**Limit connections and use connection pooling**:

1. **Set connection limits** in `postgresql.conf`:

   .. code-block:: ini

      max_connections = 500
      superuser_reserved_connections = 10

2. **Install and configure `pgBouncer` for connection pooling**:

   .. code-block:: bash

      sudo apt install pgbouncer

   Configure `pgbouncer.ini`:

   .. code-block:: ini

      [databases]
      gedi_db = host=localhost port=5432 dbname=gedi_db

      [pgbouncer]
      listen_addr = *
      listen_port = 6432
      auth_type = md5
      auth_file = /etc/pgbouncer/userlist.txt
      pool_mode = transaction
      max_client_conn = 1000
      default_pool_size = 100

Database schema overview
------------------------

The applied schema includes:

- **Granule Table**: Stores high-level metadata for GEDI data files (granules).
- **Metadata Table**: Provides descriptive information about variables within GEDI data products.
- **Shot Table**: Core table containing detailed GEDI measurements (shots) with geospatial attributes (longitude, latitude, elevation, etc.).

Each table uses PostGIS spatial types for efficient geospatial queries and is optimized with multi-dimensional indexing.

Partitioning data for performance
---------------------------------

**Approach: zoning partitioning**

GEDI shot data will be divided into geographic zones based on latitude and longitude boundaries, with specific partitions for each hemisphere and climate zone. A trigger function will dynamically assign each incoming data point to the correct zone, automating the data management process.

**Define main table and partitions**

Create the `shots` table partitioned by **zone** to improve performance for spatial queries.

.. code-block:: sql

   -- Create the main shot table partitioned by zone
   CREATE TABLE IF NOT EXISTS [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]] (
       shot_number BIGINT,
       granule VARCHAR(60),
       version VARCHAR(60),
       beam_type VARCHAR(20),
       beam_name VARCHAR(9),
       geometry geometry(Point,4326),  
       zone VARCHAR(50),
       PRIMARY KEY (zone, shot_number)
   ) PARTITION BY LIST (zone);  -- Partition by zone

**Define partitions by zone**

Define partitions for each **zone**, and dynamically assign zones with a trigger function.

.. code-block:: sql

   -- Partition example for one zone
   CREATE TABLE public.shots_wh_north_polar PARTITION OF public.shots 
       FOR VALUES IN ('wh_north_polar');

**Indexing spatial partitions**

To optimize geospatial query performance, add **GIST spatial indexes** on each partition.

.. code-block:: sql

   -- Create spatial indexes for partitions
   CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_north_polar 
   ON public.shots_wh_north_polar USING GIST (geometry);

Performance optimization and maintenance
----------------------------------------

**Advanced connection pooling and query caching**

- Use `pgBouncer` in **transaction mode** for connection pooling to optimize high-concurrency usage.
- Consider `pgpool` for limited query caching to enhance performance for repeated queries.

**Automated maintenance tasks**:

1. **Reindex periodically**:

   .. code-block:: bash

      11 3 * * * psql "gedi_db" -c 'REINDEX DATABASE "gedi_db";'

2. **Vacuum and analyze**:

   .. code-block:: bash

      vacuumdb -d gedi_db -U admin_user -z

   Alternatively, enable autovacuum:

   .. code-block:: ini

      autovacuum = on
      autovacuum_max_workers = 3

3. **Incremental backups**:

   For faster backups, use:

   .. code-block:: bash

      pg_basebackup -D /path/to/backup -Ft -z

   Restore with:

   .. code-block:: bash

      pg_restore -d gedi_db /path/to/backup

Summary
-------

This setup guide provides a secure, optimized environment for handling GEDI data, including:

- **Role-based access control**: Detailed access control for secure management.
- **Security enhancements**: SSL/TLS, SCRAM-SHA-256 authentication, and connection pooling.
- **Performance optimization**: Multi-dimensional partitioning and scheduled maintenance.
- **Monitoring**: Activity tracking with pg_stat_statements for efficient database management.
