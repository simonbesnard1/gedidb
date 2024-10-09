.. _database-setup:

Setting up PostgreSQL and PostGIS
=================================

This guide provides comprehensive instructions for setting up a **PostgreSQL** database with **PostGIS** to handle large-scale GEDI geospatial data using gediDB. It covers robust security measures, multi-user connection management, and optimization techniques for handling large datasets. The instructions are intended for database administrators and developers requiring a high level of security and scalability.

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

Or for RedHat/CentOS-based systems:

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

PostGIS is essential for handling geospatial data.

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

**SSL/TLS Encryption**:

To ensure encrypted connections:

1. **Enable SSL in `postgresql.conf`**:

   Locate and update the `postgresql.conf` file, typically found in `/etc/postgresql/<version>/main/` or `/var/lib/pgsql/data/`.

   .. code-block:: ini

      ssl = on
      ssl_cert_file = '/path/to/server.crt'
      ssl_key_file = '/path/to/server.key'

   Replace paths with your SSL certificate and key files.

2. **Require SSL in `pg_hba.conf`**:

   In `pg_hba.conf`, add:

   .. code-block:: ini

      hostssl all all 0.0.0.0/0 md5

**Enhanced authentication with SCRAM-SHA-256**:

1. **Update `pg_hba.conf` to use SCRAM-SHA-256**:

   .. code-block:: ini

      host all all 0.0.0.0/0 scram-sha-256

2. **Set password encryption in `postgresql.conf`**:

   .. code-block:: ini

      password_encryption = scram-sha-256

3. **Reload the configuration**:

   .. code-block:: bash

      sudo systemctl reload postgresql

*Note*: Existing users may need to reset passwords.

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
      pool_mode = session
      max_client_conn = 1000
      default_pool_size = 100

Database schema overview
------------------------

The applied schema includes:

- **Granule Table**: Stores high-level metadata for GEDI data files (granules), including identifiers, status, and timestamps.
- **Metadata Table**: Provides descriptive information about variables within GEDI data products, such as units and descriptions.
- **Shot Table**: Core table containing detailed GEDI measurements (shots) with metadata, quality flags, and geospatial attributes (longitude, latitude, elevation, etc.).

Each table uses PostGIS spatial types, allowing efficient geospatial queries, and is optimized for performance with indexing and partitioning.

You can download the schema file, if not already present:

:download:`Download db_scheme.sql <../_static/test_files/db_scheme.sql>`

Then, you canapply the schema to set up the required tables and relationships:

.. code-block:: bash

  psql -d gedi_db -U admin_user -f path_to_schema/db_scheme.sql

This will create tables to store GEDI shots, spatial data, and relevant metadata using PostGIS geometry types for optimized geospatial querying.


Performance optimization
------------------------

Partitioning data for performance
---------------------------------

To efficiently manage large GEDI datasets, we use partitioning based on geographic zones, optimizing read and query performance. Partitioning by **zone** groups data into predefined geographic areas, enhancing data locality and retrieval speed. 

**Approach: Zoning partitioning**

GEDI shot data will be divided into geographic zones based on latitude and longitude boundaries, with specific partitions for each hemisphere and climate zone. A trigger function will dynamically assign each incoming data point to the correct zone, automating the data management process.

**Define the main table and partitions**

Create the `shots` table as a parent table partitioned by the `zone` attribute. The `zone` field will be determined by latitude and longitude, and each partition will store data from a specific geographic area.

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

**Define geographic zones**

Use a function and trigger to automatically assign each shot to its respective zone based on latitude and longitude. This function categorizes data into zones, like `wh_north_polar`, `wh_tropical`, and `eh_south_temperate`, based on spatial criteria.

.. code-block:: sql

   -- Function to calculate zone based on longitude and latitude
   CREATE OR REPLACE FUNCTION [[DEFAULT_SCHEMA]].calculate_zone()
   RETURNS trigger AS '
   BEGIN
       IF NEW.lon_lowestmode >= -180 AND NEW.lon_lowestmode < 0 THEN
           -- Western Hemisphere
           IF NEW.lat_lowestmode >= 60 AND NEW.lat_lowestmode <= 90 THEN
               NEW.zone := ''wh_north_polar'';
           ELSIF NEW.lat_lowestmode >= 30 AND NEW.lat_lowestmode < 60 THEN
               NEW.zone := ''wh_north_temperate'';
           ELSIF NEW.lat_lowestmode >= 0 AND NEW.lat_lowestmode < 30 THEN
               NEW.zone := ''wh_tropical'';
           -- Additional zone assignments continue here
           ELSE
               RAISE EXCEPTION ''Invalid lat_lowestmode for Western Hemisphere: %'', NEW.lat_lowestmode;
           END IF;
       -- Additional longitude and latitude conditions continue here
       END IF;
       RETURN NEW;
   END;
   ' LANGUAGE plpgsql;

   -- Trigger to invoke calculate_zone function
   CREATE TRIGGER calculate_zone_trigger
   BEFORE INSERT OR UPDATE ON [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]
   FOR EACH ROW EXECUTE FUNCTION [[DEFAULT_SCHEMA]].calculate_zone();

**Create partitions by zone**

Define partitions for each zone, which are automatically assigned by the trigger function. This setup allows the database to manage data efficiently based on geographic regions.

.. code-block:: sql

   -- Zone: wh_north_polar
   CREATE TABLE IF NOT EXISTS [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]_wh_north_polar
   PARTITION OF [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]
   FOR VALUES IN ('wh_north_polar');

   -- Additional zones
   CREATE TABLE IF NOT EXISTS [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]_wh_north_temperate
   PARTITION OF [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]
   FOR VALUES IN ('wh_north_temperate');
   
   -- Continue creating partitions for each defined zone...

**Indexing spatial partitions**

To enhance geospatial query performance, create spatial indexes on each partition. The `GIST` index type supports geospatial data, improving search speed within each geographic zone.

.. code-block:: sql

   -- Create spatial indexes for partitions
   CREATE INDEX IF NOT EXISTS idx_shot_geometry_wh_north_polar 
   ON [[DEFAULT_SCHEMA]].[[DEFAULT_SHOT_TABLE]]_wh_north_polar USING GIST (geometry);

   -- Continue creating indexes for each partition...


Monitoring and logging
~~~~~~~~~~~~~~~~~~~~~~

Enable detailed logging in `postgresql.conf` to track activity:

.. code-block:: ini

   log_connections = on
   log_disconnections = on
   log_duration = on
   log_min_duration_statement = 1000
   log_line_prefix = '%m [%p] %d %u %h '

Database Maintenance
--------------------

**Automated maintenance tasks**:

Set up regular database maintenance to optimize performance. Add the following `cron` jobs for tasks like reindexing and backup.

1. **Reindex** database periodically to optimize storage and access:

   .. code-block:: bash

      crontab -e
      # Add reindexing jobs
      11 3 * * * psql "gedi_db" -c 'REINDEX DATABASE "gedi_db";'

2. **Vacuum and Analyze**:

   Schedule regular maintenance tasks to optimize performance:

   .. code-block:: bash

      vacuumdb -d gedi_db -U admin_user -z

   Alternatively, set up autovacuum in `postgresql.conf`:

      .. code-block:: ini

         autovacuum = on
         autovacuum_max_workers = 3

3. **Backup database**:

   .. code-block:: bash

      pg_dumpall | gzip -c > /path/to/backup/adsc-postgres.sql.gz

   To restore, use:

      .. code-block:: bash

         gunzip -c /path/to/backup/adsc-postgres.sql.gz | psql -U admin_user gedi_db

Summary
-------

This setup guide provides a secure, optimized environment for handling GEDI data, including:

- **User roles**: Separate access levels for secure management.
- **Security enhancements**: SSL/TLS, SCRAM-SHA-256 authentication, and connection pooling.
- **Performance optimization**: Partitioning and scheduled maintenance.
- **Monitoring**: Activity tracking for improved management.

--- 

