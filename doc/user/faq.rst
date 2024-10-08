.. _faq:

################################
Frequently Asked Questions (FAQ)
################################

How should I cite gediDB?
-------------------------

Please use the following citation when referencing **gediDB** in your work:

> Besnard, S., Dombrowski, F., & Holcomb, A. (2024). gediDB (2.0). Zenodo. https://doi.org/10.5281/zenodo.13885229

What are the main features of gediDB?
-------------------------------------

**gediDB** is a PostgreSQL/PostGIS-based system designed to manage, query, and analyze large-scale GEDI data. Its main features include:
- Efficient storage and querying of GEDI shot data with geospatial capabilities.
- Automated data loading and processing with support for GEDI L2A, L2B, L4A, and L4C products.
- Flexible filtering based on spatial, temporal, and quality parameters.

Which versions of PostgreSQL and PostGIS are supported?
-------------------------------------------------------

**gediDB** is compatible with PostgreSQL version 12 or higher and requires the PostGIS extension for geospatial operations. We recommend using the latest stable versions of both PostgreSQL and PostGIS to ensure full compatibility and performance optimization.

How do I set up the database for gediDB?
----------------------------------------

To set up the **gediDB** database:
1. Install PostgreSQL and PostGIS.
2. Follow the instructions in the setup guide to create the necessary tables and enable PostGIS.
3. Download and apply the schema file (`db_scheme.sql`) provided with **gediDB** to configure the database structure.

For more detailed instructions, refer to the :ref:`database setup <database-setup>` section.

Can I use gediDB with other data besides GEDI?
----------------------------------------------

**gediDB** is specifically optimized for GEDI data structure and geospatial requirements. While it’s possible to adapt the database schema and functionality for other data, significant modifications would be required. For best performance, use **gediDB** with GEDI-specific datasets.

What data products does gediDB support?
---------------------------------------

**gediDB** supports the following GEDI data products:
- **Level 2A**: Contains geolocated waveform data and relative height metrics.
- **Level 2B**: Includes vegetation canopy cover and vertical profile metrics.
- **Level 4A**: Provides aboveground biomass density estimates.
- **Level 4C**: Includes gridded biomass estimates at global scales.

What permissions are needed to use gediDB?
------------------------------------------

To load, query, and manage data in **gediDB**, you need access to the PostgreSQL database with appropriate permissions:
- **Admin user**: Full privileges, including schema modification and data insertion.
- **Read-only user**: Limited to querying data for analysis without modification rights.

How can I optimize gediDB performance?
--------------------------------------

For optimal performance:
1. **Partition data** by geographic zones to speed up spatial queries.
2. **Enable SSL** for secure connections and **use SCRAM-SHA-256** for authentication.
3. **Install pgBouncer** for efficient connection pooling.
4. **Regularly vacuum and analyze** tables to maintain database efficiency.

Can I use gediDB on cloud-hosted databases?
-------------------------------------------

Yes, **gediDB** can be deployed on cloud-hosted PostgreSQL instances with PostGIS support, such as Amazon RDS, Google Cloud SQL, or Microsoft Azure. Ensure the cloud database service supports PostGIS and has adequate resources to handle large GEDI datasets.

How do I troubleshoot common errors with gediDB?
------------------------------------------------

Some common issues and solutions:
- **Connection errors**: Verify your database credentials and ensure that the PostgreSQL service is running.
- **Schema errors**: Ensure you have applied the latest `db_scheme.sql` and that your user has permissions to create tables.
- **Performance issues**: Check that indexing and partitioning are correctly set up and that regular maintenance (e.g., vacuum, analyze) is performed.

How can I update gediDB to a newer version?
-------------------------------------------

To update **gediDB**:
1. Backup your database to avoid data loss.
2. Apply any new schema changes by running the updated `db_scheme.sql`.
3. Review release notes for any changes that might require modifications to your database setup or data-loading workflows.

How do I load GEDI data into gediDB?
------------------------------------

You can load GEDI data into **gediDB** by using the `GEDIProcessor` class provided in the **gediDB** package. Configure the `data_config.yml` file with your data paths and database details, then run the processor to handle data download, processing, and insertion.

Can I query gediDB data from external applications?
---------------------------------------------------

Yes, you can access **gediDB** data from external applications that support PostgreSQL connections. Use the connection details provided by your **gediDB** setup to query the data, ensuring that the application has the necessary permissions.

Where can I find more examples for using gediDB?
------------------------------------------------

Refer to the :ref:`tutorials <basics.provider>` section in the documentation for example notebooks demonstrating how to use **gediDB**’s `GEDIProcessor` and `GEDIProvider` classes for data processing and querying.

How do I contribute to gediDB development?
------------------------------------------

We welcome contributions to **gediDB**! You can contribute by:
- Reporting issues or suggesting features on our GitHub repository.
- Submitting pull requests for bug fixes, improvements, or new features.
- Providing documentation or usage examples.

For more details, please check the contributing guidelines in the **gediDB** repository.
