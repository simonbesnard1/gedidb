.. _faq:

################################
Frequently Asked Questions (FAQ)
################################

How should I cite gediDB?
-------------------------

Please use the following citation when referencing gediDB in your work:

> Besnard, S., Dombrowski, F., & Holcomb, A. (2024). gediDB (2.0). Zenodo. https://doi.org/10.5281/zenodo.13885229

What are the main features of gediDB?
-------------------------------------

GediDB is a tileDB-based tool designed to manage, query, and analyze large-scale GEDI data. Its main features include:

- Efficient storage and querying of GEDI shot data with geospatial capabilities.
- Automated data loading and processing with support for GEDI L2A, L2B, L4A, and L4C products.
- Flexible filtering based on spatial, temporal, and quality parameters.

How do I set up the database for GEDI?
----------------------------------------

The tileDB database is set up automatically in code. If you want to use a local database, no additional steps are required by you.

What data products does gediDB support?
---------------------------------------

GediDB supports the following GEDI data products:

- **Level 2A**: Contains geolocated waveform data and relative height metrics.
- **Level 2B**: Includes vegetation canopy cover and vertical profile metrics.
- **Level 4A**: Provides aboveground biomass density estimates.
- **Level 4C**: Includes gridded biomass estimates at global scales.

Can I use the GEDI database on cloud-hosted databases?
------------------------------------------------------

Yes, the GEDI database can be deployed on cloud-hosted instances such as AWS S3.

How can I update the GEDI database to a newer version?
------------------------------------------------------

To update the GEDI database:

1. Backup your database to avoid data loss.
2. Apply any new schema changes by running the updated `db_scheme.sql`.
3. Review release notes for any changes that might require modifications to your database setup or data-loading workflows.

How do I write GEDI data into the database?
-------------------------------------------

You can load GEDI data into a GEDI database by using the :py:class:`gedidb.GEDIProcessor` class provided in the gediDB package. Configure the `data_config.yml` file with your data paths and database details, then run the processor to handle data download, processing, and insertion.

Where can I find more examples for using gediDB?
------------------------------------------------

Refer to the :ref:`tutorials` section in the documentation for example notebooks demonstrating how to use :py:class:`gedidb.GEDIProcessor` and  :py:class:`gedidb.GEDIProvider` classes for data processing and querying.

How do I contribute to gediDB development?
------------------------------------------

We welcome contributions to gediDB! You can contribute by:

- Reporting issues or suggesting features on our GitHub repository.
- Submitting pull requests for bug fixes, improvements, or new features.
- Providing documentation or usage examples.

For more details, please check the contributing guidelines :ref:`devindex`.
