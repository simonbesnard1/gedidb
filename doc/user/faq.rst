.. _faq:

################################
Frequently Asked Questions (FAQ)
################################

How should I cite gediDB?
-------------------------

Please use the following citation when referencing gediDB in your work:

> Besnard, S., Dombrowski, F., & Holcomb, A. (2025). gediDB (2025.2.0). Zenodo. https://doi.org/10.5281/zenodo.13885229

What are the main features of gediDB?
-------------------------------------

GediDB is a TileDB-based Python package designed to efficiently manage, query, and analyze large-scale GEDI data. Its main features include:

- **Efficient data storage**: Stores GEDI data using TileDB arrays, enabling optimized access and scalability.
- **Geospatial querying**: Provides spatially enabled querying for regions of interest with support for bounding boxes and polygons.
- **Automated processing**: Facilitates loading, pre-filtering, and processing of GEDI L2A, L2B, L4A, and L4C data products.
- **Parallelized operations**: Leverages parallel engines (e.g., Dask) for distributed data processing, enhancing performance on large datasets.
- **Integration with Python libraries**: Outputs data in formats compatible with pandas, geopandas, and xarray for seamless analysis.

How do I set up the database for GEDI?
--------------------------------------

The TileDB database is set up automatically using the `gediDB` package. By default, it creates and manages the schema required for GEDI data. If you prefer to use a pre-existing database, ensure that the structure aligns with the schema defined by `gediDB`.

What data products does gediDB support?
---------------------------------------

GediDB supports the following GEDI data products:

- **Level 2A**: Geolocated waveform data and relative height metrics.
- **Level 2B**: Vegetation canopy cover and vertical profile metrics.
- **Level 4A**: Aboveground biomass density estimates.
- **Level 4C**: Gridded biomass estimates at global scales.

Can I use the GEDI database on cloud-hosted databases?
------------------------------------------------------

Yes, the GEDI database can be deployed on cloud-hosted storage systems like AWS S3. Use TileDB’s integration with cloud platforms to store and access GEDI data seamlessly. Refer to the cloud storage documentation in TileDB for setup instructions.

Can I add data to my GEDI database?
-----------------------------------

Yes, you can add data to an existing GEDI database using `gediDB`. Simply configure the database with the appropriate schema and use the :py:class:`gedidb.GEDIProcessor` class to process and ingest new data. Make sure to backup your database before making modifications.

How do I write GEDI data into the database?
-------------------------------------------

GEDI data can be written to the database using the :py:class:`gedidb.GEDIProcessor` class. Steps include:

1. Configure the `data_config.yml` file with paths to your GEDI HDF5 files and database settings.
2. Use the :py:class:`gedidb.GEDIProcessor` to process and insert data into the TileDB database.
3. Monitor logs for any errors or warnings during processing.

How do I contribute to gediDB development?
------------------------------------------

We welcome contributions to gediDB! Here’s how you can help:

- **Report issues**: If you encounter bugs or have suggestions, report them on our GitHub issue tracker.
- **Submit pull requests**: Contribute code for bug fixes, new features, or performance improvements.
- **Improve documentation**: Help expand the documentation by providing additional examples or clarifications.

For detailed contribution guidelines, please check the :ref:`devindex`. Additionally, join discussions on our GitHub repository to engage with the development community.

