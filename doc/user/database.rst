.. for doctest:
    >>> import gedidb as gdb

.. _database:

#####################
Setting up a Database
#####################

This section guides you through the process of setting up a PostgreSQL database with PostGIS extensions, tailored specifically for working with GEDI data via the **gediDB** package. 

A solid understanding of database management is recommended before proceeding. If you are unfamiliar with PostGIS or PostgreSQL setup, the provided external resources will give you the necessary background to get started.

Overview
--------

The **gediDB** package integrates seamlessly with PostgreSQL/PostGIS databases, enabling you to store and query large-scale GEDI data efficiently. The database is the backbone of the GEDI data workflow, supporting geospatial queries, efficient storage of multi-dimensional datasets, and enabling data filtering based on both temporal and spatial parameters.

To get started, follow the steps outlined in the guide to:
- Set up a PostGIS-enabled PostgreSQL database.
- Configure the database schema for storing GEDI data.
- Connect **gediDB** to your database for data insertion and querying.

.. note::

   PostGIS provides geospatial capabilities to your PostgreSQL database, making it the perfect choice for handling GEDI's spatial data, such as forest structure, biomass, and canopy height.

Step-by-Step Instructions
-------------------------

Below are detailed steps to help you set up your own GEDI database.

.. toctree::
   :caption: Step-by-Step Guide
   :maxdepth: 1

   database.setup
   database.scheme

Each step includes commands, configurations, and practical advice for building a database environment optimized for storing GEDI data.

External Resources
------------------

For users needing more foundational information or additional resources on setting up PostgreSQL and PostGIS databases, we've provided links to external documentation and tutorials.

.. toctree::
   :caption: External Resources
   :maxdepth: 1

   database.external

---

Whether you’re an experienced database administrator or a newcomer to PostGIS, these guides and resources are designed to help you establish a scalable and efficient database infrastructure for working with GEDI data using **gediDB**.
