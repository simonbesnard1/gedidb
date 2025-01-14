.. _database:

#####################
Setting up a Database
#####################

This section provides an overview of setting up a PostgreSQL database with PostGIS extensions to manage and query GEDI data using the gediDB package. The setup process is divided into clear steps, from installing PostgreSQL and PostGIS to configuring a schema specifically tailored for GEDI data.

Overview
--------

The gediDB package integrates with PostgreSQL/PostGIS to store, filter, and analyze large-scale GEDI data efficiently. With PostGIS extensions, you can perform geospatial operations on GEDI data, enabling powerful spatial queries. The database acts as the core of the GEDI data pipeline, facilitating efficient data storage and retrieval based on temporal and spatial parameters.

To get started, follow the detailed steps provided in the following sections:

 - **External resources**: Provides external foundational knowledge on PostgreSQL and PostGIS.

.. toctree::
   :caption: Step-by-step guide
   :maxdepth: 1

   database.external

.. note::

   Familiarity with [tile]DB is recommended. If you are new to database management, see the external resources section for additional learning materials.

---
