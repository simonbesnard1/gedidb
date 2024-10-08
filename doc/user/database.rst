#####################
Setting up a Database
#####################

This section provides an overview of setting up a PostgreSQL database with PostGIS extensions to manage and query GEDI data using the **gediDB** package. The setup process is divided into clear steps, from installing PostgreSQL and PostGIS to configuring a schema specifically tailored for GEDI data.

Overview
--------

The **gediDB** package integrates with PostgreSQL/PostGIS to store, filter, and analyze large-scale GEDI data efficiently. With PostGIS extensions, you can perform geospatial operations on GEDI data, enabling powerful spatial queries. The database acts as the core of the GEDI data pipeline, facilitating efficient data storage and retrieval based on temporal and spatial parameters.

To get started, follow the detailed steps provided in the following sections:

 - **Setting up PostgreSQL and PostGIS**: Includes installation instructions, creating the GEDI database, and enabling PostGIS for geospatial capabilities.
 - **Database schema configuration**: Provides a detailed structure of tables specifically designed for GEDI data storage.

Step-by-Step Guide
------------------

Below is a quick guide to each part of the database setup. Click on each link for in-depth instructions:

.. toctree::
   :caption: Step-by-Step Guide
   :maxdepth: 1

   database.setup

**Note**: Familiarity with PostgreSQL and SQL basics is recommended. If you are new to database management, see the external resources section for additional learning materials.

External Resources
------------------

For those needing more foundational knowledge on PostgreSQL and PostGIS, explore the resources below:

.. toctree::
   :caption: External Resources
   :maxdepth: 1

   database.external

---
