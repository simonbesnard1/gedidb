.. _whygedidb:

Overview: Why gediDB?
=====================

**gediDB** is a scalable Python package built to simplify working with **GEDI (Global Ecosystem Dynamics Investigation)** data. It offers an intuitive modules for processing, querying, and analyzing GEDI data stored in **PostgreSQL** databases.

The motivation behind gediDB
----------------------------

Working with GEDI data in its raw HDF5 format can be challenging due to:

 - **Complex data structure**: GEDI files are organized by orbit, making it inefficient for users interested in specific regions.
 - **High redundancy**: Users often need only a few metrics from across different products for each footprint, yet each HDF5 file contains extensive redundant information, leading to excessive disk and network load.
 - **Filter challenges**: The GEDI research community has identified common initial filters that reduce data size by up to 30%, yet these filters are not reflected in the raw HDF5 files.

**gediDB** was designed to address these issues by providing an efficient, pre-filtered database system that combines GEDI L2A+B and L4A+C products. Over the past year, this approach has been tested on the Cambridge computer cluster with a database of around 9.5 TB, covering tropical moist forests and Europe from 2019–2023. This collaboration with GFZ aims to expand and offer a public version of **gediDB** with Python tools that streamline access to the entire GEDI catalog.

What gediDB enables
-------------------

By overcoming GEDI’s high dimensionality and spatial complexities, **gediDB** offers powerful capabilities that simplify data access and analysis, including:

 - **Efficient, region-specific querying**: Quickly filter data by regions, variables, and time intervals for targeted analysis.
 - **Advanced geospatial querying**: Harness **PostgreSQL** and **PostGIS** for spatially enabled, SQL-based data retrieval within specified boundaries.
 - **Distributed processing**: Leverage **Dask** to parallelize and scale data processing, ensuring large-scale GEDI datasets are handled efficiently.
 - **Unified GEDI products**: Easily combine data from multiple GEDI levels (e.g., Levels 2A, 2B, and 4A) into a single dataset, enabling more comprehensive analysis.

By abstracting the complexity of raw GEDI HDF5 files, **gediDB** empowers researchers to focus on their scientific objectives without data management bottlenecks.

GEDI data structure and gediDB’s solution
-----------------------------------------

GEDI’s multi-dimensional data—spanning time, space, and height—presents unique challenges in processing and interpretation. **gediDB** simplifies these complexities by aligning data dimensions and providing intuitive modules for accessing and manipulating data. Users can:

 - **Filter GEDI data by time and space**: Retrieve data within specified geographic or temporal ranges.
 - **Merge and unify GEDI products**: Integrate multiple GEDI products for smooth, consolidated analyses.
 - **Perform spatial operations**: Execute custom spatial queries based on user-defined boundaries.

Core components of gediDB
-------------------------

**gediDB**'s two primary modules facilitate data processing and access:

1. :py:class:`gedidb.GEDIDProcessor`: This component manages data processing tasks, ensuring efficient handling and integrity across large GEDI datasets.

2. :py:class:`gedidb.GEDIProvider`: The high-level module for querying GEDI data stored in PostgreSQL. It retrieves data as **Pandas** DataFrames or **xarray** Datasets, enabling users to specify variables, apply spatial filters, and set time ranges.

These modules provide structured access to GEDI data, preserving relationships and metadata between datasets for comprehensive analysis.

Goals and aspirations
---------------------

**gediDB**'s primary objective is to create an efficient, scalable platform that meets the needs of various research fields, including:

 - **Geosciences**: Facilitating research on forest structure, canopy height, and biomass.
 - **Remote Sensing**: Enabling cross-referencing GEDI data with other remote sensing products for ecosystem studies.
 - **Data Science & Machine Learning**: Supporting developers in integrating GEDI data into data pipelines for modeling and large-scale analyses.

With a robust foundation for querying and processing GEDI data, **gediDB** aims to be a useful tool for conducting analyses of ecosystem dynamics as inferred from GEDI observations.

---

A Collaborative project
=======================

**gediDB** began as a research tool during **Amelia Holcomb**'s PhD at the University of Cambridge and evolved into a Python package through collaboration with **Simon Besnard** and **Felix Dombrowski** from the `Global Land Monitoring Group <https://www.gfz-potsdam.de/en/section/remote-sensing-and-geoinformatics/topics/global-land-monitoring>`_ at the Helmholtz Center Potsdam GFZ German Research Centre for Geosciences. This transition to a production-ready tool was driven by the need to handle large datasets and complex queries effectively. The project remains open-source and welcomes contributions from the research community to support its growth and adaptability.

---
