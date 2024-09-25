
Overview: Why gediDB?
=====================

**gediDB** is a scalable Python package that provides a robust framework for interacting with, querying, and processing **GEDI (Global Ecosystem Dynamics Investigation)** data. gediDB introduces an easy-to-use interface for querying data from PostgreSQL databases, performing geospatial and temporal operations, and integrating with modern data science workflows. 

What gediDB enables
-------------------

Handling and processing GEDI data can be complex due to its multidimensional nature and geospatial components. gediDB addresses these challenges by offering a high-level API to access and manage GEDI data efficiently. It leverages common Python data science libraries, such as **GeoPandas**, **xarray**, and **Dask**, to simplify workflows and scale processing.

With gediDB, users can:

- **Query GEDI data by name or time**: Retrieve data by specifying variables, spatial regions, and time ranges.
- **Leverage PostgreSQL and PostGIS for geospatial querying**: gediDB enables users to work with GEDI data stored in a spatially-enabled PostgreSQL database, utilizing SQL queries and spatial indexing.
- **Scale processing with Dask**: Distribute the workload across multiple processes to handle large-scale GEDI datasets.
- **Join and merge geospatial data**: Effortlessly combine multiple GEDI products (such as Level 2A, 2B, and 4A) and work with unified datasets.

By offering a streamlined interface to GEDI data stored in relational databases, gediDB ensures that the complexity of working with raw HDF5 data is abstracted, allowing scientists and data analysts to focus on their analyses.

The GEDI data model
-------------------

GEDI data is highly multi-dimensional, involving various sensor measurements across time, space, and height. Working with this data can be tedious and error-prone if handled directly as raw numbers. gediDB simplifies this process by aligning these data dimensions and offering intuitive APIs for accessing specific data.

For example:

- **Apply operations over time or space**: Filter GEDI data based on spatial coordinates or a specific time window.
- **Join multiple data products**: Combine different levels of GEDI products into a single dataset for comprehensive analysis.
- **Leverage spatial joins and overlays**: Query data based on geospatial boundaries, such as forests, national parks, or user-defined polygons.

Core data structures
--------------------

gediDB introduces two main components for working with GEDI data:

1. **`GEDIProvider`**: The high-level interface to query and retrieve GEDI data from a PostgreSQL database, returning data in Pandas or Xarray formats. It allows users to define which variables to query, filter by geometry, and specify a time range.
   
2. **`GEDIDataBuilder`**: The core connector that interfaces with the PostgreSQL database, executes SQL queries, and fetches both the data and metadata required for analysis. This ensures that the query results are correctly aligned with the metadata associated with each GEDI product.

These components allow users to seamlessly access GEDI data while preserving the underlying structure and relationships between datasets.

Goals and aspirations
---------------------

gediDB aims to provide an efficient, scalable, and user-friendly platform for working with GEDI data. It is designed to meet the needs of researchers and analysts working in:

- **Geosciences**: To analyze forest structure, biomass, and canopy data from GEDI missions.
- **Remote Sensing**: To leverage multi-dimensional GEDI data in combination with other remote sensing products.
- **Data Science and Machine Learning**: To integrate GEDI data into scalable pipelines for further analysis, modeling, and predictions.

By focusing on building a strong foundation for querying and processing GEDI data, gediDB aspires to be a go-to solution for researchers working with large-scale ecosystem dynamics datasets.

gediDB is a collaborative project, and we welcome contributions from the community. Initially, the project was started by **Amelia Holcomb** during her PhD, with contributions from the **GLM team** to transform the original code into a scalable, production-ready Python package. We continue to actively develop gediDB, ensuring it evolves to meet the needs of researchers across various domains.
