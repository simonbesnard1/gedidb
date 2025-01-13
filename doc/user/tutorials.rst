.. _tutorials:

****************
gediDB Tutorials
****************

The following tutorials provide practical, hands-on examples of how to use gediDB. Each notebook focuses on a core component of the package, covering workflows for downloading, processing, storing, and querying GEDI data.

.. toctree::
   :maxdepth: 1

Notebooks
---------

These tutorials are structured as Jupyter notebooks, providing interactive guides to understanding and using gediDB. Below is an overview of each tutorial available:

1. **Using `GEDIProcessor` for data processing**

   This notebook demonstrates how to use the :py:class:`gedidb.GEDIProcessor` class to manage the full GEDI data pipeline. By working through this tutorial, users will learn how to:

   - Configure essential settings in the `data_config.yml` file.
   - Download and preprocess GEDI granules for multiple products (L2A, L2B, L4A, and L4C).
   - Apply quality filters and merge data across products.
   - Store processed data in a [tile]DB database.

   This tutorial guides users step-by-step through the initialization, downloading, and storage process. By the end, users will have set up a database ready for analysis with structured, high-quality GEDI data.

   :download:`Download GEDIProcessor Tutorial Notebook <../_static/notebooks/gedi_processor_tutorial.ipynb>`

2. **Querying data with `GEDIProvider`**

   The second notebook focuses on the :py:class:`gedidb.GEDIProvider` class, which enables users to retrieve and analyze GEDI data stored in a PostgreSQL/PostGIS database. Key objectives of this tutorial include:

   - Connecting to the database using `GEDIProvider` and defining key settings.
   - Querying data with spatial, temporal, and variable-specific filters.
   - Retrieving data in various formats (`xarray.Dataset` and `pandas.DataFrame`) depending on analysis requirements.
   - Understanding and visualizing the structure of GEDI data, including dimensions, coordinates, and core variables like `rh`, `pavd_z`, and `pai`.

   This tutorial covers various query scenarios and highlights how to effectively extract the data you need for analysis, providing a robust foundation for leveraging GEDI data in research.

   :download:`Download GEDIProvider Tutorial Notebook <../_static/notebooks/gedi_provider_tutorial.ipynb>`

---

Each tutorial provides an interactive, practical approach to mastering GEDI data processing and retrieval, helping users get the most out of gediDB.
