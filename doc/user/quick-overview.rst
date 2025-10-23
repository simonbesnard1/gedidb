.. _overview:

################
Quick Overview
################

This quick overview demonstrates how to query GEDI data in just a few minutes using **gediDB**. You'll connect to a global database and retrieve processed data without worrying about downloading granules, managing HDF5 files, or handling authentication.

.. note::
   **Install gediDB** (takes ~1 minute):
   
   .. code-block:: bash
   
      pip install gedidb
   
   Verify the installation:
   
   .. code-block:: bash
   
      python -c "import gedidb; print(gedidb.__version__)"
   
   For optional features and troubleshooting, see :ref:`installing`.

Quick Start: Query GEDI Data in Minutes
========================================

The fastest way to get started with **gediDB** is to query data from the publicly available global TileDB database hosted by the `Global Land Monitoring group <https://www.gfz.de/en/section/remote-sensing-and-geoinformatics/topics/global-land-monitoring>`_ at GFZ-Potsdam. This database contains all processed GEDI L2A, L2B, L4A, and L4C data (~20TB) and is optimized for fast spatial and temporal queries.

**Example: Query biomass and canopy height data for Zambia**

.. code-block:: python

    import gedidb as gdb
    
    # Connect to the global database (no credentials needed!)
    provider = gdb.GEDIProvider(
        storage_type='s3',
        s3_bucket="dog.gedidb.gedi-l2-l4-v002", 
        url="https://s3.gfz-potsdam.de"
    )
    
    # Define a region of interest (small area in Zambia)
    # You can use coordinates, bounding box, or load from a GeoJSON file
    from shapely.geometry import box
    roi = box(30.256, -15.853, 30.422, -15.625)  # (min_lon, min_lat, max_lon, max_lat)
    
    # Query biomass and canopy height data
    gedi_data = provider.get_data(
        variables=["agbd", "rh"],  # aboveground biomass density, relative height metrics
        query_type="bounding_box",
        geometry=roi,
        start_time="2019-01-01",
        end_time="2023-12-31",
        return_type='xarray'  # returns as xarray.Dataset
    )
    
    print(f"Retrieved {len(gedi_data.shot_number)} GEDI shots")
    print(f"Variables: {list(gedi_data.data_vars)}")

**What just happened?**

In just a few lines of code, you:

- Connected to a global database with ~20TB of processed GEDI data
- Queried 5 years of biomass and canopy height data for your region
- Retrieved the data as an **xarray Dataset** ready for analysis
- **No downloads, no HDF5 parsing, no NASA Earthdata credentials required**

Visualize Your Data
-------------------

Let's create a quick visualization of the retrieved data:

.. code-block:: python

    import matplotlib.pyplot as plt
    
    # Extract biomass values and coordinates
    agbd = gedi_data['agbd'].values
    lat = gedi_data['latitude'].values
    lon = gedi_data['longitude'].values
    
    # Create a scatter plot
    fig, ax = plt.subplots(figsize=(10, 8))
    scatter = ax.scatter(lon, lat, c=agbd, cmap='YlGn', s=1, vmin=0, vmax=200)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title('GEDI Aboveground Biomass Density (Mg/ha)')
    plt.colorbar(scatter, ax=ax, label='AGBD (Mg/ha)')
    plt.tight_layout()
    plt.show()
    
    # Print summary statistics
    print(f"Mean biomass: {agbd.mean():.2f} Mg/ha")
    print(f"Median biomass: {gedi_data['agbd'].median().values:.2f} Mg/ha")
    print(f"Max biomass: {agbd.max():.2f} Mg/ha")

Alternative: Using GeoJSON
--------------------------

You can also query using a GeoJSON file:

.. code-block:: python

    import geopandas as gpd
    
    # Load region of interest from GeoJSON
    region_of_interest = gpd.read_file('path/to/your_area.geojson')
    
    # Query with the same provider
    gedi_data = provider.get_data(
        variables=["agbd", "rh"],
        query_type="bounding_box",
        geometry=region_of_interest,
        start_time="2019-01-01",
        end_time="2023-12-31",
        return_type='xarray'
    )

Why Use gediDB?
===============

**gediDB** simplifies GEDI data access by handling the complexity for you:

- ✅ **No granule management**: Query by location and time, not by granule IDs
- ✅ **No HDF5 wrestling**: Data is pre-processed and stored in an optimized format
- ✅ **No authentication hassles**: The global database is publicly accessible
- ✅ **Fast queries**: Spatial consolidation makes regional queries much faster
- ✅ **Analysis-ready**: Returns data as xarray or pandas DataFrames

**Performance example**: Querying the region above returns ~4x more data in ~5 minutes compared to downloading, processing, and querying locally (~13 minutes).

When to Set Up Your Own Database
=================================

The global database at GFZ-Potsdam is perfect for most use cases. However, you might want to set up your own local **gediDB** instance if you need:

- **Custom quality filtering**: Apply specialized filters during processing
- **Repeated analysis**: Faster queries for your specific region of interest
- **Offline access**: Work without internet connectivity
- **Custom variables**: Store additional derived metrics
- **Private data**: Process proprietary or restricted datasets

To learn how to set up your own database, process GEDI data, and customize configurations, see:

- :ref:`fundamentals-setup` - Configuration files and setup
- :ref:`tiledb_database` - Details about the TileDB architecture
- :ref:`fundamentals` - Advanced features and detailed use cases

Available Variables
===================

The database includes a comprehensive set of GEDI variables across all products (L2A, L2B, L4A, L4C):

**Common variables**:

- ``agbd``: Aboveground biomass density (Mg/ha) - L4A
- ``rh``: Relative height metrics at 1% intervals (m) - L2A
- ``cover``: Total canopy cover (%) - L2B
- ``pai``: Plant Area Index (m²/m²) - L2B
- ``quality_flag``, ``l2_quality_flag``, ``l4_quality_flag``: Quality indicators

For the complete list of 100+ available variables with descriptions, units, and products, see :ref:`tiledb_database`.

Next Steps
==========

- Explore the :ref:`fundamentals` for advanced querying techniques
- Learn about :ref:`quality filtering <fundamentals-setup>` to refine your queries
- Check out the :ref:`tiledb_database` documentation for technical details
- Visit the `GitHub repository <https://github.com/simonbesnard1/gedidb>`_ for examples and issues