.. for doctest:
    >>> import gedidb as gdb

.. _fundamentals-filters:

#################
Quality Filtering
#################

This guide details the quality filtering applied to GEDI Level 2A (L2A), Level 2B (L2B), and Level 4A (L4A) data in the gediDB package. Each data product uses specific filters to ensure only high-quality data is processed, enhancing data reliability in analysis.

Overview
--------

Quality filters in gediDB are applied sequentially to various GEDI data products during the merging process, ensuring consistent and high-quality data across products. These filters are automatically applied by default, targeting specific criteria to remove data points with potential quality issues based on flags, thresholds, and classifications.

Filtering and product merging
-----------------------------

The filters are applied sequentially to each data product (L2A, L2B, L4A, and L4C) as they are merged. This ensures that only data meeting high-quality standards across products is retained in the final dataset. The merging process uses `shot_number` as the key to join each product’s data into a cohesive GeoDataFrame.

Each product’s data is automatically filtered by the respective beam class (`L2ABeam` and `L2BBeam`) to meet quality standards before merging. The merging employs an **inner join** on `shot_number`, so only records with matches across all required products are included in the final dataset. If a `shot_number` is missing in any product, that record is excluded, ensuring high data integrity and consistency in the output.


L2A Product Quality Filtering
-----------------------------

The `L2ABeam` class applies several filters to ensure data quality for Level 2A data:

 - **`quality_flag`**: Retains data where `quality_flag` is 1, ensuring high-quality measurements.
 - **`sensitivity_a0`**: Keeps data with `sensitivity` between 0.9 and 1.0.
 - **`sensitivity_a2`**: Selects data with `sensitivity_a2` between 0.95 and 1.0.
 - **`degrade_flag`**: Excludes data with specific `degrade_flag` values that indicate degraded data.
 - **`surface_flag`**: Retains data with `surface_flag` set to 1, indicating reliable surface measurements.
 - **`elevation_difference_tdx`**: Retains data where the difference between `elev_lowestmode` and `digital_elevation_model` is within -150 to 150 meters.

L2B Product Quality Filtering
-----------------------------

For Level 2B data, the `L2BBeam` class applies the following filters:

 - **`water_persistence`**: Keeps data with `landsat_water_persistence` below 10.
 - **`urban_proportion`**: Excludes data where `urban_proportion` exceeds 50.
