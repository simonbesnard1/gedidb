.. for doctest:
    >>> import gedidb as gdb

.. _fundamentals-filters:

#################
Quality Filtering
#################

This guide details the quality filtering applied to GEDI Level 2A (L2A), Level 2B (L2B), and Level 4A (L4A) data in the gediDB package. Each data product uses specific filters to ensure only high-quality data is processed, enhancing data reliability in analysis. The default quality filtering is based on the `data processing pipelines <https://docs.google.com/document/d/1XmcoV8-k-8C_Tmh-CJ4sYvlvOqkbiXP1Kah_KrCkMqU/edit>`_ developed by Patrick Burns, Chris Hakkenberg, and Scott Goetz, and should be appropriately credited. Please cite the paper data `Repeat GEDI footprints measure the effects of tropical forest disturbances <https://www.sciencedirect.com/science/article/pii/S0034425724001925?via%3Dihub#f0035>`_ if you end up using the default filters.

Overview
--------

Quality filters in gediDB are applied sequentially to various GEDI data products during the merging process, ensuring consistent and high-quality data across products. These filters are automatically applied by default, targeting specific criteria to remove data points with potential quality issues based on flags, thresholds, and classifications.

Filtering and product merging
-----------------------------

The filters are applied sequentially to each data product (L2A, L2B, L4A, and L4C) as they are merged. This ensures that only data meeting high-quality standards across products is retained in the final dataset. The merging process uses `shot_number` as the key to join each product’s data into a cohesive GeoDataFrame.

Each product’s data is automatically filtered by the respective beam class (`L2ABeam`, `L2BBeam`, and `L4ABeam`) to meet quality standards before merging. The merging employs an **inner join** on `shot_number`, so only records with matches across all required products are included in the final dataset. If a `shot_number` is missing in any product, that record is excluded, ensuring high data integrity and consistency in the output.


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

 - **`l2a_quality_flag`**: Ensures data has `l2a_quality_flag` set to 1.
 - **`l2b_quality_flag`**: Keeps data where `l2b_quality_flag` is 1.
 - **`sensitivity`**: Includes data with `sensitivity` between 0.9 and 1.0.
 - **`rh100`**: Retains data where `rh100` values are between 0 and 1200.
 - **`water_persistence`**: Keeps data with `landsat_water_persistence` below 10.
 - **`urban_proportion`**: Excludes data where `urban_proportion` exceeds 50.

L4A Product Quality Filtering
-----------------------------

In Level 4A data, the `L4ABeam` class uses the following filters for precise biomass estimates:

 - **`l2_quality_flag`**: Filters for data with `l2_quality_flag` set to 1.
 - **`sensitivity_a0`**: Includes data with `sensitivity` between 0.9 and 1.0.
 - **`sensitivity_a2`**: Retains data where `sensitivity_a2` is between 0.9 and 1.0.
 - **`pft_sensitivity_filter`**: Applies different `sensitivity_a2` thresholds based on `pft_class`. For **tropical evergreen broadleaf forests** (`pft_class == 2`), `sensitivity_a2` must be greater than 0.98. For other `pft_class` values, `sensitivity_a2` must exceed 0.95.


