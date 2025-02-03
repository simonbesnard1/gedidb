
.. for doctest:
    >>> import gedidb as gdb

.. _fundamentals-filters:

#################
Quality Filtering
#################

This guide details the quality filtering applied to GEDI Level 2A (L2A) and Level 2B (L2B) data in the gediDB package. Each data product uses specific filters to ensure only high-quality data is processed, enhancing data reliability in analysis.

Overview
--------

In gediDB, we have implemented a **default quality filtering routine** based on community-recommended practices. These filters are automatically applied during data ingestion, reducing the dataset size by up to **30%** without compromising data quality. This streamlines your analysis pipeline, saving both time and computational resources.

The filters rely on quality-related variables present in the raw HDF5 files. While these variables are available, they are **not pre-applied** in the raw data, which can lead to the inclusion of low-quality data if not filtered. gediDB automates this process, applying filters consistently to ensure high data integrity.

Filtering and Product Merging
-----------------------------

The filters are applied **sequentially** to each data product (L2A and L2B) before merging. This ensures that only data meeting high-quality standards across products is retained in the final dataset. The merging process uses `shot_number` as the key to join each product’s data into a cohesive DataFrame.

Key points:

- **Product-Specific Filtering:** Each product’s data is automatically filtered using the respective beam class (`L2ABeam` and `L2BBeam`).
- **Merging Logic:** An **inner join** on `shot_number` ensures that only records present in both L2A and L2B (after filtering) are included.
- **Data Integrity:** Records missing in any product after filtering are excluded, maintaining consistency and reliability in the final dataset.

.. note::

    Quality filters are only applied to the L2A and L2B products. No additional filters are applied to L4A and L4C products. However, because the merging relies on the filtered L2A and L2B data, any GEDI shots marked as low-quality in L2A or L2B will also be excluded from L4A and L4C. This approach assumes that if a shot is of poor quality in L2A/B, it is likely unreliable in L4A/C as well.


L2A Product Quality Filtering
-----------------------------

The `L2ABeam` class applies several filters to ensure data quality for Level 2A data:

- **`quality_flag`**: Retains data where `quality_flag` is `1`, ensuring high-quality measurements.
- **`sensitivity_a0`**: Keeps data with `sensitivity_a0` between `0.9` and `1.0`.
- **`sensitivity_a2`**: Selects data with `sensitivity_a2` between `0.95` and `1.0`.
- **`degrade_flag`**: Excludes data with `degrade_flag` values indicating degraded data quality.
- **`surface_flag`**: Retains data where `surface_flag` is `1`, indicating reliable surface measurements.
- **`elevation_difference_tdx`**: Retains data where the difference between `elev_lowestmode` and `digital_elevation_model` is within `-150` to `150` meters.

L2B Product Quality Filtering
-----------------------------

For Level 2B data, the `L2BBeam` class applies the following filters:

- **`water_persistence`**: Retains data with `landsat_water_persistence` below `10`, reducing noise from persistent water bodies.
- **`urban_proportion`**: Excludes data where `urban_proportion` exceeds `50%`, as urban areas can introduce data artifacts.