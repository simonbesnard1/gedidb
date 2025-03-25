---
title: 'gediDB: A toolbox for processing and providing Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data'
tags:
  - Python
  - GEDI
  - LiDAR
  - remote sensing
  - TileDB
authors:
  - name: Simon Besnard
    orcid: 0000-0000-0000-0000
    affiliation: 1
    corresponding: true
  - name: Felix Dombrowski
    affiliation: 2
  - name: Amelia Holcomb
    affiliation: 3
affiliations:
 - name: GFZ Helmholtz Centre Potsdam, Germany
   index: 1
 - name: University of Potsdam, Germany
   index: 2
 - name: University of Cambridge, UK
   index: 3
date: 25 March 2025
bibliography: paper.bib
---

# Summary

The Global Ecosystem Dynamics Investigation (GEDI) mission provides high-resolution LiDAR observations critical for understanding Earth's forest structure and carbon dynamics. However, GEDI datasets are large, complex, and cumbersome to process due to their HDF5-based granule structure. To overcome these challenges, we developed `gediDB`, an open-source Python package designed to simplify the processing, querying, and management of GEDI Level 2A-B and Level 4A-C datasets.

# Statement of need

GEDI's granule-based storage presents significant practical challenges: retrieving data for specific geographic or temporal extents can be inefficient and computationally demanding, particularly at regional or global scales. Current approaches often require substantial preprocessing and handling of large numbers of individual HDF5 files. `gediDB` streamlines this process by reorganizing GEDI data into spatially indexed TileDB arrays, significantly improving efficiency, scalability, and accessibility for large-scale analysis.

![Illustration of the online monitoring process for a univariate time-series, depicting the distinct phases including the history period, monitoring period, and a detected break against modeled values. \label{fig:concept}](figs/NRT_image.png)

![Figure 1: Comparison of traditional GEDI data access workflow versus the simplified workflow provided by gediDB. \label{fig:concept}](figs/GEDIDB_FLOWCHART.png)

# Features

`gediDB` provides:

- **Efficient storage and indexing**: Uses TileDB for high-performance storage and fast querying across spatial, temporal, and attribute dimensions.
- **Parallel data processing**: Integrates Dask for parallel downloads and processing, enabling scalability for large datasets.
- **Flexible querying interface**: Supports bounding-box and temporal-range queries, as well as direct retrieval of nearest GEDI shots.
- **Metadata management**: Comprehensive storage of metadata, including dataset descriptions, units, and provenance.
- **Robust downloading**: Includes a `CMRDataDownloader` module with built-in error handling and retry mechanisms for reliable data acquisition from NASA's Common Metadata Repository (CMR).
- **Configuration management**: Allows users to manage data retrieval parameters, TileDB storage schemas, and querying options via flexible configuration files, enhancing adaptability across various use cases.

![Figure 2: Illustration of the global GEDI data storage schema using TileDB arrays. \label{fig:data-structure}](figs/tileDB_fragment_structure.png)

# Performance benchmarks

The performance of `gediDB` was evaluated using realistic querying scenarios. The following table illustrates typical query response times:

| Scenario                  | Spatial extent         | Time range | Variables queried           | Query time (seconds) |
|---------------------------|------------------------|------------|-----------------------------|----------------------|
| Local-scale query         | 1° × 1° bounding box   | 1 month    | rh98, canopy_cover          | 2.1                  |
| Regional-scale query      | 10° × 10° bounding box | 6 months   | rh98, biomass, pai          | 7.4                  |
| Amazon Basin query        | Amazon Basin           | 1 year     | canopy_cover, biomass       | 20.8                 |

Performance testing was conducted on a Linux system (Intel Xeon CPU, 64 GB RAM, NVMe SSD). Results show rapid query performance even at continental scales, highlighting the effectiveness of the TileDB storage approach.

# Example use case

An example use case includes analyzing forest recovery in the Amazon, where `gediDB` efficiently extracted relevant GEDI variables over large spatial-temporal extents. Users can integrate outputs seamlessly with popular scientific Python libraries like `geopandas` and `xarray` for advanced spatial analysis and visualization. This has practical applications in forest dynamics studies, biomass estimation, carbon accounting, and ecological assessments.

# Community impact and future development

`gediDB` is actively maintained and designed with community-driven development in mind. Future developments include:
- Integration with cloud storage services for easier access and scalability.
- Enhancements in parallel processing capabilities for even faster data retrieval and analysis.
- Continued expansion of the documentation and user tutorials to facilitate broader adoption by the remote sensing community.

# Acknowledgements

Development was supported by the European Union through the FORWARDS and OpenEarthMonitor projects. We thank the R2D2 Workshop at GFZ Potsdam (March 2024) for valuable discussions.