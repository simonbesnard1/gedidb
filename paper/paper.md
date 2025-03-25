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
    orcid: 0000-0002-1137-103X
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
bibliography: refs.bib
--- 

# Abstract

The Global Ecosystem Dynamics Investigation (GEDI) mission provides high-resolution spaceborne LiDAR observations critical for understanding Earth's forest structure and carbon dynamics. However, GEDI datasets, structured as HDF5 granules, are inherently complex and challenging to efficiently process for large-scale analyses. To facilitate operational processing and large-scale querying of GEDI data, we developed `gediDB`, an open-source Python toolbox that restructures and manages GEDI Level 2A-B and Level 4A-C data using TileDB, an optimized multidimensional array database. `gediDB` significantly enhances the efficiency and scalability of GEDI data analysis, enabling rapid spatial and temporal queries and fostering reproducible workflows in forestry, ecology, and environmental research.

# Statement of need

High-volume GEDI LiDAR data are essential for studying forest dynamics, biomass estimation, and carbon cycling. Yet, traditional workflows involving raw GEDI HDF5 granules are heavy due to the substantial overhead of file management, preprocessing, and querying over large geographic extents. Researchers and practitioners need accessible, streamlined solutions for retrieving spatially and temporally explicit subsets of GEDI data without the computational burden typically associated with handling raw granules. `gediDB` addresses this critical gap by providing an efficient, scalable framework that leverages spatial indexing through TileDB, significantly simplifying and accelerating data handling.

![Schematic representation of the gediDB workflow](figs/GEDIDB_FLOWCHART.png)
*Figure 1: A schematic representation of the gediDB data workflow.*

# Core functionalities

`gediDB` provides:

- **High-performance data storage**: Efficiently stores GEDI data using TileDB, optimized for rapid multidimensional data access.
- **Parallel processing capabilities**: Seamless integration with Dask for parallel downloading, processing, and storage of large GEDI datasets.
- **Advanced querying interface**: Facilitates spatial bounding-box queries, temporal range selections, and retrieval of nearest-neighbor GEDI observations.
- **Comprehensive metadata management**: Tracks detailed metadata, including data provenance, units, variable descriptions, and product versioning.
- **Robust data downloading**: Implements a reliable CMRDataDownloader module that handles data acquisition from NASA's Common Metadata Repository (CMR) with built-in retry and error handling mechanisms.
- **Configurable and reproducible workflows**: Utilizes customizable configuration files for managing TileDB schemas, data retrieval parameters, and query specifications, ensuring reproducible and adaptable analyses.

![TileDB fragment schema for GEDI data](figs/tileDB_fragment_structure.png)
*Figure 2: Illustration of the global GEDI data storage schema using TileDB arrays.*

# Performance benchmarks

The efficiency of `gediDB` was rigorously evaluated under realistic scenarios representative of typical research needs. The following table highlights query times for various spatial and temporal extents:

| Scenario                  | Spatial extent         | Time range | Variables queried           | Query time (seconds) |
|---------------------------|------------------------|------------|-----------------------------|----------------------|
| Local-scale query         | 1° × 1° bounding box   | 1 month    | rh98, canopy_cover          | 2.1                  |
| Regional-scale query      | 10° × 10° bounding box | 6 months   | rh98, biomass, pai          | 7.4                  |
| Continental-scale query   | Amazon Basin           | 1 year     | canopy_cover, biomass       | 20.8                 |

Testing was conducted on a Linux server with Intel Xeon CPUs, 64 GB RAM, and NVMe storage, demonstrating gediDB's capability to efficiently manage and query large GEDI datasets.

# Example use cases

An illustrative application involved analyzing canopy height changes across the Amazon Basin. Using gediDB, GEDI variables such as canopy height and canopy cover were rapidly extracted for a large spatial-temporal extent. The retrieved data were then aggregated within a spatial grid composed of 3x3 degree hexagons, facilitating detailed analysis of canopy height variability and temporal changes across the region. Integration with Python libraries such as geopandas and xarray streamlined spatial analyses and visualization, offering practical insights into forest structure dynamics.

# Community impact and future development

`gediDB` aims to foster a collaborative, community-driven environment. Planned future enhancements include:
- Support for upcoming GEDI data releases and versions.
- Further optimization of parallel processing for increased performance.
- Expansion of comprehensive documentation, tutorials, and example workflows to facilitate adoption across the remote sensing community.

We encourage contributions and collaboration from researchers and practitioners interested in large-scale GEDI data analysis.

# Acknowledgements

The development of `gediDB` was supported by the European Union through the FORWARDS and OpenEarthMonitor projects. We gratefully acknowledge discussions and contributions from participants of the R2D2 Workshop at GFZ Potsdam (March 2024).
