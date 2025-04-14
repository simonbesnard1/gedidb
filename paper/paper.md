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

The Global Ecosystem Dynamics Investigation (GEDI) mission ([Fig. 1](#fig1)) generates spaceborne LiDAR observations essential for characterising Earth's forest structure and carbon dynamics. Despite their high scientific value, GEDI datasets are distributed as complex HDF5 granules, posing significant challenges for efficient large-scale data processing and analysis. To address these challenges, we developed `gediDB`, an open-source Python toolbox designed to streamline the management and analysis of GEDI Level 2A-B and Level 4A-C datasets. Leveraging the optimised multidimensional array database `TileDB`, `gediDB` facilitates operational-scale processing, supports rapid spatial and temporal data queries, and enhances reproducibility in forestry, ecological, and environmental research workflows.

![<a name="fig1"></a>Schematic representation of the GEDI data structure](figs/beam_product_footprint.png)
*Fig. 1: A schematic representation of the GEDI data structure. Credits: Amelia Holcomb's PhD dissertation*

# Statement of need

High-volume LiDAR datasets from the GEDI mission have become indispensable for quantifying forest dynamics, estimating biomass, and analysing carbon cycling. The availability of open-access, spaceborne GEDI LiDAR data has created unprecedented opportunities to extend forest structural analyses from local and regional scales to near-global applications. However, despite the wealth of information in GEDI datasets, practical usability is hindered by the complexity inherent in raw HDF5 granules, the absence of scalable infrastructure for efficient data retrieval, and a lack of standardised tooling for spatial and temporal subsetting at scale.

Current software tools for GEDI data analysis typically target small-scale or isolated use cases and are generally insufficient for handling large-scale, automated workflows. Consequently, researchers conducting extensive spatial and temporal analyses frequently encounter significant challenges related to computational performance, consistency, and reproducibility.

`gediDB` addresses these challenges by providing a robust, scalable framework that unifies access to GEDI Level 2 and Level 4 data through an intuitive API. Built upon the TileDB storage engine, `gediDB` facilitates rapid queries of multidimensional arrays, enabling efficient extraction of large GEDI data subsets based on spatial extents, temporal windows, and specific variable selections. Additionally, `gediDB` integrates seamlessly with Python's geospatial data processing ecosystem, including libraries such as `xarray` and `geopandas`, thereby supporting reproducible workflows optimised for high-performance computing clusters and cloud environments. By leveraging TileDB's advanced spatial indexing, `gediDB` significantly simplifies GEDI data processing (see [Fig. 2](#fig2)).

![<a name="fig2"></a>Schematic representation of the gediDB workflow](figs/GEDIDB_FLOWCHART.png)
*Fig. 2: A schematic representation of the gediDB data workflow.*

# Core functionalities

Extensive documentation and detailed user tutorials for `gediDB` are available at [https://gedidb.readthedocs.io](https://gedidb.readthedocs.io). These provide comprehensive setup instructions, configuration guidance, and workflow examples. Users have immediate access to a globally processed GEDI dataset via TileDB, eliminating the need for local data downloads, as described in the [database documentation](https://gedidb.readthedocs.io/en/latest/user/tiledb_database.html).

`gediDB` is structured around two main modules: [`GEDIProcessor`](https://gedidb.readthedocs.io/en/latest/user/fundamentals.processor.html) and [`GEDIProvider`](https://gedidb.readthedocs.io/en/latest/user/fundamentals.provider.html). The `GEDIProcessor` class systematically ingests and transforms raw GEDI granules into structured TileDB arrays, incorporating critical steps such as data filtering, standardisation, and spatial-temporal chunking. The `GEDIProvider` class enables efficient and flexible data access through spatial bounding-box queries, temporal filtering, and tailored variable selections, outputting results compatible with established Python geospatial libraries (e.g., `xarray`, `pandas`).

Key functionalities of `gediDB` include:

- **High-performance Data Storage**: Efficiently manages GEDI data storage using spatially and temporally optimised TileDB arrays, structured via a global schema ([Fig. 3](#fig3)).

- **Parallel Processing Capabilities**: Incorporates Dask for parallelised downloading, processing, and storage, significantly accelerating data handling for large-scale analyses.

- **Advanced Querying**: Supports advanced spatial and temporal queries, including bounding-box, time-range, and nearest-neighbor retrievals. It seamlessly accommodates both scalar and profile-type variables.

- **Robust Data Downloading**: Includes reliable data acquisition modules interfacing with NASA’s Common Metadata Repository (CMR), featuring built-in retry logic and comprehensive error handling mechanisms.

- **Comprehensive Metadata Management**: Tracks extensive metadata such as data provenance, measurement units, variable definitions, and product versioning directly within the TileDB arrays.

- **Configurable and Reproducible Workflows**: Provides customisable configuration files for defining TileDB schemas and managing data retrieval parameters, ensuring flexibility and reproducibility across varied research contexts.

![<a name="fig3"></a>TileDB fragment schema for GEDI data](figs/tileDB_fragment_structure.png)
*Fig. 3: Illustration of the global GEDI data storage schema using TileDB arrays.*

# Performance benchmarks

The efficiency of `gediDB` was rigorously evaluated under realistic research scenarios. The table below summarises query times across different spatial and temporal extents:

| Scenario                  | Spatial extent         | Time range | Variables queried           | Query time (seconds) |
|---------------------------|------------------------|------------|-----------------------------|----------------------|
| Local-scale query         | 1° × 1° bounding box   | 1 month    | rh profile, canopy cover    | 1.8                  |
| Regional-scale query      | 10° × 10° bounding box | 6 months   | rh profile, biomass, pai    | 17.9                 |
| Continental-scale query   | Amazon Basin           | 1 year     | canopy cover, biomass       | 28.9                 |

Benchmarks were conducted on a Linux server equipped with dual Intel® Xeon® E5-2643 v4 CPUs (12 physical cores, 24 threads total), 503 GB RAM, and a combination of NVMe SSD (240 GB) and HDD storage (16.4 TB total). Queries were executed from NVMe-backed storage to ensure high I/O performance. Compared to workflows based on direct HDF5 access, `gediDB` provides a significant speedup and streamlined user experience.

# Example use cases

An illustrative use case involved the analysis of aboveground biomass and canopy cover dynamics across the Amazon Basin ([Fig. 4](#fig4)). Utilising `gediDB`, variables representing aboveground biomass and canopy cover were efficiently extracted across large spatial extents over multiple years. Data were aggregated within a 1°×1° hexagonal grid framework, facilitating rigorous spatiotemporal analysis of forest structure changes. Integration with Python's geospatial libraries, such as `geopandas`, `xarray`, and `matplotlib` enabled a complete and reproducible workflow from data extraction to visualisation.

Beyond regional change assessments, `gediDB` supports advanced analyses, including biome-level comparisons of forest structural profiles, precise retrieval of GEDI data near field plots for calibration and validation, and the production of spatially gridded datasets at diverse resolutions. The modular and extensible API facilitates seamless integration into cloud-based processing pipelines, long-term ecological monitoring programs, and machine learning workflows that necessitate scalable, high-performance access to remote sensing data.

![<a name="fig4"></a>Changes in aboveground biomass and canopy cover across the Amazon Basin](figs/amazon_changes.png)
*Fig. 4: Visualisation of changes in aboveground biomass density (AGBD) and canopy cover between 2018–2020 and 2021–2023, aggregated to a 1°×1° hexagonal grid over the Amazon Basin.*

# Community impact and future development

`gediDB` fosters an open and collaborative research environment, actively encouraging community-driven development and contributions through its [GitHub repository](https://github.com/simonbesnard1/gedidb). Future development plans for `gediDB` include:

- Ongoing compatibility with future GEDI data releases and product enhancements.
- Expansion of user tutorials and documentation to accommodate diverse user communities.
- Improve testing frameworks.
- Enhanced flexibility and efficiency in querying profile variables.

Feedback, feature requests, and code contributions are actively encouraged from users and developers working with LiDAR data and large-scale environmental analyses. The open-source approach of `gediDB` promotes transparency, reproducibility, and enduring accessibility to support diverse scientific research needs.

# Conclusion

`gediDB` substantially enhances the practical usability of GEDI LiDAR datasets by addressing critical challenges in data complexity, scalability, and reproducibility. By leveraging TileDB’s optimised multidimensional array storage, the package enables efficient management, rapid querying, and integration into diverse geospatial analysis workflows. This promotes the systematic exploration of forest dynamics and carbon cycling at unprecedented spatial and temporal scales. Through its open-source and community-oriented approach, `gediDB` facilitates collaborative advancements in remote sensing, ecology, and environmental research, supporting ongoing developments in Earth observation.

# Acknowledgements

The development of `gediDB` was supported by the European Union through the FORWARDS and OpenEarthMonitor projects. We would also like to acknowledge the R2D2 Workshop (March 2024, GFZ Potsdam) for providing the opportunity to meet and discuss GEDI data processing.
