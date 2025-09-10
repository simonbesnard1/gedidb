---
title: 'gediDB: A toolbox for processing and providing Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data'
tags:
  - Python
  - GEDI
  - LiDAR
  - forest
  - geosciences
  - tileDB
authors:
  - name: Simon Besnard
    orcid: 0000-0002-1137-103X
    corresponding: true
    affiliation: 1
  - name: Felix Dombrowski
    orcid: 0009-0000-9210-3530
    affiliation: 2
  - name: Amelia Holcomb
    orcid: 0000-0001-5081-7201
    affiliation: 3
affiliations:
 - name: GFZ Helmholtz Centre Potsdam, Potsdam, Germany
   index: 1
 - name: University of Potsdam, Potsdam, Germany
   index: 2
 - name: Department of Computer Science, University of Cambridge, Cambridge, UK
   index: 3
date: 25 April 2025
bibliography: refs.bib
---

# Abstract

The Global Ecosystem Dynamics Investigation (GEDI) mission provides spaceborne LiDAR observations that are essential for characterising Earth's forest structure and carbon dynamics. However, GEDI datasets are distributed as complex HDF5 granules, which pose significant challenges for efficient, large-scale data processing and analysis. To overcome these hurdles, we developed `gediDB`, an open-source Python standardised Application Programming Interface (API) that streamlines both the processing and querying of GEDI Level 2A–B and Level 4A–C datasets. Built on the optimised multidimensional array database `TileDB`, `gediDB` enables operational-scale processing, rapid spatial and temporal queries, and reproducible LiDAR-based analyses of forest biomass, carbon stocks, and structural change.



# Statement of Need

High-volume LiDAR datasets from the Global Ecosystem Dynamics Investigation (GEDI) mission [@dubayah2020global] ([Fig. 1](#fig1)) have become essential for quantifying forest dynamics, estimating biomass, and analysing carbon cycling. The open availability of GEDI’s spaceborne LiDAR data has enabled forest structural analysis at near-global scales. However, practical use remains hindered by the complexity of raw HDF5 granules, the absence of scalable infrastructure for efficient access, and a lack of standardised tools for large-scale spatial and temporal subsetting.

![<a name="fig1"></a>Schematic representation of the GEDI data structure](figs/beam_product_footprint.png)
*Fig. 1: A schematic representation of the GEDI data structure. Credits: Amelia Holcomb's PhD dissertation*

Several efforts in the broader NASA LiDAR community have tackled similar challenges. For example, SlideRule [@Shean2023] provides a scalable, cloud-based framework for processing ICESat-2 photon data, enabling users to query and transform complex satellite LiDAR datasets into analysis-ready forms. This illustrates a common pattern: while raw LiDAR missions deliver highly relevant observations, the data formats and scales make direct scientific use difficult without specialised infrastructure. For GEDI, existing services such as NASA's GEDI Subsetter via the Multi-Mission Algorithm and Analysis Platform (MAAP) [@chuck_daniels_2025_15122227] offer useful access for small to moderate-scale extractions, but they are not designed for operational-scale workflows or integration into reproducible pipelines.

`gediDB` addresses this gap by providing a robust, scalable Python-based API for unified access to GEDI Level 2A [@dubayah2021gedi_l2a], 2B [@dubayah2021gedi_l2b], 4A [@dubayah2022gedi_l4a], and 4C [@deconto2024gedi_l4c] products. Built on the TileDB storage engine [@tiledb2025], it enables fast querying of multidimensional arrays by spatial extent, temporal range, and variable selection. Seamless integration with geospatial libraries such as xarray [@hoyer2017xarray] and geopandas [@kelsey_jordahl_2020_3946761] ensures compatibility with reproducible workflows, from local machines to cloud and high-performance computing environments. By leveraging TileDB’s advanced spatial indexing, `gediDB` simplifies and accelerates GEDI data access and analysis (see [Fig. 2](#fig2)).

The increasing use of GEDI in global applications, such as canopy height mapping [@pauls2024estimatingcanopyheightscale], disturbance assessment [@HOLCOMB2024114174], and forest degradation monitoring [@bourgoin2024human], highlights the need for efficient, scalable tooling. By streamlining data access and enabling large-scale, reproducible workflows, `gediDB` fills this role for the GEDI community, complementing efforts like SlideRule in the ICESat-2 domain and supporting broader ecological monitoring and policy-relevant research.

![<a name="fig2"></a>Schematic representation of the gediDB workflow](figs/GEDIDB_FLOWCHART.png)
*Fig. 2: A schematic representation of the gediDB data workflow.*



# Core functionalities

Extensive documentation and tutorials are available at [https://gedidb.readthedocs.io](https://gedidb.readthedocs.io), offering clear setup instructions, configuration guidance, and workflow examples. Users can access a globally processed GEDI dataset directly, avoiding the need for local downloads, as detailed in the [database documentation](https://gedidb.readthedocs.io/en/latest/user/tiledb_database.html).

## Data processing framework

The `gediDB` package centres on two primary modules that streamline GEDI data ingestion and access:

- **`GEDIProcessor`**: Ingests raw GEDI granules and transforms them into structured TileDB arrays ([Fig. 3](#fig3)). The process includes filtering, standardisation, and spatio-temporal chunking to ensure high-performance querying.

- **`GEDIProvider`**: Enables flexible access to GEDI data using spatial and temporal filters and variable selection. Output is compatible with Python libraries such as xarray and pandas [@reback2020pandas].

## Configurable and reproducible workflows

Custom configuration files define the TileDB schema and data retrieval parameters, supporting reproducibility and adaptability across diverse computing environments.

## Robust data downloading

The API connects directly to NASA’s Common Metadata Repository (CMR) and includes robust retry logic and error handling to ensure consistent, fault-tolerant data acquisition.

## High-performance data storage

GEDI data are stored as sparse TileDB arrays optimised for fast spatial and temporal queries. The array structure accommodates large, multidimensional datasets efficiently ([Fig. 3](#fig3)).

## Parallel processing capabilities

`gediDB` supports parallelised downloading, processing, and storage using libraries such as `Dask` [@rocklin2015dask] and `concurrent.futures`, enabling high-throughput workflows on HPC systems.

## Advanced querying functionality

`gediDB` offers flexible querying capabilities, including bounding-box, temporal range, and nearest-neighbour queries. Both scalar and profile-type variables are supported.

## Rich metadata integration

Comprehensive metadata—covering provenance, units, variable descriptions, and versioning—is embedded within the TileDB arrays, ensuring transparency and reproducibility.

![<a name="fig3"></a>TileDB fragment schema for GEDI data](figs/tileDB_fragment_structure.png)
*Fig. 3: Illustration of the global GEDI data storage schema using TileDB arrays.*



# Performance benchmarks

To evaluate the efficiency of `gediDB`, we benchmarked representative research scenarios and compared them to equivalent queries performed with NASA’s GEDI Subsetter via the Multi-Mission Algorithm and Analysis Platform (MAAP). This comparison highlights not only absolute performance, but also practical trade-offs between the two approaches. The table below reports query times (in seconds) for varying spatial and temporal extents.

| Scenario                  | Spatial extent         | Time range | Variables queried                                  | Query time (gediDB) | Query time (MAAP) |
|---------------------------|------------------------|------------|----------------------------------------------------|---------------------|-------------------|
| Local-scale query         | 1° × 1° bounding box   | 1 month    | relative height metrics, canopy cover              | 1.8                 | *TBD*             |
| Regional-scale query      | 10° × 10° bounding box | 6 months   | relative height metrics, biomass, plant area index | 17.9                | *TBD*             |
| Continental-scale query   | Amazon Basin           | 1 year     | canopy cover, biomass                              | 28.9                | *TBD*             |

Benchmarks for `gediDB` were performed on a Linux server with dual Intel® Xeon® E5-2643 v4 CPUs (12 cores, 24 threads), 503 GB RAM, and NVMe SSD (240 GB) + HDD (16.4 TB) storage. All queries ran on NVMe-backed data to ensure high I/O throughput.  



# Example use cases

We used `gediDB` to analyse aboveground biomass and canopy cover dynamics across the Amazon Basin ([Fig. 4](#fig4)). The workflow extracted variables including aboveground biomass, canopy cover, and relative height (RH) metrics across large spatial extents and multiple years. Data were aggregated within a 1°×1° hexagonal grid to support spatiotemporal analysis of forest structural change. The analysis pipeline was implemented entirely in Python using `geopandas` and `xarray`, making it fully reproducible from data extraction to visualisation.

![<a name="fig4"></a>Changes in aboveground biomass and canopy cover across the Amazon Basin](figs/amazon_changes.png)
*Fig. 4: Visualisation of changes in aboveground biomass density (AGBD) (top left panel) and canopy cover (top right panel) between 2018–2020 and 2021–2023, aggregated to a 1°×1° hexagonal grid over the Amazon Basin. The bottom left panel shows the relationship between changes in ΔRH50 and ΔRH98, with each point representing a hexagon. The bottom right panel shows the relationship between changes in canopy height metrics (ΔRH50 and ΔRH98) and ΔAGBD, with each point representing a hexagon. This highlights how vertical canopy structure dynamics relate to biomass change across the region.*

A key advantage of `gediDB` is that large-scale extractions can be performed directly within Python workflows, eliminating the need for manual downloads or interactive tools such as MAAP. For example, the following snippet retrieves biomass, canopy cover, and RH metrics for the Amazon Basin:

```python
import geopandas as gpd
import gedidb as gdb

# Instantiate provider with S3 backend
provider = gdb.GEDIProvider(
    storage_type="s3",
    s3_bucket="dog.gedidb.gedi-l2-l4-v002",
    url="https://s3.gfz-potsdam.de"
)

# Load region of interest (Amazon Basin)
roi = gpd.read_file("amazon_basin.geojson")

# Query GEDI data as xarray dataset
ds = provider.get_data(
    variables=["agbd", "cover", "rh_98", "rh_50"],
    query_type="bounding_box",
    geometry=roi,
    start_time="2018-01-01",
    end_time="2024-01-01",
    return_type="xarray"
)

# Future development

Planned future developments for `gediDB` are designed to improve usability and extend the package’s scope for both researchers and operational users:

- **Compatibility with upcoming GEDI product releases**: ensures long-term sustainability of the toolbox as new mission data become available, avoiding version lock-in for users building workflows on `gediDB`.  

- **Improved performance and flexibility in querying profile variables**: will make it easier for users to analyse canopy structure profiles (e.g., RH metrics) at scale, which are currently among the most data-intensive GEDI products.  

- **Support for direct HDF5 access from AWS S3**: will enable `gediDB` to operate directly on cloud-hosted GEDI granules (e.g., on NASA’s MAAP infrastructure), avoiding the need for local downloads and reducing storage overhead. This work corresponds to [Issue #15](https://github.com/simonbesnard1/gedidb/issues/15) (“Allow direct h5 read w/o download”), which aims to let the `GEDIProcessor` access HDF5 files in an S3 bucket without intermediate steps.  

- **Expanded documentation and tutorials**: will benefit new users by lowering the entry barrier, providing clear end-to-end examples, and connecting scientific use cases to code snippets.  

- **Strengthened testing for reliability and maintainability**: supports developers and long-term users by ensuring that changes do not break existing workflows, and by increasing trust in the reproducibility of analyses built on `gediDB`.  

Development progress and discussion of these features are tracked openly through the project’s [GitHub issues](https://github.com/your-org/gedidb/issues) and roadmap.



# Future development

Planned future developments for `gediDB` include:

- Compatibility with upcoming GEDI product releases
- Improved performance and flexibility in querying profile variables
- Support for direct HDF5 access from AWS S3
- Expanded documentation and tutorials
- Strengthened testing for reliability and maintainability



# Conclusion

`gediDB` enhances the usability of GEDI LiDAR datasets by addressing challenges of data complexity, scalability, and reproducibility. Built on TileDB, it enables efficient data management, fast querying, and integration into geospatial workflows—facilitating large-scale analysis of forest dynamics and carbon cycling. Its open-source, community-driven design supports ongoing progress in remote sensing and environmental science.



# Acknowledgements

The development of `gediDB` was supported by the European Union through the FORWARDS and NextGenCarbon projects. We also acknowledge funding for 3D-ABC by the Helmholtz Foundation Model Initiative, supported by the Helmholtz Association. We would also like to thank the R2D2 Workshop (March 2024, Potsdam) for providing the opportunity to meet and discuss GEDI data processing. We recognise using OpenAI's ChatGPT and Grammarly AI tools to enhance the manuscript's sentence structure, conciseness, and grammatical accuracy.



# References


