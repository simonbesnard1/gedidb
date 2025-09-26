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
 - name: GFZ Helmholtz Centre for Geosciences Potsdam, Potsdam, Germany
   index: 1
 - name: University of Potsdam, Potsdam, Germany
   index: 2
 - name: Department of Computer Science, University of Cambridge, Cambridge, UK
   index: 3
date: 10 September 2025
bibliography: refs.bib
---

# Abstract

The Global Ecosystem Dynamics Investigation (GEDI) mission provides spaceborne LiDAR observations that are essential for characterising Earth's forest structure and carbon dynamics. However, GEDI datasets are distributed as complex HDF5 granules, which pose significant challenges for efficient, large-scale data processing and analysis. To overcome these hurdles, we developed `gediDB`, an open-source Python standardised Application Programming Interface (API) that streamlines both the processing and querying of GEDI Level 2A–B and Level 4A–C datasets. Built on the optimised multidimensional array database TileDB, `gediDB` enables operational-scale processing, rapid spatial and temporal queries, and reproducible LiDAR-based analyses of forest biomass, carbon stocks, and structural change.



# Statement of Need

High-volume LiDAR datasets from the Global Ecosystem Dynamics Investigation (GEDI) mission [@dubayah2020global] ([Fig. 1](#fig1)) are central for quantifying forest dynamics, estimating biomass, and analysing carbon cycling. Yet, their practical use is limited by the complexity of raw HDF5 granules, the lack of scalable infrastructure, and the absence of standardised tools for large-scale spatio-temporal subsetting. The increasing use of GEDI in global applications, such as canopy height mapping [@pauls2024estimatingcanopyheightscale], disturbance assessment [@HOLCOMB2024114174], and forest degradation monitoring [@bourgoin2024human], underscores the need for efficient and scalable tooling.

![Schematic representation of the GEDI data structure](figs/beam_product_footprint.png){#fig1}  
*Fig. 1: Schematic representation of the GEDI data structure. Credits: Amelia Holcomb's PhD dissertation [@holcomb_measuring_2025].*

Several efforts in the NASA LiDAR community highlight similar challenges. For example, SlideRule [@Shean2023] provides a scalable, cloud-based framework for ICESat-2, illustrating a common pattern: while raw LiDAR missions deliver highly relevant observations, the formats and scales hinder direct scientific use without specialised infrastructure. For GEDI, existing services such as NASA’s GEDI Subsetter via MAAP [@chuck_daniels_2025_15122227] are useful for small extractions but not designed for large-scale, reproducible workflows. They also introduce two key limitations:  
- **Per-product queries:** Requests must be made separately for each GEDI product (L2A, L2B, L4A, L4C), preventing cross-product filtering.  
- **Restricted access:** MAAP accounts are limited to NASA/ESA-affiliated researchers.  

`gediDB` fills this gap with a Python-based API that unifies access to all GEDI products (Level 2A [@dubayah2021gedi_l2a], 2B [@dubayah2021gedi_l2b], 4A [@dubayah2022gedi_l4a], and 4C [@deconto2024gedi_l4c]). Built on the TileDB engine [@tiledb2025], it enables fast, scalable queries by space, time, and variable. Results integrate seamlessly with `xarray` [@hoyer2017xarray] and `geopandas` [@kelsey_jordahl_2020_3946761], supporting reproducible workflows from laptops to HPC (see [Fig. 2](#fig2)). By simplifying access and scaling to global analyses, `gediDB` complements efforts like SlideRule in the ICESat-2 domain and supports ecological monitoring and policy-relevant research.

![Schematic representation of the gediDB workflow](figs/GEDIDB_FLOWCHART.png){#fig2}
*Fig. 2: A schematic representation of the gediDB data workflow.*

# Core functionalities

Full documentation and tutorials are available at [https://gedidb.readthedocs.io](https://gedidb.readthedocs.io), including setup, configuration, and workflow examples. Users can also access a globally processed GEDI dataset without local downloads, as detailed in the [database documentation](https://gedidb.readthedocs.io/en/latest/user/tiledb_database.html).

`gediDB` centres on two modules:

- **`GEDIProcessor`**: Converts raw GEDI granules into structured TileDB arrays through filtering, standardisation, and spatio-temporal chunking.  
- **`GEDIProvider`**: Supports efficient spatio-temporal queries, variable access, and quality filtering, with outputs in `xarray` or `pandas` [@reback2020pandas].

Data are stored as **sparse TileDB arrays**, optimised for the footprint-based nature of GEDI, enabling compact storage and fast queries at global scale ([Fig. 3](#fig3)). Parallel processing with `Dask` [@rocklin2015dask] supports high-throughput workflows, while rich metadata (provenance, units, versioning) ensures transparency and reproducibility.

![TileDB fragment schema for GEDI data](figs/tileDB_fragment_structure.png){#fig3}  
*Fig. 3: Global GEDI data storage schema in TileDB.*



# Performance benchmarks

To evaluate the efficiency of `gediDB`, we benchmarked spatiotemporal select queries against two alternatives:  

1. **NASA's GEDI Subsetter** on the MAAP, which reads GEDI products directly as HDF5 from LPDAAC's S3 bucket using temporary AWS credentials, rather than operating on a preprocessed copy of the data.    
2. **A single-server PostGIS instance** hosting the GEDI data, following the approach in [Holcomb, 2023](https://github.com/ameliaholcomb/gedi-database). Postgres cannot be sharded across multiple servers, so while it may be a good option for users with only one machine, it does not achieve the speeds of a distributed database like TileDB.  

This comparison highlights both absolute performance and practical trade-offs between the three approaches.  

| Scenario                | Spatial extent         | Time range | Variables queried                                  | Query time (`gediDB`) | Query time (MAAP) | Query time (PostGIS) |
|-------------------------|------------------------|------------|----------------------------------------------------|-----------------------|-------------------|----------------------|
| Local-scale query       | 1° × 1° bounding box   | 1 month    | relative height metrics, canopy cover              | 1.8 s                 | 51 s              | 5.0 s                |
| Regional-scale query    | 10° × 10° bounding box | 6 months   | relative height metrics, biomass, plant area index | 17.9 s                | 3,037 s           | 596.6 s              |
| Continental-scale query | Amazon Basin           | 1 year     | canopy cover, biomass                              | 28.9 s                | 17,917 s          | 4,812.9 s            |

**Benchmark setup**  
- **gediDB:** Client machine: Linux server with dual Intel® Xeon® E5-2643 v4 CPUs (12 cores, 24 threads), 503 GB RAM, and local NVMe SSD (240 GB) + HDD (16.4 TB) storage. Queries were executed against GEDI data stored in a Ceph object storage cluster (version Quincy) comprising 18× DELL R7515 nodes, each with 11-18× 18-20 TB Seagate data drives and 1× 1.6 TB NVMe (metadata). 
- **gediDB:** Linux server with dual Intel® Xeon® E5-2643 v4 CPUs (12 cores, 24 threads), 503 GB RAM, and NVMe SSD (240 GB) + HDD (16.4 TB) storage. Queries ran on NVMe-backed data to ensure high I/O throughput.  
- **MAAP + GEDI Subsetter:** version 0.12.0 running on `maap-dps-worker-32gb`. Because the subsetter requires each product to be queried separately, per-product jobs were initiated in parallel, and the benchmark time is the longest runtime of any individual product (excluding queueing time).  
- **PostGIS:** version 14, hosted on a server with 4× 3.84 TB SATA SSDs (software RAID), two 18-core dual-threaded Intel® Xeon® CPU E5-2695 v4 @ 2.10GHz, and 512 GB RAM.  

**Interpretation**  
These benchmarks demonstrate that `gediDB` consistently outperforms both MAAP and PostGIS across local, regional, and continental queries. The difference is most pronounced at larger scales: for the Amazon Basin, `gediDB` returns results in under 30 seconds, compared to ~1.3 hours with PostGIS and nearly 5 hours with MAAP. 



# Example use cases

We use `gediDB` to analyse aboveground biomass and canopy cover dynamics across the Amazon Basin ([Fig. 4](#fig4)). The workflow extracts variables including aboveground biomass, canopy cover, and relative height (RH) metrics across large spatial extents and multiple years. Data are aggregated within a 2°×2° hexagonal grid to support spatiotemporal analysis of forest structural change. The analysis pipeline is implemented entirely in Python using `geopandas` and `xarray`, making it fully reproducible from data extraction to visualisation.

![Changes in aboveground biomass and canopy cover across the Amazon Basin](figs/amazon_changes.png){#fig4}
*Fig. 4: Visualisation of changes in aboveground biomass density (AGBD) (top left panel) and canopy cover (top right panel) between 2018–2020 and 2021–2023, aggregated to a 1°×1° hexagonal grid over the Amazon Basin. The bottom left panel shows the relationship between changes in ΔRH50 and ΔRH98, with each point representing a hexagon. The bottom right panel shows the relationship between changes in canopy height metrics (ΔRH50 and ΔRH98) and ΔAGBD, with each point representing a hexagon. This highlights how vertical canopy structure dynamics relate to biomass change across the region.*  

A key advantage of `gediDB` is that large-scale extractions can be performed directly within Python workflows, eliminating the need for manual downloads or interactive tools such as MAAP. Unlike the GEDI Subsetter, which requires per-product queries and does not support multi-product filtering, `gediDB` allows unified access across Level 2A, 2B, 4A, and 4C products in a single query.  

For example, the following snippet demonstrates how GEDI variables can be retrieved for the Amazon Basin as an `xarray.Dataset`:  

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
```



# Conclusion

`gediDB` improves the usability of GEDI LiDAR datasets by removing key barriers of data complexity, scalability, and reproducibility. By representing GEDI products as sparse multidimensional arrays in TileDB, it enables fast, flexible queries across space, time, and variables, and integrates seamlessly into established geospatial workflows. This allows researchers to perform analyses that extend from local case studies to continental-scale assessments of forest dynamics and the carbon cycle. As an open-source and community-driven project, `gediDB` provides a sustainable framework for large-scale exploitation of spaceborne LiDAR data in remote sensing and environmental science.



# Acknowledgements

The development of `gediDB` was supported by the European Union through the [FORWARDS](https://forwards-project.eu/) and [NextGenCarbon](https://www.nextgencarbon-project.eu/) projects, and by the Helmholtz Association via the Helmholtz Foundation Model Initiative ([3D-ABC project](https://www.3d-abc.ai/)). AC acknowledges funding from the Harding Distinguished Postgraduate Scholarship. We are grateful to participants of the R2D2 Workshop (March 2024, Potsdam) for valuable discussions on GEDI data processing that helped shape this work. We also acknowledge the use of language-assistance tools (OpenAI ChatGPT and Grammarly) to improve readability and grammar. The design, conceptualisation, and scientific results of this work are entirely those of the authors.



# References


