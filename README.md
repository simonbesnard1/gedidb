<p align="center">
<a href="https://git.gfz-potsdam.de/global-land-monitoring/gedidb.git">
    <img src="https://git.gfz-potsdam.de/global-land-monitoring/gedidb/-/raw/main/doc/_static/logos/gediDB_logo.svg" 
         alt="gediDB Logo" height="120px" hspace="0px" vspace="30px" align="left">
</a>
</p>


# gediDB: A toolbox for Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data

[![Pipelines](https://git.gfz-potsdam.de/global-land-monitoring/gedidb/badges/main/pipeline.svg)](https://git.gfz-potsdam.de/global-land-monitoring/gedidb/pipelines)
[![Code Coverage](https://git.gfz-potsdam.de/global-land-monitoring/gedidb/badges/main/coverage.svg)](https://global-land-monitoring.git-pages.gfz-potsdam.de/gedidb/coverage/)
[![Docs](https://img.shields.io/static/v1?label=Documentation&message=GitLab%20Pages&color=orange)](https://global-land-monitoring.git-pages.gfz-potsdam.de/gedidb/)
[![Available on PyPI](https://img.shields.io/pypi/v/gedidb.svg)](https://pypi.python.org/pypi/gedidb/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.13885229.svg)](https://doi.org/10.5281/zenodo.13885229)

**gediDB** is an open-source Python package designed to streamline the processing, analysis, and management of GEDI L2A-B and L4A-C data. This toolbox enables efficient and flexible data querying and management of large GEDI datasets stored with [TileDB](https://tiledb.com/), a high-performance, multi-dimensional array database.

**gediDB** integrates key functionalities such as structured data querying, multi-dimensional data processing, and metadata management. With built-in support for parallel engines (e.g. Dask), the toolbox ensures scalability for large datasets, allowing efficient parallel processing on local machines or clusters.

## Key Features of gediDB

- **TileDB-Based Storage**: GEDI data is stored and managed in TileDB arrays, providing efficient, scalable, multi-dimensional data storage, enabling fast and flexible access to large volumes of data.
- **Flexible Data Querying**: Easily query GEDI data across spatial, temporal, and variable dimensions. Access data within bounding boxes, or retrieve the nearest shots to a specific location, using intuitive filtering options for precision.
- **Parallel Processing**: Process large GEDI datasets in parallel, enabling concurrent downloading, processing, and TileDB insertion of GEDI products. The number of concurrent processes can be easily controlled based on available system resources.
- **Metadata-Driven**: Maintain and manage metadata for each dataset, ensuring that important contextual information like units, descriptions, and source details are stored and accessible.
- **Geospatial Data Management**: Integrate seamlessly with tileDB to enable spatial queries, transformations, and geospatial analyses.

## Why gediDB?
**gediDB** simplifies and automates the workflow for GEDI data processing, making it easier to retrieve, filter, and analyze complex datasets in an efficient, scalable manner. Whether you're investigating biomass distribution, monitoring forest dynamics, or conducting large-scale ecological studies, **gediDB** supports users with tools to handle and analyze large GEDI datasets with ease.

## Documentation

Learn more about gediDB in its official documentation at
<https://global-land-monitoring.git-pages.gfz-potsdam.de/gedidb/>.

## Contributing

You can find information about contributing to gediDB on our
[Contributing page](https://global-land-monitoring.git-pages.gfz-potsdam.de/gedidb/user/contributing.html).

## History

The development of the gediDB package began during the PhD of Amelia Holcomb, who initially created part of this toolset to analyze and manage GEDI data for her research. Recognizing the potential of her work to benefit the broader scientific community, the [Global Land Monitoring](https://www.gfz-potsdam.de/en/section/remote-sensing-and-geoinformatics/topics/global-land-monitoring) team collaborated with Amelia in March 2024 to expand and optimize her code, transforming it into a scalable and versatile Python package named gediDB. This collaboration refined the toolbox to handle large-scale datasets with TileDB, integrate parallel processing, and incorporate a robust querying and metadata management system. Today, gediDB is designed to help researchers in ecological and environmental sciences by making GEDI data processing more efficient and accessible.

## About the authors

Simon Besnard, a senior researcher in the Global Land Monitoring Group at GFZ Helmholtz Centre Potsdam, studies terrestrial ecosystems' dynamics and their feedback on environmental conditions. He specializes in developing methods to analyze large EO and climate datasets to understand ecosystem functioning in a changing climate. His current research focuses on forest structure changes over the past decade and their links to the carbon cycle. 

Felix Dombrowski is a Bachelor’s student in Computer Science at the University of Potsdam and a research intern in the Global Land Monitoring Group at GFZ Helmholtz Centre Potsdam. At GFZ, his work has focused on developing toolboxes to process Earth Observation data efficiently.

Amelia Holcomb is a PhD candidate in Computer Science at the University of Cambridge, researching remote sensing and machine learning to study carbon sequestration and forest regrowth. Previously, she worked as a site reliability engineer at Google on Bigtable. She holds an MMath from the University of Waterloo and a B.A. in Mathematics from Yale.

## Contact

For any questions or inquiries, please contact:
- Simon Besnard (besnard@gfz.de)
- Felix Dombrowski (felix.dombrowski@uni-potsdam.de)
- Amelia Holcomb (ah2174@cam.ac.uk)

## Acknowledgments
We acknowledge funding support by the European Union through the [FORWARDS](https://forwards-project.eu/) and [OpenEarthMonitor](https://earthmonitor.org/) projects. We would also like to thank the R2D2 Workshop (March 2024, GFZ Potsdam) for providing the opportunity to meet and discuss GEDI data processing.

## License
This project is licensed under the EUROPEAN UNION PUBLIC LICENCE v.1.2 License - see the LICENSE file for details.
