# gediDB: A toolbox for Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data

[![CI](https://github.com/pydata/xarray/workflows/CI/badge.svg?branch=main)](https://github.com/pydata/xarray/actions?query=workflow%3ACI)
[![Code coverage](https://codecov.io/gh/pydata/xarray/branch/main/graph/badge.svg?flag=unittests)](https://codecov.io/gh/pydata/xarray)
[![Docs](https://readthedocs.org/projects/xray/badge/?version=latest)](https://docs.xarray.dev/)
[![Benchmarked with asv](https://img.shields.io/badge/benchmarked%20by-asv-green.svg?style=flat)](https://pandas.pydata.org/speed/xarray/)
[![Available on pypi](https://img.shields.io/pypi/v/xarray.svg)](https://pypi.python.org/pypi/xarray/)
[![Formatted with black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.11183201.svg)](https://doi.org/10.5281/zenodo.11183201)

**gediDB** is an open source project and Python
package that makes working with GEDI L2A-B and L4A-C data easy and fun!

**gediDB** is designed to streamline the processing, analysis, and management of GEDI data. The toolbox provides an intuitive interface for handling large volumes of GEDI data, automating the downloading, parsing, and processing of GEDI granules. Built to work efficiently with GEDI’s products, it is tailored for scientists, researchers, and data analysts working in fields like ecology, forestry, remote sensing, and environmental science.

**gediDB** integrates key functionalities such as structured data querying, multi-dimensional data processing, and metadata management. With built-in support for Dask, the toolbox ensures scalability for large datasets, allowing efficient parallel processing on local machines or clusters.

## Key Features of gediDB

- Granule Management: Automatically download and process GEDI granules across different product levels (e.g., L2A, L2B, L4A, L4C) in parallel, ensuring metadata consistency and data quality.
- Flexible Data Querying: Query data from the GEDI database using labeled dimensions such as time, location, and beam name. The SQL-based querying allows for precise data extraction, filtered by various spatial and temporal constraints.
- Parallel Processing with Dask: Seamlessly process large datasets in parallel using Dask, enabling concurrent downloading, processing, and database insertion of GEDI products. The number of concurrent processes can be easily controlled based on system resources.
- Integration with HDF5 and Parquet Formats: The toolbox supports parsing and saving data in both HDF5 and Parquet formats, providing flexibility in managing data storage and retrieval.
- Multi-product Handling: Process multiple products per granule simultaneously, including Level 2 and Level 4 GEDI products, ensuring that data from different sources are handled efficiently.
- Metadata-Driven: Maintain and manage metadata for each dataset, ensuring that important contextual information such as units, descriptions, and source details are stored and accessible.
- Geospatial Data Management: Built-in support for geospatial data types using GeoPandas, allowing seamless spatial queries and operations.

## Why gediDB?
**gediDB** simplifies and automates the workflow of processing GEDI satellite data, reducing manual work and enabling efficient parallel processing for large-scale datasets. Whether you're conducting ecological studies, analyzing biomass data, or investigating land surface dynamics, **gediDB** empowers users with the tools to quickly process and analyze GEDI data.

## Documentation

Learn more about xarray in its official documentation at
<https://docs.xarray.dev/>.

Try out an [interactive Jupyter
notebook](https://mybinder.org/v2/gh/pydata/xarray/main?urlpath=lab/tree/doc/examples/weather-data.ipynb).

## Contributing

You can find information about contributing to xarray at our
[Contributing
page](https://docs.xarray.dev/en/stable/contributing.html).

## Get in touch

- Ask usage questions ("How do I?") on
  [GitHub Discussions](https://github.com/pydata/xarray/discussions).
- Report bugs, suggest features or view the source code [on
  GitHub](https://github.com/pydata/xarray).
- For less well defined questions or ideas, or to announce other
  projects of interest to xarray users, use the [mailing
  list](https://groups.google.com/forum/#!forum/xarray).

## History

Xarray is an evolution of an internal tool developed at [The Climate
Corporation](http://climate.com/). It was originally written by Climate
Corp researchers Stephan Hoyer, Alex Kleeman and Eugene Brevdo and was
released as open source in May 2014. The project was renamed from
"xray" in January 2016. Xarray became a fiscally sponsored project of
[NumFOCUS](https://numfocus.org) in August 2018.


## Contact person
For any questions or inquiries, please contact Amelia Holcomb (ah2174@cam.ac.uk), Felix Dombrowski (felix.dombrowski@uni-potsdam.de) and Simon Besnard (besnard@gfz-potsdam.de) 

## Acknowledgments
We acknowledge funding support by the European Union through the FORWARDS (https://forwards-project.eu/) and OpenEarthMonitor (https://earthmonitor.org/) projects. We also would like to thank the R2D2 Workshop (March 2024, GFZ. Potsdam) for providing the opportunity to meet and discuss GEDI data processing.

## License
This project is licensed under the EUROPEAN UNION PUBLIC LICENCE v.1.2 License - see the LICENSE file for details.