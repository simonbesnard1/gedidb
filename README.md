<p align="center">
<a href="https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git">
    <img src="https://media.gfz-potsdam.de/gfz/wv/pic/Bildarchiv/gfz/GFZ-CD_LogoRGB_en.png" alt="Master" height="158px" hspace="10px" vspace="0px" align="right">
  </a>
</p>

***
# GEDI toolbox: A toolbox to download, process, store and visualise Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data #

### *by [Amelia Holcomb](https://ameliaholcomb.github.io/), Felix Dombrowski and [Simon Besnard](https://simonbesnard1.github.io/)*

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Package installation](#package-installation)
- [Setting up the PostgreSQL database](#setting-up-the-postgresql-database)
- [Getting started](#getting-started)
- [Contributing](#contributing)
- [License](#license)
- [Citing geditoolbox](#citing-geditoolbox)
- [Contact person](#contact-person)

## Overview
This repository contains the code and data associated to download, process, store and visualise GEDI L2A-B and L4A-C data. The data are stored in a PostgreSQL database. 

## Repository Structure

```plaintext
├── config_files
├── data
├── gedidb 		           # Core of the package
│   ├── core
│   ├── database
│   ├── downloader
│   ├── processor
│   ├── providers
│   ├── tests
│   ├── utils
├── images
├── scripts
├── LICENSE
├── README.md
├── setup.cfg
└── setup.py
```

## Package installation

The code requires `python>=3.12`

Install gedi-toolbox:

```
pip install git+https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git

```

or clone the repository locally and install with

```
git clone git@git.gfz-potsdam.de:global-land-monitoring/gedi-toolbox.git
cd gedi-toolbox; pip install .
```

## Getting started

### 1. Download, process and store GEDI data to the database
```
from gedidb.database.db_builder import GEDIGranuleProcessor


#%% Initiate database builder
database_builder = GEDIGranuleProcessor(data_config_file = "./config_files/data_config.yml", 
                                        sql_config_file='./config_files/db_scheme.sql')

#%% Process GEDI data
database_builder.compute()

```

### 2. Reading data from the database
```
from gedidb.providers.gedi_provider import GEDIProvider

#%% Instantiate the GEDIProvider
provider = GEDIProvider(config_file='./config_files/data_config.yml',
                        table_name="filtered_l2ab_l4ac_shots")

#%% Define the columns to query and additional parameters
vars_selected = ["rh", "pavd_z", "pai"]
dataset = provider.get_dataset(variables=vars_selected, geometry=None, 
                               start_time="2018-01-01", end_time="2023-12-31", 
                               limit=100, force=True, order_by=["-shot_number"])
```

## Contributing

We welcome contributions to this project. If you would like to contribute, please follow these steps:

- Fork the repository.
- Create a new branch (git checkout -b feature-branch).
- Make your changes.
- Commit your changes (git commit -am 'Add new feature').
- Push to the branch (git push origin feature-branch).
- Create a new Pull Request.

## License
This project is licensed under the EUROPEAN UNION PUBLIC LICENCE v.1.2 License - see the LICENSE file for details.

## Citing geditoolbox

If you use geditoolbox in your research, please use the following BibTeX entry.

```

```
Please note that the default quality filtering is based on the [data processing pipelines](https://docs.google.com/document/d/1XmcoV8-k-8C_Tmh-CJ4sYvlvOqkbiXP1Kah_KrCkMqU/edit) developed by Patrick Burns, Chris Hakkenberg, and Scott Goetz, and should be appropriately credited. Please cite the paper [Repeat GEDI footprints measure the effects of tropical forest disturbances
](https://www.sciencedirect.com/science/article/pii/S0034425724001925?via%3Dihub#f0035) if you end up using the default filters.

## Contact person
For any questions or inquiries, please contact Amelia Holcomb (ah2174@cam.ac.uk), Felix Dombrowski (felix.dombrowski@uni-potsdam.de) and Simon Besnard (besnard@gfz-potsdam.de) 

## Acknowledgments
We acknowledge funding support by the European Union through the FORWARDS (https://forwards-project.eu/) and OpenEarthMonitor (https://earthmonitor.org/) projects. We also would like to thank the R2D2 Workshop (March 2024, GFZ. Potsdam) for providing the opportunity to meet and discuss GEDI data processing.