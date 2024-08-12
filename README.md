<p align="center">
<a href="https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git">
    <img src="https://media.gfz-potsdam.de/gfz/wv/pic/Bildarchiv/gfz/GFZ-CD_LogoRGB_en.png" alt="Master" height="158px" hspace="10px" vspace="0px" align="right">
  </a>
</p>

***
# GEDI toolbox: A toolbox to download, process, store and visualise Global Ecosystem Dynamics Investigation (GEDI) L1B, L2A-B and L4A-C data #

### *by [Amelia Holcomb](https://ameliaholcomb.github.io/), Felix Dombrowski and [Simon Besnard](https://simonbesnard1.github.io/)*

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Package installation](#package-installation)
- [Contributing](#contributing)
- [License](#license)
- [Citing forest-age-upscale](#citing-forest-age-upscale)
- [Contact person](#contact-person)

## Overview
This repository contains the code and data associated to download, process, store and visualise GEDI L1B, L2A-B and L4A-C data.

## Repository Structure

```plaintext
├── config_files
├── data
├── GEDItools 		# Core of the package
│   ├── database
│   ├── downloader
│   ├── processor
│   ├── provider
│   ├── utils
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
cd gedi-toolbox; pip install -e .
```

## Getting started

Download, process and store to the database
```
from GEDItools.database.db_builder import GEDIGranuleProcessor


#%% Initiate database builder
database_builder = GEDIGranuleProcessor(database_config_file = './config_files/database_params.yml', 
                                        column_to_field_config_file = './config_files/column_to_field.yml',
                                        quality_config_file = './config_files/quality_filters.yml',
                                        field_mapping_config_file = './config_files/field_mapping.yml')

#%% Process GEDI data
database_builder.compute()

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

## Contact person
For any questions or inquiries, please contact Amelia Holcomb (ah2174@cam.ac.uk), Felix Dombrowski (felix.dombrowski@uni-potsdam.de) and Simon Besnard (besnard@gfz-potsdam.de)

