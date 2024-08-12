<p align="center">
<a href="https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git">
    <img src="https://media.gfz-potsdam.de/gfz/wv/pic/Bildarchiv/gfz/GFZ-CD_LogoRGB_en.png" alt="Master" height="158px" hspace="10px" vspace="0px" align="right">
  </a>
</p>

***
# GEDI toolbox: A toolbox to download, process, store and visualise Global Ecosystem Dynamics Investigation (GEDI) L1B, L2A-B and L4A-C data #

### *by Felix Dombrowski, [Simon Besnard](https://simonbesnard1.github.io/) and [Amelia Holcomb](https://ameliaholcomb.github.io/)*

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Package installation](#package-installation)
- [Contributing](#contributing)
- [License](#license)
- [Citing geditoolbox](#citing-geditoolbox)
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

## Setting up PostgreSQL database

This guide covers the steps to install PostgreSQL on an Ubuntu system, create a new database, and a new user.

### 1. Update the Package List
It's always a good idea to update the package list before installing new software:

```bash
sudo apt update
```

### 2. Install PostgreSQL
Install PostgreSQL and its associated tools:

```bash
sudo apt install postgresql postgresql-contrib
```

### 3. Access the PostgreSQL Prompt
Switch to the `postgres` user and access the PostgreSQL prompt:

```bash
sudo -i -u postgres
psql
```

### 4. Create a New Database
Once inside the PostgreSQL prompt, create a new database:

```sql
CREATE DATABASE mydatabase;
```

Replace `mydatabase` with your desired database name.

### 5. Create a New User
Create a new user with a password:

```sql
CREATE USER myuser WITH PASSWORD 'mypassword';
```

Replace `myuser` with your desired username and `mypassword` with your desired password.

### 6. Grant Privileges to the User
Grant all privileges on the newly created database to the new user:

```sql
GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;
```

### 7. Exit the PostgreSQL Prompt
To exit the PostgreSQL prompt, use the following command:

```bash
\q
```

### 8. Connect to the New Database
To start working with your new database, connect to it using:

```bash
psql -h localhost -U myuser -d mydatabase
```

Replace `localhost`, `myuser`, and `mydatabase` with your server address, username, and database name respectively.

### 9. Optional: PostgreSQL Configuration for Remote Access
If you need to allow remote connections to your PostgreSQL server, you may need to modify the PostgreSQL configuration files.

- Edit the `postgresql.conf` file (usually located in `/etc/postgresql/[version]/main/`) to allow listening on all IP addresses:

```plaintext
listen_addresses = '*'
```

- Edit the `pg_hba.conf` file (in the same directory) to allow connections from specific IP ranges:

```plaintext
host    all             all             0.0.0.0/0               md5
```

- Restart the PostgreSQL service to apply the changes:

```bash
sudo systemctl restart postgresql
```

## Getting started

### Adding the data schema to the database
```


```

### Download, process and store to the database
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

### Reading and visualise the database
```


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
Please note that the default quality filtering is based on the [data processing pipelines](https://docs.google.com/document/d/1XmcoV8-k-8C_Tmh-CJ4sYvlvOqkbiXP1Kah_KrCkMqU/edit) developed by Patrick Burns, Chris Hakkenberg, and Scott Goetz, and should be appropriately credited.

## Contact person
For any questions or inquiries, please contact Felix Dombrowski (felix.dombrowski@uni-potsdam.de), Simon Besnard (besnard@gfz-potsdam.de) and Amelia Holcomb (ah2174@cam.ac.uk)

