# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-License-Identifier: EUPL-1.2 
# Version :   1.0
# Contact :   besnard@gfz-potsdam.de

import os
import re
import setuptools

PKG_NAME = "GEDItools"

HERE = os.path.abspath(os.path.dirname(__file__))

PATTERN = r'^{target}\s*=\s*([\'"])(.+)\1$'

AUTHOR = re.compile(PATTERN.format(target='__author__'), re.M)
VERSION = re.compile(PATTERN.format(target='__version__'), re.M)
LICENSE = re.compile(PATTERN.format(target='__license__'), re.M)
AUTHOR_EMAIL = re.compile(PATTERN.format(target='__author_email__'), re.M)


def parse_init():
    with open(os.path.join(HERE, PKG_NAME, '__init__.py')) as f:
        file_data = f.read()
        print(file_data)
    return [regex.search(file_data).group(2) for regex in
            (AUTHOR, VERSION, LICENSE, AUTHOR_EMAIL)]


with open("README.md", "r") as fh:
    long_description = fh.read()

author, version, license, author_email = parse_init()

setuptools.setup(
    name=PKG_NAME,
    author=author,
    author_email=author_email,
    license=license,
    version=version,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox",
    packages=setuptools.find_packages(include=['GEDItools',
                                               'GEDItools.*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: EUROPEAN UNION PUBLIC LICENCE v.1.2 (EUPL-1.2)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.12',
    install_requires=[
        "pandas==2.2.2",
        "pyarrow==17.0.0",
        "geopandas==1.0.1",
        "SQLAlchemy==2.0.31",
        "GeoAlchemy2==0.15.2",
        "h5py==3.11.0",
        "numpy==2.0.1",
        "pyspark==3.5.1",
        "psycopg2==2.9.9"
        ],
    include_package_data=True,
)
