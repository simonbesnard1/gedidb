.. _installing:

Installation
============

Required dependencies
---------------------

To use gediDB, please ensure the following dependencies are installed:

- Python >= 3.12
- `numpy <https://numpy.org/>`__ >= 2.0
- `packaging <https://packaging.pypa.io/en/latest/>`__ >= 23.1
- `pandas <https://pandas.pydata.org/>`__ >= 2.2
- `pyarrow <https://arrow.apache.org/>`__ >= 17.0
- `geopandas <https://geopandas.org/>`__ >= 1.0
- `SQLAlchemy <https://www.sqlalchemy.org/>`__ >= 2.0
- `GeoAlchemy2 <https://geoalchemy-2.readthedocs.io/en/latest/>`__ >= 0.15
- `h5py <https://www.h5py.org/>`__ >= 3.11
- `psycopg2 <https://www.psycopg.org/>`__ >= 2.9
- `xarray <https://xarray.pydata.org/>`__ >= 2024.7.0
- `retry <https://github.com/invl/retry>`__ >= 0.9
- `dask <https://dask.org/>`__ >= 2024.8.2
- `distributed <https://distributed.dask.org/>`__ >= 2024.8.2

Optional dependencies
---------------------

For additional functionality, consider installing these optional dependencies:

- **PostGIS**: Provides geospatial functions in **PostgreSQL**
- **matplotlib** and **seaborn**: For enhanced data visualization
- **netCDF4**: For working with netCDF data formats

.. note::

   Optional dependencies can be installed via *extras* using **pip**. See the instructions below on how to install these via **pip** or **conda**.

Installation instructions
-------------------------

GediDB is a pure Python package, though some dependencies may have complex installations. We recommend using **conda** for the most straightforward setup.

To install gediDB and all required dependencies with **conda** from the **conda-forge** channel, use:

.. code-block:: bash

    $ conda install -c conda-forge gedidb

For non-conda installations, first ensure **numpy** and **pandas** are installed, then use **pip** to install gediDB:

.. code-block:: bash

    $ python -m pip install gedidb

To include optional dependencies, specify them using **pip** extras. For example:

.. code-block:: bash

    $ python -m pip install gedidb[full]

This command installs additional dependencies listed in the `pyproject.toml` file under `[project.optional-dependencies]`.

Development versions
--------------------

For the latest development version of gediDB, install directly from the **GitLab** repository:

.. code-block:: bash

    $ python -m pip install git+https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git

Testing
-------

To run tests after installing gediDB, first install **pytest**:

.. code-block:: bash

    $ python -m pip install pytest

Navigate to the root directory of the gediDB repository, then run:

.. code-block:: bash

    $ pytest
