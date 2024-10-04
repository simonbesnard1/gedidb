.. _installing:

Installation
============

Required dependencies
----------------------

To use **gediDB**, ensure the following dependencies are installed:

- Python (>= 3.12)
- `numpy <https://numpy.org/>`__ (>= 2.0)
- `packaging <https://packaging.pypa.io/en/latest/>`__ (>= 23.1)
- `pandas <https://pandas.pydata.org/>`__ (>= 2.2)
- `pyarrow <https://arrow.apache.org/>`__ (>= 17.0)
- `geopandas <https://geopandas.org/>`__ (>= 1.0)
- `SQLAlchemy <https://www.sqlalchemy.org/>`__ (>= 2.0)
- `GeoAlchemy2 <https://geoalchemy-2.readthedocs.io/en/latest/>`__ (>= 0.15)
- `h5py <https://www.h5py.org/>`__ (>= 3.11)
- `psycopg2 <https://www.psycopg.org/>`__ (>= 2.9)
- `xarray <https://xarray.pydata.org/>`__ (>= 2024.7.0)
- `retry <https://github.com/invl/retry>`__ (>= 0.9)
- `dask <https://dask.org/>`__ (>= 2024.8.2)
- `distributed <https://distributed.dask.org/>`__ (>= 2024.8.2)

Optional dependencies
----------------------

If you're looking to enable additional functionality, gediDB provides optional dependencies:

- **PostGIS** for geospatial operations within **PostgreSQL**
- **matplotlib** and **seaborn** for data visualization
- **netCDF4** for working with netCDF data formats

.. note::

  Optional dependencies can be installed via *extras*. See the instructions below on how to install these dependencies via **pip** or **conda**.

Installation instructions
-------------------------

gediDB is a pure Python package, but some of its dependencies have complex installations. We recommend using **conda** for the easiest setup. 

To install **gediDB** and its dependencies using conda (from the **conda-forge** channel):

.. code-block:: bash

    $ conda install -c conda-forge gedidb

If you are not using **conda**, first ensure that you have **numpy** and **pandas** installed, then use **pip** to install **gediDB**:

.. code-block:: bash

    $ python -m pip install gedidb

If you want to include optional dependencies, you can specify them using **pip** extras. For example:

.. code-block:: bash

    $ python -m pip install gedidb[full]

This will install additional dependencies specified in the `pyproject.toml` file under `[project.optional-dependencies]`.

Development versions
--------------------

If you wish to install the latest development version of **gediDB**, you can install it directly from the **GitLab** repository:

.. code-block:: bash

    $ python -m pip install git+https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox.git

Testing
-------

To run the test suite after installing **gediDB**, install the **pytest** library:

.. code-block:: bash

    $ python -m pip install pytest

Then, run the tests by navigating to the root directory of the **gediDB** repository and running:

.. code-block:: bash

    $ pytest
