.. _installing:

Installation
============

Required dependencies
---------------------

- Python (3.12 or later)
- `numpy <https://www.numpy.org/>`__ (2.0 or later)
- `packaging <https://packaging.pypa.io/en/latest/#>`__ (23.1 or later)
- `pandas <https://pandas.pydata.org/>`__ (2.2 or later)
- `pyarrow  <https://pandas.pydata.org/>`__ (17.0 or later)
- `geopandas  <https://geopandas.pydata.org/>`__ (1.0 or later)
- `SQLAlchemy  <https://SQLAlchemy.pydata.org/>`__ (2.0 or later)
- `GeoAlchemy2  <https://GeoAlchemy2.pydata.org/>`__ (0.15 or later)
- `h5py  <https://h5py.pydata.org/>`__ (3.11 or later)
- `psycopg2  <https://psycopg2.pydata.org/>`__ (2.9 or later)
- `xarray  <https://xarray.pydata.org/>`__ (2024.7.0 or later)
- `retry  <https://retry.pydata.org/>`__ (0.9 or later)
- `dask  <https://dask.pydata.org/>`__ (2024.8.2 or later)
- `distributed  <https://distributed.pydata.org/>`__ (2024.8.2 or later)

.. _optional-dependencies:

Optional dependencies
---------------------

.. note::

  If you are using pip to install gediDB, optional dependencies can be installed by
  specifying *extras*. :ref:`installation-instructions` for both pip and conda
  are given below.

.. _installation-instructions:

Instructions
------------

gedidb itself is a pure Python package, but its dependencies are not. The
easiest way to get everything installed is to use conda_. To install xarray
with its recommended dependencies using the conda command line tool::

    $ conda install -c conda-forge gedidb

.. _conda: https://docs.conda.io

If you require other :ref:`optional-dependencies` add them to the line above.

We recommend using the community maintained `conda-forge <https://conda-forge.org>`__ channel,
as some of the dependencies are difficult to build. New releases may also appear in conda-forge before
being updated in the default channel.

If you don't use conda, be sure you have the required dependencies (numpy and
pandas) installed first. Then, install xarray with pip::

    $ python -m pip install gedidb

The above commands should install most of the `optional dependencies`_. However,
some packages which are either not listed on PyPI or require extra
installation steps are excluded. To know which dependencies would be
installed, take a look at the ``[project.optional-dependencies]`` section in
``pyproject.toml``:

.. literalinclude:: ../../pyproject.toml
   :language: toml
   :start-at: [project.optional-dependencies]
   :end-before: [build-system]

Development versions
--------------------
To install the most recent development version, install from github::

     $ python -m pip install git+https://github.com/pydata/xarray.git

Testing
-------

To run the test suite after installing xarray, install (via pypi or conda) `py.test
<https://pytest.org>`__ and run ``pytest`` in the root directory of the xarray
repository.

