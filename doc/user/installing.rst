.. _installing:

Installation
============

Dependencies
------------

gediDB requires Python >= 3.10 and the following dependencies. These are resolved automatically when installing via **pip**:

+------------+-----------------+--------------------------------------------+
| Dependency | Minimum Version | Link                                       |
+============+=================+============================================+
| boto3      | 1.35.49         | https://pypi.org/project/boto3/            |
+------------+-----------------+--------------------------------------------+
| dask       | 2024.8.2        | https://dask.org/                          |
+------------+-----------------+--------------------------------------------+
| distributed| 2024.8.2        | https://distributed.dask.org/              |
+------------+-----------------+--------------------------------------------+
| geopandas  | 1.0.1           | https://geopandas.org/                     |
+------------+-----------------+--------------------------------------------+
| h5py       | 3.11            | https://www.h5py.org/                      |
+------------+-----------------+--------------------------------------------+
| numpy      | 2.0.1           | https://numpy.org/                         |
+------------+-----------------+--------------------------------------------+
| pandas     | 2.2.2           | https://pandas.pydata.org/                 |
+------------+-----------------+--------------------------------------------+
| requests   | 2.32.3          | https://requests.readthedocs.io/en/latest/ |
+------------+-----------------+--------------------------------------------+
| retry      | 0.9.2           | https://pypi.org/project/retry/            |
+------------+-----------------+--------------------------------------------+
| scipy      | 1.14.1          | https://scipy.org/                         |
+------------+-----------------+--------------------------------------------+
| tiledb     | 0.33            | https://pypi.org/project/tiledb/           |
+------------+-----------------+--------------------------------------------+
| xarray     | 2024.7.0        | https://xarray.pydata.org/                 |
+------------+-----------------+--------------------------------------------+


Optional Dependencies
----------------------

For additional functionality, the following optional dependencies are available:

+-------------+-----------------------------------------------------------+
| Dependency  | Purpose                                                   |
+=============+===========================================================+
| matplotlib  | Enhanced data visualization                               |
+-------------+-----------------------------------------------------------+
| netCDF4     | Support for netCDF data formats                           |
+-------------+-----------------------------------------------------------+
| seaborn     | Statistical data visualization                            |
+-------------+-----------------------------------------------------------+

To install optional dependencies, use **pip** with the extras syntax (e.g., `pip install gedidb[full]`).

Installation Instructions
-------------------------

Install via **pip**:

.. code-block:: bash

    $ pip install gedidb

To include optional dependencies:

.. code-block:: bash

    $ pip install gedidb[full]

Development Versions
--------------------

To install the latest development version from GitLab:

.. code-block:: bash

    $ pip install git+https://git.gfz-potsdam.de/global-land-monitoring/gedidb.git

Testing
-------

To run tests after installing gediDB, first install **pytest**:

.. code-block:: bash

    $ pip install pytest

Navigate to the root directory of the gediDB repository, then run:

.. code-block:: bash

    $ pytest
