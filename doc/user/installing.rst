.. _installing:

Installation
============

Requirements
------------

* Python >= 3.10

Runtime dependencies are declared in ``pyproject.toml`` with **minimum compatible versions** and are
resolved automatically when installing via pip.


User installation
-----------------

Install the latest release from PyPI:

.. code-block:: bash

    python -m pip install gedidb


Optional features
-----------------

Install optional components using pip “extras”:

* **Full stack (IO + viz + performance + logging)**

  .. code-block:: bash

      python -m pip install "gedidb[full]"

* Mix-and-match individual extras (advanced):

  - ``accel`` — performance helpers (bottleneck, numba, etc.)
  - ``io`` — cloud/filesystem and raster IO (s3fs, zarr, rasterio, pyproj, etc.)
  - ``viz`` — plotting (matplotlib, cartopy, seaborn, etc.)
  - ``debug`` — rich logging

  Example:

  .. code-block:: bash

      python -m pip install "gedidb[io,viz]"


External system libraries
-------------------------

Some geospatial backends rely on native libraries:

* **GDAL / PROJ / GEOS** — Recommended to install via a conda-forge environment if you are not
  already set up with these on your system:

  .. code-block:: bash

      mamba create -n gedidb -c conda-forge python>=3.10 gdal proj geos
      mamba activate gedidb
      python -m pip install "gedidb[full]"

* **wget** — used by ``EarthDataAuthenticator`` for downloads. This is preinstalled on most Linux/macOS;
  on Windows you can install via ``winget`` or ``choco``.


Development setup
-----------------

Clone and install an editable development environment with tooling:

.. code-block:: bash

    git clone https://github.com/simonbesnard1/gedidb.git
    cd gedidb

    # (optional) create and activate a virtual environment
    python -m venv .venv
    source .venv/bin/activate   # Windows: .venv\Scripts\activate

    python -m pip install -U pip wheel
    # editable install with dev tools (pytest, ruff, mypy, pre-commit, etc.)
    python -m pip install -e ".[dev,full]"
    pre-commit install


Install latest development snapshot directly from GitHub (no clone):

.. code-block:: bash

    python -m pip install "git+https://github.com/simonbesnard1/gedidb.git"


Testing
-------

From the project root:

.. code-block:: bash

    pytest

With coverage:

.. code-block:: bash

    pytest --cov=gedidb --cov-report=term-missing

If you maintain integration tests that require credentials or large datasets, mark them with
``@pytest.mark.integration`` and run selectively:

.. code-block:: bash

    pytest -m "not integration"
