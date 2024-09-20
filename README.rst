======================
|s2logo| S2Downloader
======================

.. |s2logo| image:: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/images/s2downloader_logo.svg
  :target: https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader
  :width: 50px

* Free software: EUPL 1.2
* **Documentation:** https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/doc/
* Information on how to **cite the S2Downloader Python package** can be found in the
  `CITATION <https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/-/blob/main/CITATION>`__ file.
* Submit feedback by filing an issue `here <https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/issues>`__

===============================
Downloader for Sentinel-2 data.
===============================

.. image:: https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/badges/main/pipeline.svg
        :target: https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/pipelines
        :alt: Pipelines
.. image:: https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/badges/main/coverage.svg
        :target: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/coverage/
        :alt: Coverage
.. image:: https://img.shields.io/static/v1?label=Documentation&message=GitLab%20Pages&color=orange
        :target: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/doc/
        :alt: Documentation
.. image:: https://zenodo.org/badge/832612594.svg
        :target: https://zenodo.org/doi/10.5281/zenodo.13123060
        :alt: DOI


Feature overview
----------------

The **S2Downloader** allows to download Sentinel-2 L2A data from the cost-free `element84 AWS <https://registry.opendata.aws/sentinel-2-l2a-cogs/>`_ Amazon Cloud server. It specifically serves the purpose to download data for user-defined area of interests (AOI), defined by a bounding box or whole tiles which are the original data product provided by ESA.

Features
########

**Features on Sentinel-2 tile level**

* download atmospheric corrected L2A Sentinel-2 thumbnail, overview, and data from AWS
* provide single date or time range for finding data at the server
* select which individual bands to download, all bands are supported: ``"coastal"``, ``"blue"``, ``"green"``, ``"red"``, ``"rededge1"``, ``"rededge2"``, ``"rededge3"``, ``"nir"``, ``"nir08"``, ``"nir09"``, ``"cirrus"``, ``"swir16"``, ``"swir22"``
* provide UTM zone, latitude band and grid square to download whole tiles


**Features on AOI level**

* input data: configuration json file
* customizable filtering for noData
* optional: customizable mask SCL classes (default masked classes: clouds shadow, clouds, cirrus -> 3, 7, 8, 9, 10), all available classes:

  * 0 - No data
  * 1 - Saturated / Defective
  * 2 - Dark Area Pixels
  * 3 - Cloud Shadows
  * 4 - Vegetation
  * 5 - Bare Soils
  * 6 - Water
  * 7 - Clouds low probability / Unclassified
  * 8 - Clouds medium probability
  * 9 - Clouds high probability
  * 10 - Cirrus
  * 11 - Snow / Ice


* mosaic data from different tiles (same utm zone) into one tif file
* resample bands to user-defined target resolution
* select resampling method

**Features for saving the results**

* define output location
* save thumbnails for the available scenes
* save overviews for the available scenes


Installation
------------

`Install <https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/doc/installation.html>`_ S2Downloader


Usage
-----

Run with relative or absolute path to config json file:
::

    S2Downloader --filepath "path/to/config.json"

Relative paths in the config file are supposed to be relative to the location of the repository.

See `usage <https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/doc/usage.html>`_ for more details about the config file.

Expected Output
---------------

The downloaded raster files and overviews are saved in .tif format, the thumbnails are saved as .jpg. Additional information is saved in a.json and a logging file.

See `usage <https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/doc/usage.html>`_ for more details about the output files.

History / Changelog
-------------------

You can find the protocol of recent changes in the S2Downloader package
`here <https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/-/blob/main/HISTORY.rst>`__.


Contribution
------------

Contributions are always welcome. Please contact us, if you wish to contribute to the S2Downloader.


Developed by
------------

.. image:: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/s2downloader/images/fernlab_logo.svg
  :target: https://fernlab.gfz-potsdam.de/
  :width: 10 %

S2Downloader has been developed by `FERN.Lab <https://fernlab.gfz-potsdam.de/>`_, the Helmholtz Innovation Lab "Remote sensing for sustainable use of resources", located at the `Helmholtz Centre Potsdam, GFZ German Research Centre for Geosciences <https://www.gfz-potsdam.de/en/>`_. FERN.Lab is funded by the `Initiative and Networking Fund of the Helmholtz Association <https://www.helmholtz.de/en/about-us/structure-and-governance/initiating-and-networking/>`_.


Credits
------------

This package was created with Cookiecutter_ and the `fernlab/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`fernlab/cookiecutter-pypackage`: https://github.com/fernlab/cookiecutter-pypackage
.. _coverage: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/sentinel2_portal/coverage/
.. _pytest: https://fernlab.git-pages.gfz-potsdam.de/products/data-portal/sentinel2_portal/test_reports/report.html
.. _default_config.json: https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/-/blob/main/data/default_config.json

