=======
History
=======

1.3.0 (2024-08-02)
------------------
* Support all bands
* Add band information to documentation
* Add logo

1.2.4 (2024-07-30)
-------------------
* Create a subset of tests and run the large ones only on main
* Add DOI
* use newer version of geopandas
* only support python >= 3.10

1.2.3 (2024-07-29)
-------------------
* Add logpath to default_config.json
* Add default_config and data to package
* remove non necessary dependencies
* install dependencies via conda if possible

1.2.2 (2024-07-23)
-------------------
* Add GitHub release pipeline to CI.

1.2.1 (2024-07-12)
-------------------
* Add some classifiers for PyPi.
* Fix image URL.

1.2.0 (2024-07-09)
-------------------
* Adapt to PyPi structure.

1.1.0 (2024-07-01)
-------------------
* Support AOI defined as a Polygon.
* Establish tests for SCL-filtering and background masking.

1.0.6 (2024-06-19)
------------------
* Update Pydantic to version 2.* and unpin all the libraries.

1.0.5 (2024-06-18)
------------------
* Allow separate filtering of background|nodata and (non-)valid data

1.0.4 (2024-06-12)
------------------
* Fix bug in SCL-filtering.

1.0.3 (2024-04-25)
------------------
* Filter query for duplicates, keep only the most up to date data in terms of s2:processing.
* Ensure data comparison is valid in terms of processing baselines.

1.0.2 (2024-04-15)
------------------
* Adapt error message and output log.
* Fix installation and requirements.

1.0.1 (2023-09-01)
------------------
* Fix wrong query operators.
* Fix logger bug.

1.0.0 (2023-07-11)
------------------
* Use new catalog (v1) of element84 for downloading.
* Align documentation.
* Pin pydantic package to v1.10.10.

0.4.3 (2023-06-21)
------------------
* Fix bugs of logger.

0.4.2 (2023-06-13)
------------------
* Use stable versions of pystac and pystac-client packages.

0.4.1 (2023-05-23)
------------------
* Add license headers for EUPL license.
* Fix paths and URLS in the documentation.

0.4.0 (2023-03-01)
-------------------
* It is possible to download data either by tileID or AOI.
* Improve memory and disk footprint (the .tifs are now compressed using lzw).
* The AOI width and height upper limit is 500 km.
* Possible to download data for any UTM zone.

0.3.0 (2023-02-10)
------------------
* Fix the a bug with the mosaic, the bounds should come from the new window for the SCL band.
* Schema validation for date_range in the aoi settings.
* Pin Python version to 3.10 for consistent results between C/I and development environments.

0.2.0 (2022-11-23)
------------------
* The .tif files have the following nomenclature: <date>_<sentinel_platform>_<band>.tif
* Scenes information saved into a JSON file.
* Provide logging.
* Split tests per module.
* It is possible to define the target resolution.
* Align the pixels and the extent of each band.
* Add documentation.

0.1.2 (2022-11-16)
------------------
* Change default config file to filter data and cloud coverage only at AOI level.
* Fix bug in AOI coverage query.
* Add information to print if AWS cloud cover is valid.

0.1.1 (2022-11-08)
------------------
* Define the first default config file and its schema.
* First full run with the default config file.
* Improve command line client.

0.1.0 (2022-11-03)
------------------
* Package skeleton as created by https://github.com/danschef/cookiecutter-pypackage.
