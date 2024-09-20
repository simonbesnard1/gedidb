.. _usage:

Usage
=====

Usage of the Python API
***********************

To use S2Downloader in a project::

    import os
    import s2downloader
    from s2downloader.s2downloader import s2DataDownloader
    from s2downloader.config import loadConfiguration, Config

    config_file = os.path.abspath("../data/default_config.json")
    config = loadConfiguration(path=config_file)

    Config(**config)
    s2DataDownloader(config_dict=config)



*Note:*

Since version 1.0.0 s2Downloader uses the new version of element84's catalog for querying through the downloadable data. If you want to use the old version
of the catalog (v0) for any reason, fall back to version 0.4.3 of s2Downloader. More information about the `catalog changes <https://www.element84.com/blog/introducing-earth-search-v1-new-datasets-now-available>`_.

----

Command line utilities
**********************

s2downloader_cli.py
-------------------

At the command line, S2Downloader provides the **s2downloader_cli.py**. Run with relative or absolute path to config json file:
::

    S2Downloader --filepath "path/to/config.json"


Input and Output
****************


Expected Input Configuration
----------------------------

The package expects a configuration file in ``json`` format, like the `default_config.json <https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/-/blob/main/data/default_config.json>`_ in the repository. A valid configuration for downloading data might look like follows:

.. code-block:: json

        {
        "user_settings": {
            "tile_settings": {
                "platform": {
                    "in": [
                        "sentinel-2b",
                        "sentinel-2a"
                    ]
                },
                "s2:nodata_pixel_percentage": {
                    "lte": 100
                },
                "mgrs:utm_zone": {},
                "mgrs:latitude_band": {},
                "mgrs:grid_square": {},
                "eo:cloud_cover": {
                    "lte": 100
                },
                "bands": [
                    "coastal",
                    "blue",
                    "rededge1"
                ]
            },
            "aoi_settings": {
                "bounding_box": [
                    13.058397,
                    52.376620,
                    13.073049,
                    52.383835
                ],
                "apply_SCL_band_mask": true,
                "SCL_filter_values": [
                    3,
                    7,
                    8,
                    9,
                    10
                ],
                "aoi_min_coverage": 90,
                "SCL_masked_pixels_max_percentage": 20,
                "valid_pixels_min_percentage": 10,
                "resampling_method": "cubic",
                "date_range": [
                    "2021-09-04",
                    "2021-09-05"
                ]
            },
            "result_settings": {
                "results_dir": "data/data_output/",
                "target_resolution": 10,
                "download_data": true,
                "download_thumbnails": false,
                "download_overviews": false,
                "logging_level": "INFO"
            }
        },
        "s2_settings": {
            "collections": [
                "sentinel-2-l2a"
            ],
            "tiles_definition_path": "data/sentinel_2_index_shapefile_attr.zip"
        }
    }



In the following, the parameter configuration is described in detail:

User Settings
-------------

Tile Settings
#############

**Note:** To get the correct UTM zone, latitude band and grid square for downloading complete tiles either the tile grid `sentinel_2_index_shapefile_attr.zip <https://git.gfz-potsdam.de/fernlab/products/data-portal/s2downloader/-/blob/main/data/sentinel_2_index_shapefile_attr.zip>`_ can be displayed in a GIS or this `web map <https://eatlas.org.au/data/uuid/f7468d15-12be-4e3f-a246-b2882a324f59>`_ can be used.

.. list-table::
    :header-rows: 1
    :class: tight-table

    * - Parameter
      - Description
      - Examples
    * - ``Platform``
      - Which satellite to use. Default: both A and B.
      - ``"platform" : {"in": ["sentinel-2b", "sentinel-2a"]}``
    * - ``NoData pixel percentage``
      - Defines how many pixels of a  requested Sentinel-2 tile have NoData values. Leave empty to only validate the AOI for data coverage.
      - ``"s2:nodata_pixel_percentage": {"eq": 0}``, ``"s2:nodata_pixel_percentage": {"lt": 80}``
    * - ``UTM zone``
      - UTM zone. Must be a list with one or more integers from 1 to 60 or empty if AOI is provided. Example: "32" for tile 32UQC.
      - ``"mgrs:utm_zone": {"in": 33}``
    * - ``Latitude Band``
      - The latitude band of a preferred UTM zone. Empty if AOI is provided. Example: "U" for tile 32UQC.
      - ``"mgrs:latitude_band": {"eq": "N"}``
    * - ``Grid Square``
      - The grid square to specify the tile. Empty if AOI is provided. Example: "QC" for tile 32UQC.
      - ``"mgrs:grid_square": {"in": ["RK", "TE"]``
    * - ``eo:cloud_cover``
      - The amount of clouds that are allowed at the **entire** Sentinel-2 scene. Leave empty to only validate the AOI for cloud coverage.
      - ``"eo:cloud_cover": {"eq": 0}``, ``"eo:cloud_cover": {"lt": 20}``
    * - ``bands``
      - Defines which Sentinel-2 bands to download. You may choose from these options: ["coastal", "blue", "green", "red", "rededge1", "rededge2", "rededge3", "nir", "nir08", "nir09", "cirrus", "swir16", "swir22"]. See the table below for more information about the bands.
      - ``"bands": ["coastal", "rededge1", "nir"]``

Sentinel-2 Bands
================

.. list-table:: Sentinel-2 Bands Information
   :widths: 15 35 30 20
   :header-rows: 1

   * - Band Name
     - Description
     - Wavelength (µm)
     - Resolution (m)
   * - coastal
     - Coastal aerosol (band 1)
     - 0.433
     - 60
   * - blue
     - Blue (band 2)
     - 0.49
     - 10
   * - green
     - Green (band 3)
     - 0.56
     - 10
   * - red
     - Red (band 4)
     - 0.665
     - 10
   * - rededge1
     - Vegetation red edge 1 (band 5)
     - 0.704
     - 20
   * - rededge2
     - Vegetation red edge 2 (band 6)
     - 0.74
     - 20
   * - rededge3
     - Vegetation red edge 3 (band 7)
     - 0.783
     - 20
   * - nir
     - NIR 1 (band 8)
     - 0.842
     - 10
   * - nir08
     - Narrow NIR (band 8A)
     - 0.865
     - 20
   * - nir09
     - Water vapour (band 9)
     - 0.945
     - 60
   * - cirrus
     - SWIR - Cirrus (band 10)
     - 1.3735
     - 60
   * - swir16
     - SWIR 1 (band 11)
     - 1.61
     - 20
   * - swir22
     - SWIR 2 (band 12)
     - 2.19
     - 20

AOI Settings
############

**Note:** Please make sure that you either provide only a Bounding Box or an AOI. A Bounding Box will always be four coordinates, a rectangle and in parallel with lon/lat. A polygon can have multiple shapes and orientations.

.. list-table::
    :header-rows: 1
    :class: tight-table

    * - Parameter
      - Description
      - Examples
    * - ``Bounding Box``
      - The BoundingBox in lon/lat format.
      - ``"bounding_box": [13.058397, 52.376620, 13.073049, 52.383835]``
    * - ``Polygon``
      - A polygon which defines an AOI to which the results are cropped. The format is one geometry feature of a geojson in lon/lat coordinates.
      - ``"polygon": {"coordinates": [[[12.438319245776597, 52.41747810004975], [12.404920243911363, 52.38094275671861], [12.479968200292518, 52.37964316533544], [12.438319245776597, 52.41747810004975]]], "type": "Polygon"}``
    * - ``apply_SCL_band_mask``
      - Boolean Variable. If set to true the SCL band of Sentinel-2 is used to mask out pixels. The SCL band is saved along to an extra file.
      - ``"apply_SCL_band_mask": true``
    * - ``SCL_filter_values``
      - List of integer-Values corresponding to the SCL classes. It's default classes are: cloud shadow (class 3), clouds (classes 7, 8, 9) and thin cirrus (class 10).
      - ``"SCL_filter_values": [3, 7, 8, 9, 10]"``
    * - ``aoi_min_coverage``
      - User defined threshold for noData values inside the AOI. It may happen due to Sentinel-2 data tile structure that parts of the AOI have noData values. Here the user can define a percentage value of minimum valid pixels inside the AOI.
      - ``"aoi_min_coverage": 90``
    * - ``SCL_masked_pixels_max_percentage``
      - User defined threshold for the SCL masked values inside the AOI. Here the user can define a percentage value of maximum masked pixels after masking in order to save the image. As an example, allow at most 20% cloud coverage in the image.
      - ``"SCL_masked_pixels_max_percentage": 20``
    * - ``valid_pixels_min_percentage``
      - If cloud masking based on the SCL band is applied, it may happen that images are saved which contain only very few valid pixels. Here the user can define a percentage value of minimum valid pixels that should be left over after masking in order to save the image.
      - ``"valid_pixels_min_percentage": 70``
    * - ``resampling_method``
      - User definition of the resampling method that should be used. Currently, these options are supported: nearest, bilinear, cubic.
      - ``"resampling_method": "nearest"``, ``"resampling_method": "bilinear"``, ``"resampling_method": "cubic"``
    * - ``date_range``
      - The period of time data should be looked for, defined by starting and end date. It is also possible to provide just a single day.
      - ``"date_range": ["2021-09-04", "2021-09-05"]``

Result Settings
###############

.. list-table::
    :header-rows: 1
    :class: tight-table

    * - Parameter
      - Description
      - Examples
    * - ``results_dir``
      - Output directory to which the downloaded data should be saved to.
      - ``"results_dir": "data_output/"``
    * - ``target_resolution``
      - The spatial resolution the output tif file(s) should have in meters. It should be either 10, 20 or 60.
      - ``"target_resolution": 10``
    * - ``download_data``
      - Boolean variable, If set to true the scenes are downloaded. If set to false only a list of available data is saved as a JSON file but no data is downloaded.
      - ``"download_data": true``
    * - ``download_thumbnails``
      - Boolean variable. If this parameter is set to true the thumbnail for each available scenes is downloaded.
      - ``"download_thumbnails": false``
    * - ``download_overviews``
      - Boolean variable. If this parameter is set to true the overview for each available scenes is downloaded.
      - ``"download_overviews": false``
    * - ``logging_level``
      - Logging level, it should be one of: DEBUG, INFO, WARN, or ERROR.
      - ``"logging_level": "INFO"``


S2 Settings
-----------

**Note:** The S2 settings are not to be altered by the user!

.. list-table::
    :header-rows: 1
    :class: tight-table

    * - Parameter
      - Description
      - Examples
    * - ``collections``
      - The Sentinel-2 preprocessing level of data to be downloaded. Currently only the S2 L2A download is tested.
      - ``"collections": ["sentinel-2-l2a"]``
    * - ``"tiles_definition_path``
      - Path to the tile grid of Sentinel-2 data.
      - ``"tiles_definition_path": "data/sentinel_2_index_shapefile_attr.zip"``

Expected Output
---------------

With Bounding Box or AOI
########################

The following files are saved within the defined output folder:

.. code-block::

  - <date_sensor_band>.tif
  - <date_sensor>_SCL.tif
  - <sensor_tile_date>_0_L2A_TCI.tif
  - <sensor_tile_date>_0_L2A_thumbnail.jpg
  - s2DataDownloader.log
  - scenes_info_<daterange>.json

**date_sensor_band.tif**
The tif file of each band. Example: 20210905_S2B_coastal.tif for date 2021-09-05, sensor B and band 1.

**date_sensor_SCL.tif**
The tif file for the scl band of the according date. Example: 20210905_S2B_SCL.tif

**sensor_tile_date_0_L2A_L2A_PVI.tif**
If "download_overviews" is set to true this file contains the overview per sensor, tile and date. Example: S2B_33UUU_20210908_0_L2A_TCI.tif

**sensor_tile_date_0_L2A_preview.jpg**
If "download_thumbnails" is set to true this file contains the thumbnail per sensor, tile and date. Example: S2B_33UUU_20210908_0_L2A_thumbnail.jpg

**s2DataDownloader.log**
The log file containing all logs. The logging level can be set in the result settings in the config.json.

**scenes_info_daterange.json**
The information about the scenes for a certain date range. Example: scenes_info_2021-09-04_2021-09-05.json.

.. code-block:: json

    {
        "20210905": {
            "item_ids": [
                {
                    "id": "S2B_33UUU_20210905_0_L2A"
                }
            ],
            "nonzero_pixels": [
                100.0
            ],
            "valid_pixels": [
                100.0
            ],
            "data_available": [
                true
            ],
            "error_info": [
                ""
            ]
        }
    }

For each date the following information is saved:

**item_ids:** The items (scenes) found at aws for that date.

**nonzero_pixels:** Percentage of pixels with non zero values.

**valid_pixels:** Percentage of pixels with valid data.

**data_available:** If false no data for this date was found.

**error_info:** If any error occurred during the download the error message will be saved here.

With tile ID
############

The difference when downloading complete tiles is that they are sorted in multiple folders with the following structure::

    <utm_zone>
      <latitude_band>
        <grid_square>
          <year>
            <month>
              <platform>_<product_level>_<time>_<processing_baseline_number>_<relative_orbit_number>_<tile_number>_<product_id>
                <band_number>.tif

_______
