======================
|gedidblogo| gediDB
======================

.. |gedidblogo| image:: https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox/docs/images/gediDB_logo.png
  :target: https://git.gfz-potsdam.de/global-land-monitoring
  :width: 50px

* Free software: EUPL 1.2
* **Documentation:** https://gedi-toolbox.readthedocs.io/en/latest/
* Information on how to **cite the gediDB Python package** can be found in the
  `CITATION <https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox/-/blob/main/CITATION>`__ file.
* Submit feedback by filing an issue `here <https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox/-/issues>`__

===============================
Toolbox for GEDI data.
===============================
.. image:: https://img.shields.io/static/v1?label=Documentation&message=GitLab%20Pages&color=orange
        :target: https://gedi-toolbox.readthedocs.io/en/latest/
        :alt: Documentation
.. image:: https://zenodo.org/badge/832612594.svg
        :target: https://zenodo.org/doi/10.5281/zenodo.13123060
        :alt: DOI

Getting started
----------------

The **gediDB** is a toolbox to download, process, store and visualise Global Ecosystem Dynamics Investigation (GEDI) L2A-B and L4A-C data

Features
===============

1. Download, process, and store GEDI data to the database:

.. code-block:: python

    import gedidb as gdb

    #%% Initiate database builder
    database_builder = gdb.GEDIProcessor(data_config_file = "./config_files/data_config.yml", 
                                         sql_config_file='./config_files/db_scheme.sql')

    #%% Process GEDI data
    database_builder.compute()

2. Reading data from the database:

.. code-block:: python

    import gedidb as gdb

    #%% Instantiate the GEDIProvider
    provider = gdb.GEDIProvider(config_file='./config_files/data_config.yml',
                            table_name="filtered_l2ab_l4ac_shots",
                            metadata_table="variable_metadata")

    #%% Define the columns to query and additional parameters
    vars_selected = ["rh", "pavd_z", "pai"]
    dataset = provider.get_dataset(variables=vars_selected, geometry=None, 
                                   start_time="2018-01-01", end_time="2023-12-31", 
                                   limit=100, force=True, order_by=["-shot_number"], 
                                   return_type='xarray')

Installation
------------

`Install <https://gedi-toolbox.readthedocs.io/en/latest//installation.html>`_ gediDB


History / Changelog
-------------------

You can find the protocol of recent changes in the gediDB package
`here <https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox/-/blob/main/HISTORY.rst>`__.


Contribution
------------

Contributions are always welcome. Please contact us, if you wish to contribute to the gediDB.


Developed by
------------

.. image:: https://git.gfz-potsdam.de/global-land-monitoring/gedi-toolbox/docs/images/GLM_logo.png
  :target: https://git.gfz-potsdam.de/global-land-monitoring
  :width: 10 %

gediDB has been developed by Simon Besnard (besnard@gfz-potsdam.de) and Felix Dombrowski (felix.dombrowski@uni-potsdam.de) from the `Global Land Monitoring group <https://www.gfz-potsdam.de/sektion/fernerkundung-und-geoinformatik/themen/global-land-monitoring>`_, located at the `Helmholtz Centre Potsdam, GFZ German Research Centre for Geosciences <https://www.gfz-potsdam.de/en/>`_ and Amelia Holcomb (ah2174@cam.ac.uk).

Acknowledgments
------------
We acknowledge funding support by the European Union through the FORWARDS (`https://forwards-project.eu/ <https://forwards-project.eu/>`_) and OpenEarthMonitor (`https://earthmonitor.org/ <https://earthmonitor.org/>`_) projects. We also would like to thank the R2D2 Workshop (March 2024, GFZ, Potsdam) for providing the opportunity to meet and discuss GEDI data processing.

