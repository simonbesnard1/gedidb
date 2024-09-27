.. for doctest:
    >>> import gedidb as gdb

.. _basics.authenticate:

#####################
NASA authentification
#####################


CMR login credentials
---------------------

To download the needed ``.h5`` files from NASA's Common Metadata Repository (CMR),
you need a NASA Earthdata user account.
An Earthdata account can be created `here <https://urs.earthdata.nasa.gov/>`_.
Your credentials must be inserted into the ``data_config.yml`` for the package to work.

::

    earth_data_info:
        credentials:
            username: 'your_username'
            password: 'your_password'

Authenticating your credentials
-------------------------------

The gediDB package comes with an included function to check your credentials and save them to a ``.netrc``
file for later use. The provided code snippet can be used to authenticate your credentials.

::

    from gedidb.downloader.authentication import EarthDataAuthenticator

    authenticator = EarthDataAuthenticator('./path/to/data_config.yml')
    authenticator.authenticate()

If successful, the logger will give you the following output.

::

    2024-09-27 15:34:08,253 - INFO - Directory ensured: data\earth_data
    2024-09-27 15:34:08,253 - INFO - No authentication cookies found, fetching Earthdata cookies...
    2024-09-27 15:34:08,253 - INFO - Credentials added to .netrc file.
    2024-09-27 15:34:08,253 - INFO - Earthdata cookies successfully fetched and saved.
