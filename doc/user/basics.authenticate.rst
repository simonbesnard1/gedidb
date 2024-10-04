.. _basics.authenticate:

#####################
NASA Authentication
#####################

To interact with NASA's Common Metadata Repository (CMR) and download the required ``.h5`` files, **gediDB** uses NASA Earthdata credentials for authentication. This guide will help you set up the authentication process and ensure your credentials are properly stored and used.

CMR login credentials
---------------------

To access GEDI data hosted on NASA’s servers, you need an **Earthdata** account. If you don't already have an account, you can create one at the following link:

`Create an Earthdata Account <https://urs.earthdata.nasa.gov/>`_

Once your Earthdata account is created, add your credentials (username and password) to the **gediDB** configuration file (`data_config.yml`) to enable access. This file serves as the central hub for all configuration parameters, including authentication details.

Example configuration for Earthdata credentials:

::

    earth_data_info:
        credentials:
            username: 'your_username'
            password: 'your_password'

Make sure to replace `'your_username'` and `'your_password'` with your actual Earthdata credentials.

Authenticating your credentials
-------------------------------

To avoid typing your credentials repeatedly, **gediDB** provides a built-in function that saves your login information securely in a ``.netrc`` file, which is then used to automatically authenticate your requests to NASA’s servers. 

Use the following code snippet to authenticate your credentials and store them in the ``.netrc`` file:

.. code-block:: python

    from gedidb.downloader.authentication import EarthDataAuthenticator

    authenticator = EarthDataAuthenticator('./path/to/data_config.yml')
    authenticator.authenticate()

Explanation:
- `EarthDataAuthenticator` is used to handle your credentials and automatically authenticate them.
- The function reads your credentials from the `data_config.yml` file and ensures they are valid.
- It then stores the credentials in the ``.netrc`` file for future use.

Successful Authentication
--------------------------

After successful authentication, the system will log helpful messages in the console to let you know that your credentials have been successfully stored and verified. Here’s an example of what you should expect:

.. code-block:: none

    2024-09-27 15:34:08,253 - INFO - Directory ensured: data/earth_data
    2024-09-27 15:34:08,253 - INFO - No authentication cookies found, fetching Earthdata cookies...
    2024-09-27 15:34:08,253 - INFO - Credentials added to .netrc file.
    2024-09-27 15:34:08,253 - INFO - Earthdata cookies successfully fetched and saved.

In this example:
- The directory for Earthdata-related data is confirmed to exist.
- Credentials are securely saved in the ``.netrc`` file for later requests.
- A cookie is fetched from the Earthdata server to avoid repeated logins in future sessions.

Security Considerations
-----------------------

- The credentials stored in the ``.netrc`` file are sensitive. Ensure the file is properly protected by restricting file permissions on your system.
- You should never share your ``data_config.yml`` or ``.netrc`` file with anyone, as it contains your NASA Earthdata login details.

Now that your credentials are authenticated, you can begin downloading and processing GEDI data using the **gediDB** package.
