.. _fundamentals-authenticate:

#####################
NASA authentication
#####################

To interact with NASA's Common Metadata Repository (CMR) and download the required `.h5` files, gediDB requires NASA Earthdata credentials for authentication. This guide will help you set up and securely store your credentials to enable seamless access.

Creating CMR login credentials
------------------------------

To access GEDI data hosted on NASA’s servers, you need an **Earthdata** account. If you don't have one, create an account at the following link:

`Create an Earthdata Account <https://urs.earthdata.nasa.gov/>`_

Storing credentials for authentication
--------------------------------------

To avoid re-entering your credentials each time, gediDB includes a function that securely saves your login information in a `.netrc` file, enabling automatic authentication for future requests to NASA’s servers.

Use the following code snippet to authenticate and store your credentials in the `.netrc` file:

.. code-block:: python

    import gedidb as gdb

    authentificator = gdb.EarthDataAuthenticator()
    authentificator.authenticate()
    
Explanation:
- :py:class:`gedidb.EarthDataAuthenticator` asks for your credentials in the prompt and verifies them.
- Once authenticated, the credentials are saved in the `.netrc` file for future use, avoiding repeated login prompts.

Successful authentication confirmation
--------------------------------------

Upon successful authentication, gediDB logs messages to confirm that your credentials were correctly stored and verified. Here’s an example of the log output:

.. code-block:: none

    2024-09-27 15:34:08,253 - INFO - Directory ensured: path/to/directory
    2024-09-27 15:34:08,253 - INFO - No authentication cookies found, fetching Earthdata cookies...
    2024-09-27 15:34:08,253 - INFO - Credentials added to .netrc file.
    2024-09-27 15:34:08,253 - INFO - Earthdata cookies successfully fetched and saved.

This output confirms:
- The directory for Earthdata data exists.
- Credentials are securely saved in the `.netrc` file.
- Authentication cookies are fetched from Earthdata servers, enabling easier login in subsequent sessions.

Security considerations
-----------------------

- **Protecting `.netrc` file permissions**: The `.netrc` file contains sensitive login information. Limit access by setting appropriate file permissions (e.g., `chmod 600 .netrc` on Unix systems).
- **Avoid sharing sensitive files**: Do not share your `.netrc` file, as they contain your Earthdata credentials.
  
With authentication configured, you are now ready to download and process GEDI data using gediDB.
