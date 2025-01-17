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

    # Example Output:
    2025-01-17 15:56:48,114 - INFO - No authentication files found; starting Earthdata authentication.
    2025-01-17 15:56:48,115 - INFO - Prompting user to create credentials.
    Please enter your Earthdata Login username: 
    Earthdata Login password: 

Explanation:

- :py:class:`gedidb.EarthDataAuthenticator` asks for your credentials in the prompt and verifies them.
- Once authenticated, the credentials are saved in the `.netrc` file for future use, avoiding repeated login prompts.
- By default credentials files are stores in your home directory.
- Authentication cookies are fetched and stored in a .cookies file for efficient reuse.


Credential validation process
-----------------------------

As part of the authentication workflow:

- gediDB checks for the existence of a .netrc file in the specified or default directory (typically the user’s home directory).
- If the file is missing, the user is prompted to input their credentials, which are then securely stored in the .netrc file.
- Authentication cookies are retrieved from NASA servers and stored in the .cookies file, reducing the need for repeated credential validation during subsequent requests.


Successful authentication confirmation
--------------------------------------

Upon successful authentication, gediDB logs messages to confirm that your credentials were correctly stored and verified. Here’s an example of the log output:

.. code-block:: none

    2025-01-17 15:57:53,340 - INFO - Credentials added to .netrc file.
    2025-01-17 15:57:53,341 - INFO - Attempting to fetch Earthdata cookies and save to /home/$user/.cookies
    2025-01-17 15:57:54,364 - INFO - Earthdata cookies successfully fetched and saved to /home/$user//.cookies.

This output confirms:

- The directory for Earthdata data exists.
- Credentials are securely saved in the `.netrc` file.
- Authentication cookies are fetched from Earthdata servers, enabling easier login in subsequent sessions.

Security considerations
-----------------------

- **Protecting `.netrc` file permissions**: The `.netrc` file contains sensitive login information. Limit access by setting appropriate file permissions (e.g., `chmod 600 .netrc` on Unix systems).
- **Avoid sharing sensitive files**: Do not share your `.netrc` file, as they contain your Earthdata credentials.

Testing authentication
----------------------

To verify that your credentials are correct and valid:

- Run the authentication process using :py:class:`gedidb.EarthDataAuthenticator`.
- Check the log messages for successful credential verification and cookie fetching.

Troubleshooting tips:
---------------------

- Ensure you are using the correct username and password for your Earthdata account.
- If authentication fails, check the permissions and format of your `.netrc` file.
- Contact NASA Earthdata support if issues persist: `Earthdata Support <https://www.earthdata.nasa.gov/contact>`_
  
With authentication configured, you are now ready to download and process GEDI data using gediDB.
