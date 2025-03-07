
.. DO NOT EDIT.
.. THIS FILE WAS AUTOMATICALLY GENERATED BY SPHINX-GALLERY.
.. TO MAKE CHANGES, EDIT THE SOURCE PYTHON FILE:
.. "auto_examples/authentification.py"
.. LINE NUMBERS ARE GIVEN BELOW.

.. only:: html

    .. note::
        :class: sphx-glr-download-link-note

        :ref:`Go to the end <sphx_glr_download_auto_examples_authentification.py>`
        to download the full example code.

.. rst-class:: sphx-glr-example-title

.. _sphx_glr_auto_examples_authentification.py:


Authenticating with NASA EarthData API
======================================

This example demonstrates how to authenticate with the NASA EarthData API using the `EarthDataAuthenticator` class.
The authentication process involves managing `.netrc` and cookie files to ensure seamless automated login.

Before running this example:

1. Ensure you have a valid EarthData account. You can create one at https://urs.earthdata.nasa.gov.
2. Install `wget`, which is used to fetch cookies. You can install it via your system's package manager (e.g., `apt-get install wget` on Debian/Ubuntu).

.. GENERATED FROM PYTHON SOURCE LINES 13-36

.. code-block:: Python


    from pathlib import Path

    from gedidb.downloader.authentication import EarthDataAuthenticator

    # Specify the directory where `.netrc` and cookies will be stored
    earth_data_dir = Path.home()

    # Initialize the authenticator
    authenticator = EarthDataAuthenticator(earth_data_dir=earth_data_dir, strict=False)

    # Authenticate and ensure `.netrc` and cookies are valid
    authenticator.authenticate()

    # Expected Output:
    # ----------------
    # INFO:__main__:EarthData authentication setup incomplete; starting setup.
    # Please enter your Earthdata Login username: <your_username>
    # Please enter your Earthdata Login password:
    # INFO:__main__:Credentials added to .netrc file.
    # INFO:__main__:Attempting to fetch Earthdata cookies and save to /home/username/.earthdata_auth/.cookies
    # INFO:__main__:Earthdata cookies successfully fetched and saved to /home/username/.earthdata_auth/.cookies.
    # Authentication complete.


.. _sphx_glr_download_auto_examples_authentification.py:

.. only:: html

  .. container:: sphx-glr-footer sphx-glr-footer-example

    .. container:: sphx-glr-download sphx-glr-download-jupyter

      :download:`Download Jupyter notebook: authentification.ipynb <authentification.ipynb>`

    .. container:: sphx-glr-download sphx-glr-download-python

      :download:`Download Python source code: authentification.py <authentification.py>`

    .. container:: sphx-glr-download sphx-glr-download-zip

      :download:`Download zipped: authentification.zip <authentification.zip>`


.. only:: html

 .. rst-class:: sphx-glr-signature

    `Gallery generated by Sphinx-Gallery <https://sphinx-gallery.github.io>`_
