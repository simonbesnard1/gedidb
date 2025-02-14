
.. DO NOT EDIT.
.. THIS FILE WAS AUTOMATICALLY GENERATED BY SPHINX-GALLERY.
.. TO MAKE CHANGES, EDIT THE SOURCE PYTHON FILE:
.. "auto_examples/get_variable_list.py"
.. LINE NUMBERS ARE GIVEN BELOW.

.. only:: html

    .. note::
        :class: sphx-glr-download-link-note

        :ref:`Go to the end <sphx_glr_download_auto_examples_get_variable_list.py>`
        to download the full example code.

.. rst-class:: sphx-glr-example-title

.. _sphx_glr_auto_examples_get_variable_list.py:


Retrieve List of Variables in TileDB Array
==========================================

This example demonstrates how to retrieve the list of variables stored in a TileDB array
using the `GEDIProvider` class from the `gedidb` package. The output is a `pandas.DataFrame`
containing variable names, descriptions, and metadata.

Prerequisites:
--------------
1. GEDI data must be processed and stored in TileDB arrays.
2. Configure the TileDB storage backend (local or S3).

Steps:
------
1. Configure the TileDB backend (local or S3).
2. Initialize the `GEDIProvider`.
3. Retrieve and display the list of available variables.

.. GENERATED FROM PYTHON SOURCE LINES 20-40

.. code-block:: Python


    import gedidb as gdb

    # Step 1: Configure the TileDB storage backend
    storage_type = "local"  # Options: "local" or "s3"
    local_path = "/path/to/processed/gedi/data"  # Update with your local TileDB path
    s3_bucket = None  # Set the S3 bucket name if using S3 storage

    # Step 2: Initialize the GEDIProvider
    provider = gdb.GEDIProvider(
        storage_type=storage_type,
        local_path=local_path,
        s3_bucket=s3_bucket,
    )

    # Step 3: Retrieve the list of available variables in the TileDB array
    variables_df = provider.get_available_variables()

    # Display the resulting DataFrame
    print(variables_df)


.. _sphx_glr_download_auto_examples_get_variable_list.py:

.. only:: html

  .. container:: sphx-glr-footer sphx-glr-footer-example

    .. container:: sphx-glr-download sphx-glr-download-jupyter

      :download:`Download Jupyter notebook: get_variable_list.ipynb <get_variable_list.ipynb>`

    .. container:: sphx-glr-download sphx-glr-download-python

      :download:`Download Python source code: get_variable_list.py <get_variable_list.py>`

    .. container:: sphx-glr-download sphx-glr-download-zip

      :download:`Download zipped: get_variable_list.zip <get_variable_list.zip>`


.. only:: html

 .. rst-class:: sphx-glr-signature

    `Gallery generated by Sphinx-Gallery <https://sphinx-gallery.github.io>`_
