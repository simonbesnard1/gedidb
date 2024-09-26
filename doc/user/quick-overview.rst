##############
Quick overview
##############

Here are some quick examples of what you can do with :py:class:`xarray.DataArray`
objects. Everything is explained in much more detail in the rest of the
documentation.

To begin, import numpy, pandas and xarray using their customary abbreviations:

.. ipython:: python

    import numpy as np
    import pandas as pd
    import xarray as xr

Create a DataArray
----------

You can make a DataArray from scratch by supplying data in the form of a numpy
array or list, with optional *dimensions* and *coordinates*:

.. ipython:: python

    data = xr.DataArray(np.random.randn(2, 3), dims=("x", "y"), coords={"x": [10, 20]})
    data

In this case, we have generated a 2D array, assigned the names *x* and *y* to the two dimensions respectively and associated two *coordinate labels* '10' and '20' with the two locations along the x dimension. If you supply a pandas :py:class:`~pandas.Series` or :py:class:`~pandas.DataFrame`, metadata is copied directly:

.. ipython:: python

    xr.DataArray(pd.Series(range(3), index=list("abc"), name="foo"))

Here are the key properties for a ``DataArray``:

.. ipython:: python

    # like in pandas, values is a numpy array that you can modify in-place
    data.values
    data.dims
    data.coords
    # you can use this dictionary to store arbitrary metadata
    data.attrs


Indexing
--------

Xarray supports four kinds of indexing. Since we have assigned coordinate labels to the x dimension we can use label-based indexing along that dimension just like pandas. The four examples below all yield the same result (the value at `x=10`) but at varying levels of convenience and intuitiveness.

.. ipython:: python

    # positional and by integer label, like numpy
    data[0, :]

    # loc or "location": positional and coordinate label, like pandas
    data.loc[10]

    # isel or "integer select":  by dimension name and integer label
    data.isel(x=0)

    # sel or "select": by dimension name and coordinate label
    data.sel(x=10)


Unlike positional indexing, label-based indexing frees us from having to know how our array is organized. All we need to know are the dimension name and the label we wish to index i.e. ``data.sel(x=10)`` works regardless of whether ``x`` is the first or second dimension of the array and regardless of whether ``10`` is the first or second element of ``x``. We have already told xarray that x is the first dimension when we created ``data``: xarray keeps track of this so we don't have to. For more, see :ref:`indexing`.
