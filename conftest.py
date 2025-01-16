# SPDX-License-Identifier: EUPL-1.2
# Version: 2.0
# Contact: ah2174@cam.ac.uk, felix.dombrowski@uni-potsdam.de, besnard@gfz-potsdam.de
# SPDX-FileCopyrightText: 2024 Simon Besnard
# SPDX-FileCopyrightText: 2024 Felix Dombrowski
# SPDX-FileCopyrightText: 2024 Amelia Holcomb
# SPDX-FileCopyrightText: 2024 Helmholtz Centre Potsdam - GFZ German Research Centre for Geosciences
"""Configuration for pytest."""

import pytest


def pytest_addoption(parser):
    """Add command-line flags for pytest."""
    parser.addoption("--run-flaky", action="store_true", help="Run flaky tests")
    parser.addoption(
        "--run-network-tests",
        action="store_true",
        help="Run tests requiring a network connection",
    )

def pytest_runtest_setup(item):
    """Skip tests based on custom markers and command-line options."""
    if "flaky" in item.keywords and not item.config.getoption("--run-flaky"):
        pytest.skip("Set --run-flaky option to run flaky tests")
    if "network" in item.keywords and not item.config.getoption("--run-network-tests"):
        pytest.skip("Set --run-network-tests to run tests requiring an internet connection")

@pytest.fixture(autouse=True)
def add_standard_imports(doctest_namespace, tmpdir):
    """Provide standard imports and setup for doctests."""
    import numpy as np
    import pandas as pd
    import gedidb as gdb

    # Add commonly used modules to the doctest namespace
    doctest_namespace["np"] = np
    doctest_namespace["pd"] = pd
    doctest_namespace["gdb"] = gdb

    # Seed numpy.random for deterministic examples
    np.random.seed(0)

    # Switch to the temporary directory for file operations
    tmpdir.chdir()

    # Configure dask to suppress deprecation warnings (if installed)
    try:
        import dask
    except ImportError:
        pass
    else:
        dask.config.set({"dataframe.query-planning": True})

