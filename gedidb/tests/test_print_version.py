from unittest import mock
from io import StringIO
import sys

from gedidb.utils.print_versions import get_sys_info, netcdf_and_hdf5_versions, show_versions


def test_get_sys_info():
    sys_info = get_sys_info()
    assert isinstance(sys_info, list)
    assert any(k == "python" for k, _ in sys_info)
    assert any(k == "OS" for k, _ in sys_info)


def test_netcdf_and_hdf5_versions_with_netcdf4():
    with mock.patch.dict('sys.modules', {
        'netCDF4': mock.Mock(__hdf5libversion__='1.10.6', __netcdf4libversion__='4.7.4')
    }):
        versions = netcdf_and_hdf5_versions()
        assert versions[0][1] == '1.10.6'
        assert versions[1][1] == '4.7.4'


def test_netcdf_and_hdf5_versions_with_h5py():
    with mock.patch.dict('sys.modules', {
        'netCDF4': None,
        'h5py': mock.Mock(version=mock.Mock(hdf5_version='1.12.0'))
    }):
        versions = netcdf_and_hdf5_versions()
        assert versions[0][1] == '1.12.0'
        assert versions[1][1] is None


def test_show_versions():
    output = StringIO()
    show_versions(file=output)
    content = output.getvalue()
    assert "INSTALLED VERSIONS" in content
    assert "python:" in content
    assert "OS:" in content


def test_show_versions_with_missing_modules():
    modules_to_remove = ["gedidb", "pandas"]

    # Temporarily remove modules from sys.modules
    original_modules = {mod: sys.modules.pop(mod, None) for mod in modules_to_remove}

    try:
        with mock.patch('importlib.import_module', side_effect=ImportError):
            output = StringIO()
            show_versions(file=output)
            content = output.getvalue()
            # Check for missing module representation
            assert "gedidb: None" in content or "pandas: None" in content
    finally:
        # Restore original modules
        sys.modules.update(original_modules)


def test_show_versions_with_module_errors():
    # Simulate an error when trying to get the version of pandas
    def mock_version_retrieval(_):
        raise Exception("Version error")

    modules_to_remove = ["pandas"]

    # Temporarily remove pandas to force importlib to trigger
    original_modules = {mod: sys.modules.pop(mod, None) for mod in modules_to_remove}

    mock_module = mock.Mock()

    try:
        with mock.patch('importlib.import_module', return_value=mock_module):
            with mock.patch('gedidb.utils.print_versions.show_versions.__globals__["deps"]', [
                ("pandas", mock_version_retrieval)
            ]):
                output = StringIO()
                show_versions(file=output)
                content = output.getvalue()
                # Expecting the fallback behavior when version retrieval fails
                assert "pandas: installed" in content
    finally:
        # Restore original modules
        sys.modules.update(original_modules)
