import pytest
import os
import requests_mock
import glob

import fair.server as fdp_serv
import fair.exceptions as fdp_exc
import fair.common as fdp_com

LOCALHOST = "http://localhost:8000/api/"


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_server_not_running(no_registry_autoinstall, file_always_exists, **kwargs):
    kwargs['rmock'].get(LOCALHOST, status_code=400)
    assert not fdp_serv.check_server_running(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_start_server_success(mocker, file_always_exists, subprocess_do_nothing, no_registry_autoinstall, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get(LOCALHOST, status_code=200)
    fdp_serv.launch_server(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_start_server_fail(no_registry_autoinstall, file_always_exists, subprocess_do_nothing, mocker, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get("http://badhost", status_code=400)
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.launch_server("http://badhost")


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_stop_server_success(no_registry_autoinstall, file_always_exists, mocker, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get(LOCALHOST, status_code=400)
    for run in glob.glob(os.path.join(fdp_com.session_cache_dir(), '*.run')):
        os.remove(run)
    fdp_serv.stop_server(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_stop_server_fail(no_registry_autoinstall, mocker, file_always_exists, subprocess_do_nothing, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get("http://badhost")
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.stop_server("http://badhost")
