import pytest
import os
import subprocess
import requests_mock

import fair.server as fdp_serv
import fair.exceptions as fdp_exc

LOCALHOST = "http://localhost:8000/api/"


@pytest.fixture
def subprocess_do_nothing(mocker):
    class _stdout:
        def __init__(self):
            pass

    class dummy_popen:
        def __init__(self, *args, **kwargs):
            self.stdout = _stdout()
        def wait(self):
            pass

    mocker.patch.object(subprocess, 'Popen', dummy_popen)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_server_not_running(**kwargs):
    kwargs['rmock'].get(LOCALHOST, status_code=400)
    assert not fdp_serv.check_server_running(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_start_server_success(mocker, subprocess_do_nothing, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get(LOCALHOST, status_code=200)
    fdp_serv.launch_server(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_start_server_fail(mocker, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get("http://badhost", status_code=400)
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.launch_server("http://badhost", True)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_stop_server_success(mocker, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get(LOCALHOST, status_code=400)
    fdp_serv.stop_server(LOCALHOST)


@pytest.mark.server
@requests_mock.Mocker(kw='rmock')
def test_stop_server_fail(mocker, **kwargs):
    mocker.patch.object(os.path, 'exists', lambda x : True)
    kwargs['rmock'].get("http://badhost")
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.stop_server("http://badhost")
