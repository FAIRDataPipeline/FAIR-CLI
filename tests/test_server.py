import pytest
import os
import subprocess
import requests_mock

import fair.server as fdp_serv
import fair.exceptions as fdp_exc

LOCALHOST = "http://localhost:8000/api/"

SERVER_RUN = False

_mock_json_response = {
    'users': 'http://localhost:8000/api/users/',
    'groups': 'http://localhost:8000/api/groups/',
    'file_type': 'http://localhost:8000/api/file_type/',
    'issue': 'http://localhost:8000/api/issue/',
    'author': 'http://localhost:8000/api/author/',
    'object': 'http://localhost:8000/api/object/',
    'user_author': 'http://localhost:8000/api/user_author/',
    'object_component': 'http://localhost:8000/api/object_component/',
    'code_run': 'http://localhost:8000/api/code_run/',
    'storage_root': 'http://localhost:8000/api/storage_root/',
    'storage_location': 'http://localhost:8000/api/storage_location/',
    'namespace': 'http://localhost:8000/api/namespace/',
    'data_product': 'http://localhost:8000/api/data_product/',
    'external_object': 'http://localhost:8000/api/external_object/',
    'quality_controlled': 'http://localhost:8000/api/quality_controlled/',
    'keyword': 'http://localhost:8000/api/keyword/',
    'licence': 'http://localhost:8000/api/licence/',
    'code_repo_release': 'http://localhost:8000/api/code_repo_release/',
    'key_value': 'http://localhost:8000/api/key_value/'
}


@pytest.fixture
def subprocess_do_nothing(mocker):
    class _stdout:
        def __init__(self):
            pass
        def read(self, int):
            return b'blah blah blah'
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
