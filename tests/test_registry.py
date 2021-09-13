import pytest
import requests
import pathlib
import yaml
import tempfile
import hashlib
import os

import fair.registry.storage as fdp_store
import fair.configuration as fdp_conf

LOCALHOST = "http://localhost:8000/api/"

@pytest.mark.registry
def test_add_author(mocker):
    mocker.patch.object(fdp_conf, "get_current_user_name", lambda *args, **kwargs : ('Joe', 'Bloggs'))
    mocker.patch.object(fdp_conf, "get_current_user_orcid", lambda *args, **kwargs: "0000000000")
    fdp_store.store_user('', LOCALHOST)
    assert requests.get(LOCALHOST+'author', params={'name': 'Joe Bloggs'}).json()['results']


@pytest.mark.registry
def test_create_file_type():
    fdp_store.create_file_type(LOCALHOST, extension="tst")
    assert requests.get(LOCALHOST+'file_type', params={'name': 'test file', 'extension': 'tst'}).json()['results']


@pytest.mark.registry
def test_store_wkg_config(mocker):
    mocker.patch.object(fdp_conf, "get_current_user_name", lambda *args, **kwargs : ('Joe', 'Bloggs'))
    mocker.patch.object(fdp_conf, "get_current_user_orcid", lambda *args, **kwargs: "0000000000")
    _wkg_config = {
        'run_metadata': {
            'write_data_store': '/fake/path/address'
        }
    }
    _file, _name = tempfile.mkstemp(suffix='.yaml')
    pathlib.Path(_file).touch()
    mocker.patch.object(yaml, 'safe_load', lambda *args: _wkg_config)
    fdp_store.store_working_config('', LOCALHOST, _name)
    assert requests.get(LOCALHOST+'storage_root', params={'root': f"file:///fake/path/address/"})
    _hash = hashlib.sha1(os.fdopen(_file).read().encode("utf-8")).hexdigest()
    assert requests.get(LOCALHOST+'storage_location', params={'hash': _hash}).json()['results']
