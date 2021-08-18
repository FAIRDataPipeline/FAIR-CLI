import pytest
import os
import pathlib
import yaml
import re
import tempfile

import fair.common as fdp_com
import fair.registry.requests as fdp_req
import fair.staging as fdp_stage


@pytest.fixture
def mock_uuid_return(mocker, no_init_session):
    def run_return(*args, **kwargs):
        return {"uuid": "312312312"}
    with open(fdp_com.staging_cache(no_init_session._session_loc), 'w') as f:
        yaml.dump({"run": {"312312312": False}, "file": {}}, f)
    _stager = fdp_stage.Stager(no_init_session._session_loc)
    mocker.patch.object(fdp_stage, 'Stager', _stager)
    mocker.patch.object(fdp_req, 'get', run_return)
    mocker.patch.object(fdp_com, 'local_fdpconfig', lambda *args: fdp_com.global_fdpconfig())

    return no_init_session

@pytest.mark.session
@pytest.mark.dependency()
def test_initialise(no_init_session):

    no_init_session.initialise()
    assert os.path.exists(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER)
    )


@pytest.mark.session
@pytest.mark.dependency()
def test_run_add(mock_uuid_return):
    """Test staging of a file works.

    Expect that a new entry to be added matching relative file path of
    new file, and its status to be "True"
    """
    _dummy_id = "312312312"
    mock_uuid_return.change_staging_state(_dummy_id)
    mock_uuid_return.close_session()
    assert yaml.safe_load(
        open(fdp_com.staging_cache(mock_uuid_return._session_loc))
    )["run"][_dummy_id]


@pytest.mark.session
def test_remote_list(no_init_session):
    no_init_session.make_starter_config()
    with open(fdp_com.local_fdpconfig(no_init_session._session_loc)) as f:
        _conf = yaml.safe_load(f)
    _res = no_init_session.list_remotes()
    _res = [re.findall(r'\](.+)\[', i)[0] for i in _res]
    assert sorted(['local', 'origin']) == sorted(_res)


@pytest.mark.session
def test_get_status(mock_uuid_return, capfd):
    _tempdir = os.path.join(mock_uuid_return._session_loc, "tempdir")
    os.makedirs(_tempdir, exist_ok=True)
    mock_uuid_return.status()
    out, _ = capfd.readouterr()
    _expect = "Changes not staged for synchronization:\n"
    _expect += '\t(use "fair add <job>..." to stage jobs)\n'
    _expect += '\tJobs:\n'
    _expect += '\t\t312312312\n'
    assert out == _expect
    mock_uuid_return.change_staging_state("312312312", True)
    mock_uuid_return.status()
    out, _ = capfd.readouterr()
    _expect = "Changes to be synchronized:\n"
    _expect += "\tJobs:\n"
    _expect += '\t\t312312312\n'
    assert out == _expect


@pytest.mark.session
@pytest.mark.dependency(depends=["test_initialise"])
def test_make_config(no_init_session):
    _cfg_yaml = os.path.join(no_init_session._session_loc, "config.yaml")
    os.remove(_cfg_yaml)
    no_init_session.make_starter_config()
    assert os.path.exists(_cfg_yaml)
    _config = yaml.safe_load(open(_cfg_yaml))
    _expected_meta_start = [
        "write_data_store",
        "default_input_namespace",
        "default_output_namespace",
        "description",
        "local_data_registry",
        "local_repo",
    ]
    for i in _expected_meta_start:
        assert i in _config["run_metadata"]

