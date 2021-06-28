import pytest
import logging
import os
import pathlib
import yaml

import fair.session as fdp_s
import fair.configuration as fdp_conf
import fair.server as fdp_svr
import fair.common as fdp_com


@pytest.mark.session
@pytest.fixture(scope="module")
def no_init_session(global_test, repo_root, module_mocker):
    """Creates a session without any calls to setup

    This requires mocking a few features of the FAIR class:

    - Setting the __init__ method to be "return None"
    - Make local config setup function return a premade dictionary
    - Point the global folder to '/tmp' which always exists
    - Set the generated user config to be also within this temp folder
    """
    _glob_conf = {
        "namespaces": {"input": "SCRC", "output": "test"},
        "remotes": {
            "local": "http://localhost:8000/api/",
            "origin": "http://noserver/api",
        },
        "user": {"email": "jbloggs@nowhere", "name": "Joe Bloggs"},
    }
    with open(fdp_com.global_fdpconfig(), "w") as f:
        yaml.dump(_glob_conf, f)
    _loc_conf = _glob_conf
    del _loc_conf["user"]
    _loc_conf["description"] = "Test"
    module_mocker.patch.object(fdp_s.FAIR, "__init__", lambda *args: None)
    module_mocker.patch.object(
        fdp_conf, "local_config_query", lambda *args: _loc_conf
    )
    _fdp_session = fdp_s.FAIR(repo_root)
    _fdp_session._session_config = os.path.join(repo_root, "config.yaml")
    _fdp_session._session_loc = repo_root
    _fdp_session._global_config = _glob_conf
    _fdp_session._stage_status = {}
    _fdp_session._logger = logging.getLogger("FAIR-CLI.TestFAIR")
    _fdp_session._logger.setLevel(logging.DEBUG)
    _fdp_session._session_id = None
    _fdp_session._run_mode = fdp_svr.SwitchMode.NO_SERVER
    yield _fdp_session
    _fdp_session.close_session()


@pytest.mark.session
@pytest.mark.dependency()
def test_initialise(no_init_session):
    no_init_session.initialise()
    assert os.path.exists(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER)
    )


@pytest.mark.session
@pytest.mark.dependency()
def test_file_add(no_init_session):
    """Test staging of a file works.

    Expect that a new entry to be added matching relative file path of
    new file, and its status to be "True"
    """
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    pathlib.Path(_staged_file).touch()
    no_init_session.change_staging_state(_staged_file)
    no_init_session.close_session()
    assert yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )["../temp"]


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_add"])
def test_file_reset(no_init_session):
    """Test staging of a file works.

    Expect that a new entry to be added matching relative file path of
    new file, and its status to be "True"
    """
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.change_staging_state(_staged_file, False)
    no_init_session.close_session()
    assert not yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )["../temp"]
    no_init_session.change_staging_state(_staged_file, True)


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_reset"])
def test_file_remove_soft(no_init_session):
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.remove_file(_staged_file, cached=True)
    no_init_session.close_session()
    assert os.path.exists(_staged_file)
    assert "../temp" not in yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_remove_soft"])
def test_file_remove(no_init_session):
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.change_staging_state(_staged_file)
    no_init_session.remove_file(_staged_file, cached=False)
    no_init_session.close_session()
    assert not os.path.exists(_staged_file)
    assert "../temp" not in yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )


@pytest.mark.session
def test_get_status(no_init_session, capfd):
    _tempdir = os.path.join(no_init_session._session_loc, "tempdir")
    os.makedirs(_tempdir)
    _staged_files = [os.path.join(_tempdir, f"temp_{i}") for i in range(5)]
    no_init_session.status()
    out, _ = capfd.readouterr()
    assert out == "Nothing marked for tracking.\n"
    for _staged in _staged_files:
        pathlib.Path(_staged).touch()
        no_init_session.change_staging_state(_staged)
    no_init_session.status()
    out, _ = capfd.readouterr()
    _expect = "Changes to be synchronized:\n\t\t"
    _expect += (
        "\n\t\t".join(
            f"../tempdir/{os.path.basename(file)}" for file in _staged_files
        )
        + "\n"
    )
    assert out == _expect
    for _staged in _staged_files:
        pathlib.Path(_staged).touch()
        no_init_session.change_staging_state(_staged, False)
    no_init_session.status()
    out, _ = capfd.readouterr()
    _expect = "Files not staged for synchronization:\n\t"
    _expect += '(use "fair add <file>..." to stage files)\n\t\t'
    _expect += (
        "\n\t\t".join(
            f"../tempdir/{os.path.basename(file)}" for file in _staged_files
        )
        + "\n"
    )
    assert out == _expect


@pytest.mark.session
@pytest.mark.dependency(depends=["test_initialise"])
def test_make_config(no_init_session):
    _cfg_yaml = os.path.join(no_init_session._session_loc, "config.yaml")
    os.remove(_cfg_yaml)
    no_init_session.make_starter_config()
    assert os.path.exists(_cfg_yaml)
    _config = yaml.safe_load(open(_cfg_yaml))
    assert _config["fail_on_hash_mismatch"]
    _expected_meta_start = [
        "data_store",
        "default_input_namespace",
        "default_output_namespace",
        "description",
        "local_data_registry",
        "local_repo",
    ]
    assert all(i in _config["run_metadata"] for i in _expected_meta_start)
