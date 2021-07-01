import tempfile

import pytest
import logging
import os
import yaml
import click

import fair.common as fdp_com
import fair.session as fdp_s
import fair.server as fdp_svr
import fair.configuration as fdp_conf
import fair.registry as fdp_reg


@pytest.fixture(scope="module")
def global_test(module_mocker):
    _tempdir = tempfile.mkdtemp()
    module_mocker.patch.object(fdp_com, "USER_FAIR_DIR", _tempdir)

    module_mocker.patch.object(
        fdp_com,
        "global_fdpconfig",
        lambda: os.path.join(_tempdir, "cli-config.yaml"),
    )

    return _tempdir


@pytest.fixture(scope="module")
def repo_root(module_mocker):
    _tempdir = tempfile.mkdtemp()
    module_mocker.patch("fair.common.find_fair_root", lambda *args: _tempdir)
    return _tempdir


@pytest.fixture
def no_prompt(mocker):
    def _func(*args, **kwargs):
        if "default" in kwargs:
            return kwargs["default"]
        else:
            return "TEST"

    mocker.patch.object(
        click, "prompt", lambda *args, **kwargs: _func(*args, **kwargs)
    )


@pytest.fixture
def no_registry_edits(mocker):
    class DummyReq:
        def __init__(self, *args, **kwargs):
            pass

        def get_write_storage(self):
            pass

    mocker.patch.object(fdp_reg, "Requester", DummyReq)


@pytest.mark.session
@pytest.fixture
def no_init_session(global_test, repo_root, mocker, no_registry_edits):
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
    _loc_conf["description"] = "Test"
    mocker.patch.object(fdp_s.FAIR, "__init__", lambda *args: None)
    mocker.patch.object(
        fdp_conf, "local_config_query", lambda *args: _loc_conf
    )
    _fdp_session = fdp_s.FAIR(repo_root)
    _fdp_session._session_config = os.path.join(repo_root, "config.yaml")
    _fdp_session._session_loc = repo_root
    _fdp_session._global_config = _glob_conf
    _fdp_session._local_config = _loc_conf
    _fdp_session._stage_status = {}
    _fdp_session._logger = logging.getLogger("FAIR-CLI.TestFAIR")
    _fdp_session._logger.setLevel(logging.DEBUG)
    _fdp_session._session_id = None
    _fdp_session._run_mode = fdp_svr.SwitchMode.NO_SERVER
    yield _fdp_session
    _fdp_session.close_session()
