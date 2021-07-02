import tempfile

import pytest
import logging
import os
import yaml
import click
import uuid

import fair.common as fdp_com
import fair.session as fdp_s
import fair.server as fdp_svr
import fair.configuration as fdp_conf
import fair.registry.storage as fdp_store


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
    def _func(msg, **kwargs):
        if msg == "Full Name":
            return "Joe Bloggs"
        elif msg == "Remote API URL":
            return "http://noserver/api/"
        elif msg == "Local API URL":
            return "http://localhost:8000/api/"
        elif msg == "Email":
            return "jbloggs@nowhere"
        elif msg == "ORCID":
            return "None"
        elif msg == "Default input namespace":
            return "SCRC"
        else:
            return kwargs["default"]

    mocker.patch.object(
        click, "prompt", lambda msg, **kwargs: _func(msg, **kwargs)
    )


@pytest.fixture
def no_registry_edits(mocker):
    mocker.patch.object(fdp_store, "store_working_config", lambda *args: "")


@pytest.fixture
def no_init_session(
    global_test,
    repo_root,
    mocker,
    no_prompt,
    no_registry_edits,
):
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
        "user": {
            "email": "jbloggs@nowhere",
            "given_name": "Joe",
            "family_name": "Bloggs",
            "uuid": str(uuid.uuid4()),
        },
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
