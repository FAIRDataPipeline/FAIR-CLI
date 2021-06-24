import pytest
import tempfile


import fair.session as fdp_session

_dummy_conf = {
    "remotes": {
        "remote": "https://data.scrc.uk/api/",
        "local": "https://localhost:8000",
    },
    "user": {"name": "John Smith", "email": "jsmith@no", "ORCID": None},
    "namespaces": {"input": "scrc", "output": "jsmith"},
}


@pytest.fixture()
def set_constant_paths(session_mocker):
    """Fixture to set temporary working directory for tests"""
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch("fair.common.REGISTRY_HOME", _tempdir)
    return _tempdir


@pytest.fixture()
def no_server(session_mocker):
    """Do not try to start a registry server when running test"""
    session_mocker.patch("fair.session.FAIR._launch_server")
    session_mocker.patch("fair.session.FAIR._stop_server")


@pytest.fixture()
def initialise_test_repo(no_server, session_mocker):
    """Initialises a FAIR repository"""
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch("fair.session.FAIR._global_config_query")
    session_mocker.patch("fair.session.FAIR._local_config_query")
    session_mocker.patch("fair.common.find_fair_root", lambda *x: _tempdir)

    with fdp_session.FAIR() as fair_session:
        fair_session._global_config = _dummy_conf
        fair_session._local_config = _dummy_conf
        fair_session.initialise(_tempdir)
    return _tempdir
