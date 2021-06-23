import pytest
import tempfile


import fair.session as fdp_session


@pytest.fixture(scope="session")
def set_constant_paths(session_mocker):
    """Fixture to set temporary working directory for tests"""
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch("fair.common.REGISTRY_HOME", _tempdir)


@pytest.fixture(scope="session")
def no_server(session_mocker):
    session_mocker.patch("fair.session.FAIR._launch_server")
    session_mocker.patch("fair.session.FAIR._stop_server")


@pytest.fixture(scope="session")
def initialise_test_repo(no_server, session_mocker):
    """Initialises a FAIR repository"""
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch("fair.session.FAIR._global_config_query")
    session_mocker.patch("fair.session.FAIR._local_config_query")
    with fdp_session.FAIR() as fair_session:
        fair_session.initialise(_tempdir)
    return _tempdir
