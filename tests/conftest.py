import tempfile

import pytest
import os

import fair.common as fdp_com


@pytest.fixture(scope="session")
def global_test(session_mocker):
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch.object(fdp_com, "global_config_dir", lambda: _tempdir)
    session_mocker.patch.object(
        fdp_com,
        "global_fdpconfig",
        lambda: os.path.join(_tempdir, "cli-config.yaml"),
    )


@pytest.fixture(scope="session")
def repo_root(session_mocker):
    _tempdir = tempfile.mkdtemp()
    session_mocker.patch("fair.common.find_fair_root", lambda *args: _tempdir)
    return _tempdir
