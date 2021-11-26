import logging
import os
import signal
import tempfile

import pytest
import pytest_fixture_config
import pytest_mock
import pytest_virtualenv
import yaml

import fair.common as fdp_com
import fair.testing as fdp_test

from . import registry_install as test_reg

TEST_JOB_FILE_TIMESTAMP = "2021-10-11_10_0_0_100000"


logging.getLogger("FAIRDataPipeline").setLevel(logging.DEBUG)


@pytest.fixture(scope="session")
@pytest_fixture_config.yield_requires_config(
    pytest_virtualenv.FixtureConfig(
        virtualenv_executable="venv",
    ),
    ["virtualenv_executable"],
)
def session_virtualenv():
    """Function-scoped virtualenv in a temporary workspace.

    Methods
    -------
    run()                : run a command using this virtualenv's shell environment
    run_with_coverage()  : run a command in this virtualenv, collecting coverage
    install_package()    : install a package in this virtualenv
    installed_packages() : return a dict of installed packages

    Attributes
    ----------
    virtualenv (`path.path`)    : Path to this virtualenv's base directory
    python (`path.path`)        : Path to this virtualenv's Python executable
    easy_install (`path.path`)  : Path to this virtualenv's easy_install executable
    .. also inherits all attributes from the `workspace` fixture
    """
    venv = pytest_virtualenv.VirtualEnv()
    yield venv
    venv.teardown()


@pytest.fixture
def local_config(mocker: pytest_mock.MockerFixture):
    with tempfile.TemporaryDirectory() as tempg:
        os.makedirs(os.path.join(tempg, fdp_com.FAIR_FOLDER, "registry"))
        os.makedirs(os.path.join(tempg, fdp_com.FAIR_FOLDER, "sessions"))
        _gconfig_path = os.path.join(
            tempg, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
        )
        _cfgg = fdp_test.create_configurations(tempg, None, tempg, True)
        yaml.dump(_cfgg, open(_gconfig_path, "w"))
        mocker.patch(
            "fair.common.global_config_dir", lambda: os.path.dirname(_gconfig_path)
        )
        mocker.patch("fair.common.global_fdpconfig", lambda: _gconfig_path)

        with open(fdp_com.registry_session_port_file(), "w") as pf:
            pf.write("8001")

        with tempfile.TemporaryDirectory() as templ:
            os.makedirs(os.path.join(templ, fdp_com.FAIR_FOLDER))
            _lconfig_path = os.path.join(
                templ, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
            )
            _cfgl = fdp_test.create_configurations(templ, None, templ, True)
            yaml.dump(_cfgl, open(_lconfig_path, "w"))
            with open(os.path.join(templ, fdp_com.USER_CONFIG_FILE), "w") as conf:
                yaml.dump({"run_metadata": {}}, conf)
            mocker.patch("fair.common.find_fair_root", lambda *args: templ)
            yield (tempg, templ)


@pytest.fixture
def job_directory(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set default to point to temporary
        mocker.patch("fair.common.default_jobs_dir", lambda *args: tempd)

        # Create a mock job directory
        os.makedirs(os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP))
        yield os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP)


@pytest.fixture
def job_log(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set the log directory
        mocker.patch("fair.history.history_directory", lambda *args: tempd)

        # Create mock job log
        with open(
            os.path.join(tempd, f"job_{TEST_JOB_FILE_TIMESTAMP}.log"), "w"
        ) as out_f:
            out_f.write(
                """--------------------------------
 Commenced = Fri Oct 08 14:45:43 2021
 Author    = Interface Test <test@noreply>
 Command   = fair pull
--------------------------------
------- time taken 0:00:00.791088 -------"""
            )

        yield tempd


class TestRegistry:
    def __init__(self, install_loc: str, venv_dir: str):
        self._install = install_loc
        self._venv = os.path.join(venv_dir, ".env")
        self._process = None
        if not os.path.exists(os.path.join(install_loc, "manage.py")):
            test_reg.install_registry(
                install_dir=install_loc, silent=True, venv_dir=self._venv
            )

    def rebuild(self):
        test_reg.rebuild_local(os.path.join(self._venv, "bin", "python"), self._install)

    def __enter__(self):
        try:
            self._process = test_reg.launch(
                self._install, silent=True, venv_dir=self._venv
            )
            self._token = open(os.path.join(self._install, "token")).read().strip()
        except KeyboardInterrupt as e:
            os.kill(self._process.pid, signal.SIGTERM)
            raise e

    def __exit__(self, type, value, tb):
        os.kill(self._process.pid, signal.SIGTERM)
        self._process = None


@pytest.fixture(scope="session")
def local_registry(session_virtualenv: pytest_virtualenv.VirtualEnv):
    with tempfile.TemporaryDirectory() as tempd:
        session_virtualenv.env = test_reg.django_environ(session_virtualenv.env)
        yield TestRegistry(tempd, session_virtualenv.workspace)
