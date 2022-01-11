import logging
import os
import signal
import tempfile
import git
import typing

import pytest
import pytest_fixture_config
import pytest_mock
import pytest_virtualenv
import yaml
import time

import fair.common as fdp_com
import fair.testing as fdp_test
import fair.registry.server as fdp_serv

from . import registry_install as test_reg

TEST_JOB_FILE_TIMESTAMP = "2021-10-11_10_0_0_100000"
PYTHON_API_GIT = "https://github.com/FAIRDataPipeline/pyDataPipeline.git"

TEST_OUT_DIR = os.path.join(os.getcwd(), "test_outputs")
os.makedirs(TEST_OUT_DIR, exist_ok=True)


logging.getLogger("FAIRDataPipeline").setLevel(logging.DEBUG)

def get_example_entries(registry_dir: str):
    """
    With the registry examples regularly changing this function parses the 
    relevant file in the reg repository to obtain all example object metadata
    """
    SEARCH_STR = "StorageLocation.objects.get_or_create"
    _example_file = os.path.join(
        registry_dir,
        "data_management",
        "management",
        "commands",
        "_example_data.py"
    )

    _objects: typing.List[typing.Tuple[str, str, str]] = []

    with open(_example_file) as in_f:
        _lines = in_f.readlines()
        for i, line in enumerate(_lines):
            if SEARCH_STR in line:
                _path_line_offset = 0
                while "path" not in _lines[i+_path_line_offset]:
                    _path_line_offset += 1
                _candidate = _lines[i+_path_line_offset]
                _candidate = _candidate.replace('"', "")
                _candidate = _candidate.replace("path=", "")
                _metadata, _file = _candidate.rsplit("/", 1)
                _metadata = _metadata.replace("path=", "")
                _version = ".".join(_file.split(".")[:3])
                _objects.append((*_metadata.split("/", 1), _version))
    
    return _objects


@pytest.fixture()
def pyDataPipeline():
    with tempfile.TemporaryDirectory() as temp_d:
        _repo_path = os.path.join(temp_d, 'repo')
        _repo = git.Repo.clone_from(PYTHON_API_GIT, _repo_path)
        _repo.git.checkout("dev")
        yield _repo_path
        

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
        _cfgg = fdp_test.create_configurations(tempg, None, None, tempg, True)
        yaml.dump(_cfgg, open(_gconfig_path, "w"))
        mocker.patch(
            "fair.common.global_config_dir",
            lambda: os.path.dirname(_gconfig_path),
        )
        mocker.patch("fair.common.global_fdpconfig", lambda: _gconfig_path)

        with open(fdp_com.registry_session_port_file(), "w") as pf:
            pf.write("8001")

        with tempfile.TemporaryDirectory() as templ:
            os.makedirs(os.path.join(templ, fdp_com.FAIR_FOLDER))
            _lconfig_path = os.path.join(
                templ, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
            )
            _cfgl = fdp_test.create_configurations(templ, None, None, templ, True)
            yaml.dump(_cfgl, open(_lconfig_path, "w"))
            with open(
                os.path.join(templ, fdp_com.USER_CONFIG_FILE), "w"
            ) as conf:
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


class RegistryTest:
    def __init__(self, install_loc: str, venv: pytest_virtualenv.VirtualEnv, port: int = 8000):
        self._install = install_loc
        self._venv = venv
        self._venv_dir = os.path.join(venv.workspace, ".env")
        self._process = None
        self._port = port
        if not os.path.exists(os.path.join(install_loc, "manage.py")):
            test_reg.install_registry(
                install_dir=install_loc, silent=True, venv_dir=self._venv_dir
            )
        # Start then stop to generate key
        _process = test_reg.launch(
            self._install, silent=True, venv_dir=self._venv_dir, port=self._port
        )
        while not os.path.exists(os.path.join(self._install, "token")):
            time.sleep(5)
        self._token = open(os.path.join(self._install, "token")).read().strip()
        assert self._token
        os.kill(_process.pid, signal.SIGTERM)

    def rebuild(self):
        test_reg.rebuild_local(
            os.path.join(self._venv_dir, "bin", "python"), self._install
        )

    def __enter__(self):
        try:
            self._process = test_reg.launch(
                self._install, silent=True, venv_dir=self._venv_dir, port=self._port
            )
        except KeyboardInterrupt as e:
            os.kill(self._process.pid, signal.SIGTERM)
            raise e

    def __exit__(self, type, value, tb):
        os.kill(self._process.pid, signal.SIGTERM)
        self._process = None


@pytest.fixture(scope="session")
def local_registry(session_virtualenv: pytest_virtualenv.VirtualEnv):
    if fdp_serv.check_server_running('http://127.0.0.1:8000'):
        pytest.skip("Cannot run registry tests, a server is already running on port 8000")
    with tempfile.TemporaryDirectory() as tempd:
        session_virtualenv.env = test_reg.django_environ(session_virtualenv.env)
        yield RegistryTest(tempd, session_virtualenv, port=8000)


@pytest.fixture(scope="session")
def remote_registry(session_virtualenv: pytest_virtualenv.VirtualEnv):
    if fdp_serv.check_server_running('http://127.0.0.1:8001'):
        pytest.skip("Cannot run registry tests, a server is already running on port 8001")
    with tempfile.TemporaryDirectory() as tempd:
        session_virtualenv.env = test_reg.django_environ(session_virtualenv.env)
        yield RegistryTest(tempd, session_virtualenv, port=8001)
