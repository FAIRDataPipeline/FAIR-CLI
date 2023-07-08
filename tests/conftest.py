import logging
import os
import shutil
import signal
import time
import typing
import subprocess
import platform

import git
import pytest
import pytest_fixture_config
import pytest_mock
import pytest_virtualenv
import yaml

import boto3
import os
from moto.server import ThreadedMotoServer

import requests
from urllib3.exceptions import InsecureRequestWarning

import fair.common as fdp_com
import fair.registry.server as fdp_serv
import fair.testing as fdp_test

from . import registry_install as test_reg

TEST_JOB_FILE_TIMESTAMP = "2021-10-11_10_0_0_100000"
PYTHON_API_GIT = "https://github.com/FAIRDataPipeline/pyDataPipeline.git"
PYTHON_MODEL_GIT = "https://github.com/FAIRDataPipeline/pySimpleModel.git"

TEST_OUT_DIR = os.path.join(os.getcwd(), "test_outputs")
os.makedirs(TEST_OUT_DIR, exist_ok=True)


logging.getLogger("FAIRDataPipeline").setLevel(logging.DEBUG)

def test_can_be_run(url):
    _header = {"Accept": "application/json"}
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    _response = requests.get(url, verify = False, headers = _header, allow_redirects = True)
    if _response.status_code == 200:
        return True
    return False

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
        "_example_data.py",
    )

    _objects: typing.List[typing.Tuple[str, str, str]] = []

    with open(_example_file) as in_f:
        _lines = in_f.readlines()
        for i, line in enumerate(_lines):
            if SEARCH_STR in line:
                _path_line_offset = 0
                while "path" not in _lines[i + _path_line_offset]:
                    _path_line_offset += 1
                _candidate = _lines[i + _path_line_offset]
                _candidate = _candidate.replace("path=", "")
                if _candidate.strip()[0] == "(":
                    ii = i + _path_line_offset + 1
                    while ")" not in _candidate:
                        _candidate += _lines[ii].strip()
                        ii += 1
                _candidate = _candidate.replace('"', "")
                _candidate = _candidate.replace("(", "")
                _candidate = _candidate.replace(")", "")
                _candidate = _candidate.replace(",", "")
                _metadata, _file = _candidate.rsplit("/", 1)
                _version = ".".join(_file.split(".")[:3])
                _objects.append((*_metadata.split("/", 1), _version))

    return _objects


@pytest.fixture(scope="module")
def pyDataPipeline(tmp_path_factory):
    _repo_path = tmp_path_factory.mktemp("repo_path").__str__()
    _repo = git.Repo.clone_from(PYTHON_API_GIT, _repo_path)
    _repo.git.checkout("main")
    _model_path = os.path.join(_repo_path, "model")
    _model = git.Repo.clone_from(PYTHON_MODEL_GIT, _model_path)
    _model.git.checkout("main")
    simple_model = os.path.join(_model_path, "simpleModel")
    shutil.move(simple_model, _repo_path)
    yield _repo_path

@pytest.fixture()
def pySimpleModel(tmp_path):
    _repo_path = tmp_path.__str__()
    _repo = git.Repo.clone_from(PYTHON_MODEL_GIT, _repo_path)
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

@pytest.fixture(scope='module')
def monkeypatch_module():
    from _pytest.monkeypatch import MonkeyPatch
    m = MonkeyPatch()
    yield m
    m.undo()

@pytest.fixture
def local_config(mocker: pytest_mock.MockerFixture, tmp_path):
    tempg = os.path.join(tmp_path, "tempg").__str__()
    templ = os.path.join(tmp_path, "tempd").__str__()
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
    os.makedirs(os.path.join(templ, fdp_com.FAIR_FOLDER))
    _lconfig_path = os.path.join(
        templ, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )
    _cfgl = fdp_test.create_configurations(
        templ, None, None, templ, True
    )
    yaml.dump(_cfgl, open(_lconfig_path, "w"))
    with open(
        os.path.join(templ, fdp_com.USER_CONFIG_FILE), "w"
    ) as conf:
        yaml.dump({"run_metadata": {}}, conf)
    mocker.patch("fair.common.find_fair_root", lambda *args: templ)
    yield (tempg, templ)

@pytest.fixture(scope = "module")
def global_config(monkeypatch_module, tmp_path_factory):
    tempg = tmp_path_factory.mktemp("tempg").__str__()
    os.makedirs(os.path.join(tempg, fdp_com.FAIR_FOLDER, "registry"))
    os.makedirs(os.path.join(tempg, fdp_com.FAIR_FOLDER, "sessions"))
    _gconfig_path = os.path.join(
        tempg, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )
    _cfgg = fdp_test.create_configurations(tempg, None, None, tempg, True)
    yaml.dump(_cfgg, open(_gconfig_path, "w"))
    monkeypatch_module.setattr(
        "fair.common.global_config_dir",
        lambda: os.path.dirname(_gconfig_path),
    )
    monkeypatch_module.setattr("fair.common.global_fdpconfig", lambda: _gconfig_path)
    yield (tempg)

@pytest.fixture(scope = "module")
def job_directory(monkeypatch_module, tmp_path_factory) -> str:
    tempd = tmp_path_factory.mktemp("tempd").__str__()
    # Set default to point to temporary
    monkeypatch_module.setattr("fair.common.default_jobs_dir", lambda *args: tempd)
    # Create a mock job directory
    os.makedirs(os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP))
    yield os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP)

@pytest.fixture(scope = "module")
def job_log(monkeypatch_module, tmp_path_factory) -> str:
    tempd = tmp_path_factory.mktemp("tempd").__str__()
    # Set the log directory
    monkeypatch_module.setattr("fair.history.history_directory", lambda *args: tempd)
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
    def __init__(
        self,
        install_loc: str,
        venv: pytest_virtualenv.VirtualEnv,
        port: int = 8000,
        remote: bool = False
    ):
        self._install = install_loc
        self._venv = venv
        self._venv_dir = os.path.join(venv.workspace, ".env")
        self._process = None
        self._port = port
        self._url = f"http://127.0.0.1:{port}/api/"
        self._remote = remote
        if not os.path.exists(os.path.join(install_loc, "manage.py")):
            test_reg.install_registry(
                install_dir=install_loc, silent=True, venv_dir=self._venv_dir, remote=self._remote
            )
        # Start then stop to generate key
        _process = test_reg.launch(
            self._install,
            silent=True,
            venv_dir=self._venv_dir,
            port=self._port,
            remote= remote
        )
        while not os.path.exists(os.path.join(self._install, "token")):
            time.sleep(3)
        self._token = open(os.path.join(self._install, "token")).read().strip()
        assert self._token
        pid_kill(_process.pid)

    def rebuild(self):
        _venv_bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
        test_reg.rebuild_local(
            os.path.join(self._venv_dir, _venv_bin_dir, "python"), self._install, remote=self._remote
        )
    
    def launch(self):
        self._process = test_reg.launch(
            self._install,
            silent=True,
            venv_dir=self._venv_dir,
            port=self._port,
            remote=self._remote
        )
            
    def kill(self):
        if self._process:
            pid_kill(self._process.pid)

    def __enter__(self):
        try:
            self._process = test_reg.launch(
                self._install,
                silent=True,
                venv_dir=self._venv_dir,
                port=self._port,
                remote=self._remote
            )                

        except KeyboardInterrupt as e:
            pid_kill(self._process.pid)
            raise e

    def __exit__(self, type, value, tb):
        pid_kill(self._process.pid)
        self._process = None

@pytest.fixture(scope="module")
def local_registry(session_virtualenv: pytest_virtualenv.VirtualEnv, tmp_path_factory):
    if fdp_serv.check_server_running("http://127.0.0.1:8000"):
        pytest.skip(
            "Cannot run registry tests, a server is already running on port 8000"
        )
    session_virtualenv.env = test_reg.django_environ(
        session_virtualenv.env
    )
    tempd = tmp_path_factory.mktemp("tempd").__str__()
    rtest = RegistryTest(tempd, session_virtualenv, port=8000)
    yield rtest
    if rtest._process:
        pid_kill(rtest._process.pid)
    print("TearDown of Local Registry Complete")

@pytest.fixture(scope="module")
def remote_registry(session_virtualenv: pytest_virtualenv.VirtualEnv, tmp_path_factory):
    if fdp_serv.check_server_running("http://127.0.0.1:8001"):
        pytest.skip(
            "Cannot run registry tests, a server is already running on port 8001"
        )
    session_virtualenv.env = test_reg.django_environ(
        session_virtualenv.env, True
    )
    tempd = tmp_path_factory.mktemp("tempd").__str__()
    rtest = RegistryTest(tempd, session_virtualenv, port=8001, remote= True)
    yield rtest
    if rtest._process:
        pid_kill(rtest._process.pid)
    print("TearDown of Remote Registry Complete")

@pytest.fixture(scope="module")
def fair_bucket(port: int = 3005):
    yield MotoTestServer(port)

class MotoTestServer:
    def __init__(
        self,
        port: int = 3005
    ):
        """Mocked AWS Credentials for moto."""
        os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
        os.environ['AWS_SECURITY_TOKEN'] = 'testing'
        os.environ['AWS_SESSION_TOKEN'] = 'testing'
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        self._port = port
        self._server = ThreadedMotoServer(port = port)
    def __enter__(self):
        try:
            print("Starting Moto Server")       
            self._server.start()
            print("creating Bucket")
            server_client = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{self._port}")
            server_client.create_bucket(Bucket="fair")         
        except KeyboardInterrupt as e:
            self._server.stop()
            raise e
    def __exit__(self, type, value, tb):
        self._server.stop()

def pid_kill(pid):
    if platform.system() == "Windows":
        subprocess.call(['taskkill', '/F', '/T', '/PID',  str(pid)])
    else:
        os.kill(pid, signal.SIGTERM)
