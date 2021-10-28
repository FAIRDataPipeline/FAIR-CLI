import os
import pytest
import pytest_mock
import pytest_fixture_config
import pytest_virtualenv
import logging
import tempfile
import yaml
import fair.testing as fdp_test
import fair.common as fdp_com

from fair.common import FAIR_FOLDER
from . import registry_install as test_reg

TEST_JOB_FILE_TIMESTAMP = '2021-10-11_10_0_0_100000'
REGISTRY_INSTALL_URL = "https://data.scrc.uk/static/localregistry.sh"


logging.getLogger('FAIRDataPipeline').setLevel(logging.DEBUG)

@pytest.fixture(scope='session')
@pytest_fixture_config.yield_requires_config(pytest_virtualenv.FixtureConfig(
    virtualenv_executable='venv',
), ['virtualenv_executable'])
def session_virtualenv():
    """ Function-scoped virtualenv in a temporary workspace.

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
        with tempfile.TemporaryDirectory() as templ:
            os.makedirs(os.path.join(templ, fdp_com.FAIR_FOLDER))
            os.makedirs(os.path.join(tempg, fdp_com.FAIR_FOLDER, 'registry'))
            _lconfig_path = os.path.join(templ, fdp_com.FAIR_FOLDER, 'cli-config.yaml')
            _gconfig_path = os.path.join(tempg, fdp_com.FAIR_FOLDER, 'cli-config.yaml')
            _cfgl = fdp_test.create_configurations(templ, None, templ, True)
            _cfgg = fdp_test.create_configurations(tempg, None, tempg, True)
            yaml.dump(_cfgl, open(_lconfig_path, 'w'))
            yaml.dump(_cfgg, open(_gconfig_path, 'w'))
            mocker.patch('fair.common.global_config_dir', lambda: os.path.dirname(_gconfig_path))
            mocker.patch('fair.common.global_fdpconfig', lambda: _gconfig_path)
            yield (tempg, templ)


@pytest.fixture
def job_directory(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set default to point to temporary
        mocker.patch('fair.common.default_jobs_dir', lambda: tempd)

        # Create a mock job directory
        os.makedirs(os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP))
        yield os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP)


@pytest.fixture
def job_log(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set the log directory
        mocker.patch('fair.history.history_directory', lambda *args: tempd)

        # Create mock job log
        with open(os.path.join(tempd, f'job_{TEST_JOB_FILE_TIMESTAMP}.log'), 'w') as out_f:
            out_f.write('''--------------------------------
 Commenced = Fri Oct 08 14:45:43 2021 
 Author    = Interface Test <test@noreply>
 Command   = fair pull
--------------------------------
------- time taken 0:00:00.791088 -------''')

        yield tempd


class TestRegistry:
    def __init__(self, install_loc: str, venv_dir: str):
        self._install = install_loc
        self._venv = os.path.join(venv_dir, '.env')
        self._process = None
        if not os.path.exists(os.path.join(install_loc, 'manage.py')):
            test_reg.install_registry(install_dir=install_loc, silent=True, venv_dir=self._venv)

    def rebuild(self):
        test_reg.rebuild_local(os.path.join(self._venv, 'bin', 'python'), self._install)

    def __enter__(self):
        try:
            self._process = test_reg.launch(self._install, silent=True, venv_dir=self._venv)
        except KeyboardInterrupt as e:
            self._process.kill()
            self._process.wait()
            raise e

    def __exit__(self, type, value, tb):
        self._process.kill()
        self._process.wait()
        self._process = None


@pytest.fixture(scope="session")
def local_registry(session_virtualenv: pytest_virtualenv.VirtualEnv):
    with tempfile.TemporaryDirectory() as tempd:
        session_virtualenv.env = test_reg.django_environ(session_virtualenv.env)
        yield TestRegistry(tempd, session_virtualenv.workspace)
