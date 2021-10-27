import tempfile
import pytest
import pytest_mock
import time
import requests
import typing
import os

from . import conftest as conf
import fair.registry.server as fdp_serv

LOCAL_REGISTRY_URL = 'http://localhost:8000/api'


@pytest.mark.server
def test_check_server_running(local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    assert not fdp_serv.check_server_running('http://localhost:9999/api')
    with local_registry:
        assert fdp_serv.check_server_running(LOCAL_REGISTRY_URL)

@pytest.mark.server
def test_launch_stop_server(local_config: typing.Tuple[str, str], local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    with local_registry:
        fdp_serv.launch_server()
        time.sleep(2)
        fdp_serv.stop_server(force=True)

@pytest.mark.server
def test_registry_install_uninstall(mocker: pytest_mock.MockerFixture):
    with tempfile.TemporaryDirectory() as tempd:
        mocker.patch('fair.registry.server.DEFAULT_REGISTRY_LOCATION', tempd)
        fdp_serv.install_registry(install_dir=tempd)
        assert os.path.exists(os.path.join(tempd, 'db.sqlite3'))
        fdp_serv.uninstall_registry()

