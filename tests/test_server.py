import os
import tempfile
import time
import typing

import pytest
import pytest_mock

import fair.registry.server as fdp_serv

from . import conftest as conf

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"


@pytest.mark.faircli_server
def test_check_server_running(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    assert not fdp_serv.check_server_running("http://127.0.0.1:9999/api")
    with local_registry:
        assert fdp_serv.check_server_running(LOCAL_REGISTRY_URL)


@pytest.mark.faircli_server
def test_launch_stop_server(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        fdp_serv.launch_server()
        time.sleep(2)
        fdp_serv.stop_server(force=True)


@pytest.mark.faircli_server
def test_registry_install_uninstall(mocker: pytest_mock.MockerFixture):
    with tempfile.TemporaryDirectory() as tempd:
        reg_dir = os.path.join(tempd, "registry")
        mocker.patch("fair.common.DEFAULT_REGISTRY_LOCATION", reg_dir)
        fdp_serv.install_registry(install_dir=reg_dir)
        assert os.path.exists(os.path.join(reg_dir, "db.sqlite3"))
        fdp_serv.uninstall_registry()
