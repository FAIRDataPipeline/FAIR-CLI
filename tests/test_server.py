import os
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
def test_registry_install_uninstall(mocker: pytest_mock.MockerFixture, tmp_path):
    tempd = tmp_path.__str__()
    reg_dir = os.path.join(tempd, "registry")
    mocker.patch("fair.common.DEFAULT_REGISTRY_LOCATION", reg_dir)
    fdp_serv.install_registry(install_dir=reg_dir)
    assert os.path.exists(os.path.join(reg_dir, "db.sqlite3"))
    fdp_serv.uninstall_registry()


@pytest.mark.faircli_server
def test_launch_stop_server(
    local_config: typing.Tuple[str, str], mocker: pytest_mock.MockerFixture, tmp_path
):
    tempd = tmp_path.__str__()
    reg_dir = os.path.join(tempd, "registry")
    mocker.patch("fair.common.DEFAULT_REGISTRY_LOCATION", reg_dir)
    fdp_serv.install_registry(install_dir=reg_dir)
    fdp_serv.launch_server()
    time.sleep(5)
    fdp_serv.stop_server(force=True)


@pytest.mark.faircli_server
def test_launch_stop_server_with_port(
    local_config: typing.Tuple[str, str], mocker: pytest_mock.MockerFixture, tmp_path
):
    tempd = tmp_path.__str__()
    reg_dir = os.path.join(tempd, "registry")
    mocker.patch("fair.common.DEFAULT_REGISTRY_LOCATION", reg_dir)
    fdp_serv.install_registry(install_dir=reg_dir)
    fdp_serv.launch_server(port=8005, address="0.0.0.0", verbose=True)
    time.sleep(5)
    fdp_serv.stop_server(force=True, local_uri="http://127.0.0.1:8005/api")
