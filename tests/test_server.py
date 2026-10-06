import os
import time
import typing

import pytest
import pytest_mock

import fair.exceptions as fdp_exc
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
def test_registry_install_over_existing(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # An existing install is refused, and with 'force' it is removed before
    # the new one is cloned. The clone is stopped: nothing after it is at issue
    class _CloneReached(Exception):
        pass

    reg_dir = os.path.join(tmp_path, "registry")
    os.makedirs(reg_dir)
    _old_file = os.path.join(reg_dir, "db.sqlite3")
    open(_old_file, "w").close()
    mocker.patch(
        "fair.common.global_config_dir", lambda: os.path.join(tmp_path, "cli")
    )
    mocker.patch("git.Repo.clone_from", side_effect=_CloneReached)

    with pytest.raises(fdp_exc.RegistryError, match="already installed"):
        fdp_serv.install_registry(install_dir=reg_dir)
    assert os.path.exists(_old_file)

    with pytest.raises(_CloneReached):
        fdp_serv.install_registry(install_dir=reg_dir, force=True)
    assert not os.path.exists(reg_dir)


@pytest.mark.faircli_server
def test_registry_refuses_another_token(local_registry: conf.RegistryTest):
    # What tells a registry from another that answers at its address
    with local_registry:
        assert fdp_serv._accepts_token(
            local_registry._url, local_registry._token
        )
        assert not fdp_serv._accepts_token(local_registry._url, "0" * 40)


@pytest.mark.faircli_server
@pytest.mark.parametrize("status", (200, 403))
def test_launch_server_with_another_on_its_port(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    status: int,
):
    # Another registry that holds the port answers in place of the one being
    # started, and refuses its token (403); the start script succeeds either
    # way. The script is not run here: the answers are what is at issue
    reg_dir = os.path.join(tmp_path, "registry")
    os.makedirs(os.path.join(reg_dir, "scripts"))
    for _file, _text in (
        (os.path.join("scripts", "start_fair_registry"), ""),
        (os.path.join("scripts", "start_fair_registry_windows.bat"), ""),
        ("session_port.log", "8000"),
        ("session_address.log", "127.0.0.1"),
        ("token", "this-registrys-token"),
    ):
        with open(os.path.join(reg_dir, _file), "w") as out_f:
            out_f.write(_text)
    mocker.patch.dict(os.environ)
    mocker.patch("subprocess.Popen")

    def _answer(url, headers=None):
        # Any registry answers a request that carries no token
        return mocker.Mock(status_code=status if headers else 200)

    _get = mocker.patch("requests.get", side_effect=_answer)

    if status == 200:
        fdp_serv.launch_server(registry_dir=reg_dir)
    else:
        with pytest.raises(fdp_exc.RegistryError, match="refuses that"):
            fdp_serv.launch_server(registry_dir=reg_dir)
    assert _get.call_args == mocker.call(
        "http://127.0.0.1:8000/api/users/",
        headers={"Authorization": "token this-registrys-token"},
    )


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
