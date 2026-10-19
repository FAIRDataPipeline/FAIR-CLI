import os
import time
import typing

import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
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


# A stand-in for an installed registry, with the global configuration naming
# another directory, so that a test can see whether the install rewrote it
def _existing_install(tmp_path, mocker: pytest_mock.MockerFixture) -> str:
    _cli_dir = os.path.join(tmp_path, "cli")
    mocker.patch("fair.common.global_config_dir", lambda: _cli_dir)
    os.makedirs(_cli_dir)
    with open(fdp_com.global_fdpconfig(), "w") as conf_f:
        yaml.dump(
            {"registries": {"local": {"directory": "elsewhere"}}}, conf_f
        )
    reg_dir = os.path.join(tmp_path, "registry")
    os.makedirs(reg_dir)
    with open(os.path.join(reg_dir, "db.sqlite3"), "w") as db_f:
        db_f.write("old")
    return reg_dir


# The registry directory the global configuration names
def _configured_directory() -> str:
    with open(fdp_com.global_fdpconfig()) as conf_f:
        return yaml.safe_load(conf_f)["registries"]["local"]["directory"]


@pytest.mark.faircli_server
def test_registry_install_refuses_existing(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    reg_dir = _existing_install(tmp_path, mocker)
    _build = mocker.patch("fair.registry.server._build_registry")
    with pytest.raises(fdp_exc.RegistryError, match="already installed"):
        fdp_serv.install_registry(install_dir=reg_dir)
    _build.assert_not_called()
    with open(os.path.join(reg_dir, "db.sqlite3")) as db_f:
        assert db_f.read() == "old"


@pytest.mark.faircli_server
@pytest.mark.parametrize(
    "failure",
    [fdp_exc.RegistryError("No such head or tag"), KeyboardInterrupt()],
)
def test_registry_reinstall_that_fails_keeps_the_old(
    mocker: pytest_mock.MockerFixture, tmp_path, failure: BaseException
):
    # Whatever stops the new install, the old one is put back as it was, and
    # the configuration is not rewritten
    reg_dir = _existing_install(tmp_path, mocker)

    def _fail(repository, reference, install_dir, *args):
        os.makedirs(install_dir)
        open(os.path.join(install_dir, "partial"), "w").close()
        raise failure

    mocker.patch("fair.registry.server._build_registry", side_effect=_fail)
    with pytest.raises(type(failure)):
        fdp_serv.install_registry(install_dir=reg_dir, force=True)
    assert os.listdir(reg_dir) == ["db.sqlite3"]
    with open(os.path.join(reg_dir, "db.sqlite3")) as db_f:
        assert db_f.read() == "old"
    assert sorted(os.listdir(tmp_path)) == ["cli", "registry"]
    assert _configured_directory() == "elsewhere"


@pytest.mark.faircli_server
def test_registry_reinstall_replaces_the_old(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    reg_dir = _existing_install(tmp_path, mocker)

    def _build(repository, reference, install_dir, *args):
        os.makedirs(install_dir)
        with open(os.path.join(install_dir, "db.sqlite3"), "w") as db_f:
            db_f.write("new")
        return "v9.9.9"

    mocker.patch("fair.registry.server._build_registry", side_effect=_build)
    assert (
        fdp_serv.install_registry(install_dir=reg_dir, force=True) == "v9.9.9"
    )
    with open(os.path.join(reg_dir, "db.sqlite3")) as db_f:
        assert db_f.read() == "new"
    assert sorted(os.listdir(tmp_path)) == ["cli", "registry"]
    assert _configured_directory() == reg_dir


@pytest.mark.faircli_server
def test_registry_install_that_fails_leaves_nothing(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # A first install that fails leaves no directory behind, so that the next
    # attempt is not refused as already installed
    mocker.patch(
        "fair.common.global_config_dir", lambda: os.path.join(tmp_path, "cli")
    )
    reg_dir = os.path.join(tmp_path, "registry")

    def _fail(repository, reference, install_dir, *args):
        os.makedirs(install_dir)
        raise fdp_exc.RegistryError("No such head or tag")

    mocker.patch("fair.registry.server._build_registry", side_effect=_fail)
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.install_registry(install_dir=reg_dir)
    assert not os.path.exists(reg_dir)


@pytest.mark.faircli_server
def test_registry_reinstall_that_cannot_move_the_old(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # As on Windows when the registry is running and its files are in use
    reg_dir = _existing_install(tmp_path, mocker)
    _build = mocker.patch("fair.registry.server._build_registry")
    mocker.patch("os.rename", side_effect=PermissionError("in use"))
    with pytest.raises(fdp_exc.RegistryError, match="aside"):
        fdp_serv.install_registry(install_dir=reg_dir, force=True)
    _build.assert_not_called()
    assert os.listdir(reg_dir) == ["db.sqlite3"]
    assert sorted(os.listdir(tmp_path)) == ["cli", "registry"]


@pytest.mark.faircli_server
def test_registry_reinstall_at_a_missing_version(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # The registry repository is really cloned; the version is not in it
    reg_dir = _existing_install(tmp_path, mocker)
    with pytest.raises(
        fdp_exc.RegistryError, match="registry repository. Candidates are"
    ):
        fdp_serv.install_registry(
            install_dir=reg_dir, reference="no-such-branch", force=True
        )
    with open(os.path.join(reg_dir, "db.sqlite3")) as db_f:
        assert db_f.read() == "old"
    assert sorted(os.listdir(tmp_path)) == ["cli", "registry"]


@pytest.mark.faircli_server
def test_registry_refuses_another_token(local_registry: conf.RegistryTest):
    # What tells a registry from another that answers at its address
    with local_registry:
        assert fdp_serv._accepts_token(
            local_registry._url, local_registry._token
        )
        assert not fdp_serv._accepts_token(local_registry._url, "0" * 40)


# A registry directory with the files a start reads, and nothing to run: the
# start script is not run by the tests that use it
def _registry_files(tmp_path, token: typing.Optional[str]) -> str:
    reg_dir = os.path.join(tmp_path, "registry")
    os.makedirs(os.path.join(reg_dir, "scripts"))
    _files = [
        (os.path.join("scripts", "start_fair_registry"), ""),
        (os.path.join("scripts", "start_fair_registry_windows.bat"), ""),
        ("session_port.log", "8000"),
        ("session_address.log", "127.0.0.1"),
    ]
    if token is not None:
        _files.append(("token", token))
    for _file, _text in _files:
        with open(os.path.join(reg_dir, _file), "w") as out_f:
            out_f.write(_text)
    return reg_dir


@pytest.mark.faircli_server
@pytest.mark.parametrize("token", ["this-registrys-token", "", None])
def test_launch_server_waits_and_writes_its_token(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    token,
):
    # The start script can return before the server answers, and without
    # writing its token, as on Windows with no console: the server is waited
    # for, and a missing or empty token written by the registry's own command
    reg_dir = _registry_files(tmp_path, token)
    mocker.patch.dict(os.environ)
    mocker.patch("subprocess.Popen")
    _answers = iter([False, False, True])
    mocker.patch(
        "fair.registry.server.check_server_running",
        side_effect=lambda *args: next(_answers),
    )
    _sleep = mocker.patch("fair.registry.server.time.sleep")
    _get_token = mocker.patch(
        "fair.registry.server.subprocess.check_output",
        return_value="a-new-token\n",
    )
    _accepts = mocker.patch(
        "fair.registry.server._accepts_token", return_value=True
    )

    fdp_serv.launch_server(registry_dir=reg_dir)

    assert _sleep.call_count == 2
    _wanted = token or "a-new-token"
    assert _accepts.call_args.args[1] == _wanted
    assert _get_token.called == (not token)
    if not token:
        assert _get_token.call_args.args[0][1:] == [
            os.path.join(reg_dir, "manage.py"),
            "get_token",
        ]


@pytest.mark.faircli_server
def test_launch_server_with_no_answer(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    reg_dir = _registry_files(tmp_path, "this-registrys-token")
    mocker.patch.dict(os.environ)
    mocker.patch("subprocess.Popen")
    mocker.patch(
        "fair.registry.server.check_server_running", return_value=False
    )
    _sleep = mocker.patch("fair.registry.server.time.sleep")
    with pytest.raises(fdp_exc.RegistryError, match="no response"):
        fdp_serv.launch_server(registry_dir=reg_dir)
    assert _sleep.call_count == 30


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

    def _answer(url, headers=None, timeout=None):
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
        timeout=fdp_serv.REQUEST_SECONDS,
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
