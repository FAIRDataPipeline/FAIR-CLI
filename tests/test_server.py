import glob
import os
import pathlib
import re
import sqlite3
import subprocess
import time
import typing

import click.testing
import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.registry.lifecycle as fdp_life
import fair.registry.server as fdp_serv
from fair.cli import cli

from . import conftest as conf
from .registry_database import FRESH, SCHEMA, make_install

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
    mocker.patch("fair.registry.server._refuse_failing")

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

    def _answer(url, headers=None, timeout=None, proxies=None):
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
        proxies={"http": None, "https": None, "all": None},
    )


@pytest.mark.faircli_server
def test_launch_server_that_answers_and_fails(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # This install's registry starts, and fails every request with a token, as
    # one whose database is broken does
    reg_dir = _registry_files(tmp_path, "this-registrys-token")
    mocker.patch.dict(os.environ)
    mocker.patch("subprocess.Popen")
    mocker.patch(
        "requests.get",
        side_effect=lambda url, **kwargs: mocker.Mock(
            status_code=500 if kwargs.get("headers") else 200
        ),
    )
    with pytest.raises(fdp_exc.RegistryError, match="HTTP 500") as e:
        fdp_serv.launch_server(registry_dir=reg_dir)
    assert os.path.join(reg_dir, "output.log") in e.value.hint


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


_URI = "http://127.0.0.1:8000/api/"


@pytest.fixture
def installed(tmp_path, mocker: pytest_mock.MockerFixture) -> typing.Dict:
    """An install with records, running for the user, its store, and a project

    The registry's server, its database's rebuilding and what is registered
    after are stood in for: the server runs while 'running' says so.
    """
    _store = tmp_path.joinpath("store")
    _store.joinpath("PSU").mkdir(parents=True)
    _store.joinpath("PSU", "data.csv").write_text("registered")
    _store.joinpath("my-model").mkdir()
    _store.joinpath("my-model", "README.md").write_text("a repository")
    _rows = FRESH + [
        f"INSERT INTO data_management_storageroot (root, local) VALUES "
        f"('file://{_store}/', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('PSU/data.csv', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('my-model', 1)",
        "INSERT INTO data_management_object (description, storage_location_id) "
        "VALUES ('Remote code repository.', 2)",
    ]
    _registry = make_install(tmp_path.joinpath("registry"), rows=_rows)
    _project = tmp_path.joinpath("project")
    _project.joinpath(fdp_com.FAIR_FOLDER).mkdir(parents=True)
    with open(fdp_com.staging_cache(str(_project)), "w") as out_f:
        yaml.dump(
            {"job": {"a-job": True}, "data_product": {"PSU:data@v1.0.0": False}}, out_f
        )
    _state = {"running": True}
    mocker.patch("fair.common.registry_home", lambda: _registry)
    mocker.patch("fair.configuration.get_local_uri", lambda: _URI)
    mocker.patch(
        "fair.registry.lifecycle._server_here",
        side_effect=lambda *args: True if _state["running"] else None,
    )
    mocker.patch(
        "fair.registry.server.stop_server",
        side_effect=lambda **kwargs: _state.update(running=False),
    )
    mocker.patch(
        "fair.registry.server.launch_server",
        side_effect=lambda **kwargs: _state.update(running=True),
    )
    mocker.patch("fair.registry.server._refuse_failing")
    mocker.patch("fair.registry.server._python_runs", lambda python: True)

    # A file of its own stands for the database built again
    def _build(python, registry_dir):
        with open(os.path.join(registry_dir, "db.sqlite3"), "w") as out_f:
            out_f.write("new")

    def _token(registry_dir):
        with open(os.path.join(registry_dir, "token"), "w") as out_f:
            out_f.write("new")

    with open(os.path.join(_registry, "token"), "w") as out_f:
        out_f.write("old")
    fdp_life.start_for_user(port=8000, registry_dir=_registry)
    return {
        "registry": _registry,
        "store": str(_store),
        "project": str(_project),
        "state": _state,
        "rebuild": mocker.patch(
            "fair.registry.server._build_database", side_effect=_build
        ),
        "token": mocker.patch(
            "fair.registry.server._write_missing_token", side_effect=_token
        ),
        "setup": mocker.patch("fair.registry.server.update_registry_post_setup"),
        "file_types": mocker.patch("fair.registry.storage.populate_file_type"),
    }


@pytest.mark.faircli_server
def test_a_reset_empties_and_starts_the_registry_again(installed: typing.Dict):
    _result = fdp_serv.reset_registry(repo_dir=installed["project"])
    installed["rebuild"].assert_called_once_with(
        fdp_serv._install_python(installed["registry"]), installed["registry"]
    )
    with open(os.path.join(installed["registry"], "db.sqlite3")) as in_f:
        assert in_f.read() == "new"
    assert not glob.glob(os.path.join(installed["registry"], ".reset-*"))
    with open(os.path.join(installed["registry"], "token")) as in_f:
        assert in_f.read() == "new"
    assert _result["restarted"] == _URI and installed["state"]["running"]
    assert fdp_life.status(installed["registry"])["server"]["holder"] == (
        fdp_life.HELD_BY_USER
    )
    installed["setup"].assert_called_once_with(
        installed["project"], True, installed["registry"]
    )
    with open(fdp_com.staging_cache(installed["project"])) as in_f:
        assert not any(yaml.safe_load(in_f).values())
    assert not os.path.exists(fdp_life.hold_file(installed["registry"]))
    # Its files stay unless asked
    assert os.path.exists(os.path.join(installed["store"], "PSU", "data.csv"))


@pytest.mark.faircli_server
def test_a_reset_deletes_the_stores_files_when_asked(installed: typing.Dict):
    installed["state"]["running"] = False
    fdp_life.stop_for_removal(installed["registry"])
    _result = fdp_serv.reset_registry(delete_files=True)
    assert _result["deleted"] == 1 and not _result["failed"]
    assert not os.path.exists(os.path.join(installed["store"], "PSU"))
    assert os.path.exists(os.path.join(installed["store"], "my-model", "README.md"))
    # Not running before, not started after; outside a project, the file types
    assert not _result["restarted"] and not installed["state"]["running"]
    installed["setup"].assert_not_called()
    installed["file_types"].assert_called_once()


@pytest.mark.faircli_server
def test_a_reset_waits_for_the_sessions(installed: typing.Dict):
    fdp_life.open_session(installed["registry"])
    with pytest.raises(fdp_exc.RegistryError, match="is in use"):
        fdp_serv.reset_registry()
    installed["rebuild"].assert_not_called()


@pytest.mark.faircli_server
def test_a_reset_of_a_registry_whose_python_has_gone_changes_nothing(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.registry.server._python_runs", lambda python: False)
    with pytest.raises(fdp_exc.RegistryError, match="cannot run its Python") as e:
        fdp_serv.reset_registry()
    assert "'fair registry upgrade'" in e.value.hint
    installed["rebuild"].assert_not_called()
    assert os.path.exists(os.path.join(installed["registry"], "db.sqlite3"))
    # Nothing is stopped
    assert installed["state"]["running"]
    assert fdp_life.status(installed["registry"])["server"]["holder"] == (
        fdp_life.HELD_BY_USER
    )


@pytest.mark.faircli_server
@pytest.mark.parametrize(
    "failure,raised",
    [
        (subprocess.CalledProcessError(1, "migrate"), fdp_exc.RegistryError),
        (KeyboardInterrupt(), KeyboardInterrupt),
    ],
)
def test_a_reset_that_fails_keeps_the_database_and_starts_the_registry_again(
    installed: typing.Dict, failure: BaseException, raised: typing.Type
):
    # The build fails, or is stopped by Ctrl-C, having begun a database
    def _failing(python, registry_dir):
        with open(os.path.join(registry_dir, "db.sqlite3"), "w") as out_f:
            out_f.write("half built")
        raise failure

    installed["rebuild"].side_effect = _failing
    with open(os.path.join(installed["registry"], "db.sqlite3"), "rb") as in_f:
        _records = in_f.read()
    with pytest.raises(raised) as e:
        fdp_serv.reset_registry(repo_dir=installed["project"])
    if raised is fdp_exc.RegistryError:
        assert "left as it was" in e.value.msg
        assert "'fair registry upgrade'" in e.value.hint
    with open(os.path.join(installed["registry"], "db.sqlite3"), "rb") as in_f:
        assert in_f.read() == _records
    with open(os.path.join(installed["registry"], "token")) as in_f:
        assert in_f.read() == "old"
    assert not glob.glob(os.path.join(installed["registry"], ".reset-*"))
    assert installed["state"]["running"]
    assert fdp_life.status(installed["registry"])["server"]["holder"] == (
        fdp_life.HELD_BY_USER
    )
    assert not os.path.exists(fdp_life.hold_file(installed["registry"]))
    installed["setup"].assert_not_called()


@pytest.mark.faircli_server
def test_registry_reset_command(installed: typing.Dict):
    _result = click.testing.CliRunner().invoke(cli, ["registry", "reset"])
    assert _result.exit_code == 1
    assert "'--yes'" in _result.output
    installed["rebuild"].assert_not_called()

    _result = click.testing.CliRunner().invoke(cli, ["registry", "reset", "--yes"])
    assert _result.exit_code == 0, _result.output
    assert "Emptied the local registry" in _result.output
    assert "run 'fair init'" in _result.output


# What 'git ls-remote --heads --tags' prints for the registry's repository
_LISTING = """\
1111111111111111111111111111111111111111\trefs/heads/main
2222222222222222222222222222222222222222\trefs/heads/rr/postgres
3333333333333333333333333333333333333333\trefs/tags/v1.9.0
4444444444444444444444444444444444444444\trefs/tags/v1.10.0
5555555555555555555555555555555555555555\trefs/tags/v1.10.0^{}
"""


@pytest.mark.faircli_server
@pytest.mark.parametrize(
    "given, checked_out",
    [
        ("v1.10.0", "v1.10.0"),
        ("rr/postgres", "origin/rr/postgres"),
        ("origin/rr/postgres", "origin/rr/postgres"),
        ("main", "origin/main"),
    ],
)
def test_a_version_is_found_by_its_tag_or_its_branch(
    mocker: pytest_mock.MockerFixture, given: str, checked_out: str
):
    mocker.patch("git.cmd.Git.ls_remote", return_value=_LISTING)
    assert fdp_serv.resolve_reference(given) == checked_out


@pytest.mark.faircli_server
def test_a_version_the_repository_lacks_names_its_newest_tags(
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("git.cmd.Git.ls_remote", return_value=_LISTING)
    with pytest.raises(fdp_exc.RegistryError, match="No tag or branch 'v9'") as e:
        fdp_serv.resolve_reference("v9")
    assert "v1.10.0, v1.9.0" in e.value.hint


@pytest.mark.faircli_server
def test_an_install_refused_names_upgrade(mocker: pytest_mock.MockerFixture, tmp_path):
    reg_dir = _existing_install(tmp_path, mocker)
    with pytest.raises(fdp_exc.RegistryError, match="already installed") as e:
        fdp_serv.install_registry(install_dir=reg_dir)
    assert "'fair registry upgrade --version <version>'" in e.value.hint
    assert "'fair registry install --force'" in e.value.hint


@pytest.mark.faircli_server
def test_an_install_looks_where_the_configuration_puts_the_registry(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    reg_dir = _existing_install(tmp_path, mocker)
    mocker.patch("fair.common.registry_home", lambda: reg_dir)
    _build = mocker.patch("fair.registry.server._build_registry")
    _refused = f"already installed in {re.escape(reg_dir)}"
    with pytest.raises(fdp_exc.RegistryError, match=_refused):
        fdp_serv.install_registry()
    _build.assert_not_called()


# A build of a version in an install's place: a database with the tables given
# and none of the old install's records, and a file to tell it by
def _build_with(schema: typing.List[str]) -> typing.Callable:
    def _build(repository, reference, install_dir, silent, venv_dir, venv_cmd):
        make_install(pathlib.Path(install_dir), schema=schema)
        with open(os.path.join(install_dir, "built"), "w") as out_f:
            out_f.write("v9.9.9")
        return "v9.9.9"

    return _build


# How many storage roots an install's database holds
def _storage_roots(registry_dir: str) -> int:
    _db = sqlite3.connect(os.path.join(registry_dir, "db.sqlite3"))
    try:
        return _db.execute(
            "SELECT COUNT(*) FROM data_management_storageroot"
        ).fetchone()[0]
    finally:
        _db.close()


@pytest.mark.faircli_server
def test_an_upgrade_keeps_the_records_and_starts_the_registry_again(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    mocker.patch(
        "fair.registry.server._build_registry", side_effect=_build_with(SCHEMA)
    )
    _migrate = mocker.patch("fair.registry.server._migrate")
    _result = fdp_serv.upgrade_registry()
    assert _result == {"version": "v9.9.9", "kept": True, "restarted": _URI}
    assert os.path.exists(os.path.join(installed["registry"], "built"))
    assert _storage_roots(installed["registry"]) == 1
    with open(os.path.join(installed["registry"], "token")) as in_f:
        assert in_f.read() == "old"
    _migrate.assert_called_once()
    assert installed["state"]["running"]
    assert not os.path.exists(fdp_life.hold_file(installed["registry"]))
    # Nothing is left beside it
    assert sorted(os.listdir(os.path.dirname(installed["registry"]))) == [
        "project",
        "registry",
        "store",
    ]


@pytest.mark.faircli_server
def test_an_upgrade_to_other_tables_leaves_the_install_as_it_was(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    _other = [
        _statement.replace(
            '"local" bool NOT NULL)', '"local" bool NOT NULL, "x" int NULL)'
        )
        for _statement in SCHEMA
    ]
    mocker.patch(
        "fair.registry.server._build_registry", side_effect=_build_with(_other)
    )
    _migrate = mocker.patch("fair.registry.server._migrate")
    with pytest.raises(fdp_exc.RegistryError, match="cannot be kept") as e:
        fdp_serv.upgrade_registry()
    assert "'fair registry install --force --version v9.9.9'" in e.value.hint
    assert not os.path.exists(os.path.join(installed["registry"], "built"))
    assert _storage_roots(installed["registry"]) == 1
    _migrate.assert_not_called()
    # Running again, as it was
    assert installed["state"]["running"]
    assert sorted(os.listdir(os.path.dirname(installed["registry"]))) == [
        "project",
        "registry",
        "store",
    ]


@pytest.mark.faircli_server
def test_an_upgrade_of_an_install_with_no_database(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    os.remove(os.path.join(installed["registry"], "db.sqlite3"))
    mocker.patch(
        "fair.registry.server._build_registry", side_effect=_build_with(SCHEMA)
    )
    _migrate = mocker.patch("fair.registry.server._migrate")
    assert not fdp_serv.upgrade_registry()["kept"]
    _migrate.assert_not_called()
    assert _storage_roots(installed["registry"]) == 0


@pytest.mark.faircli_server
def test_an_upgrade_waits_for_the_sessions(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    _build = mocker.patch("fair.registry.server._build_registry")
    fdp_life.open_session(installed["registry"])
    with pytest.raises(fdp_exc.RegistryError, match="is in use"):
        fdp_serv.upgrade_registry()
    _build.assert_not_called()


@pytest.mark.faircli_server
def test_registry_install_checks_the_version_before_stopping_anything(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    mocker.patch("git.cmd.Git.ls_remote", return_value=_LISTING)
    _stop = mocker.patch("fair.registry.lifecycle.stop_for_removal")
    _result = click.testing.CliRunner().invoke(
        cli, ["registry", "install", "--force", "--version", "v9"]
    )
    assert _result.exit_code != 0
    assert "No tag or branch 'v9'" in _result.output
    _stop.assert_not_called()
    assert installed["state"]["running"]


@pytest.mark.faircli_server
def test_registry_install_without_force_names_upgrade(installed: typing.Dict):
    _result = click.testing.CliRunner().invoke(cli, ["registry", "install"])
    assert _result.exit_code != 0
    assert f"already installed in {installed['registry']}" in _result.output
    assert "'fair registry upgrade --version <version>'" in _result.output


@pytest.mark.faircli_server
def test_registry_upgrade_command(
    installed: typing.Dict, mocker: pytest_mock.MockerFixture
):
    mocker.patch(
        "fair.registry.server._build_registry", side_effect=_build_with(SCHEMA)
    )
    mocker.patch("fair.registry.server._migrate")
    _result = click.testing.CliRunner().invoke(cli, ["registry", "upgrade"])
    assert _result.exit_code == 0, _result.output
    assert "Installed registry version 'v9.9.9'" in _result.output
    assert "keeping its records" in _result.output
    assert f"It is running again at {_URI}" in _result.output
