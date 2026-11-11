import os
import pathlib
import platform
import typing

import click.testing
import pytest
import pytest_mock

import fair.exceptions as fdp_exc
import fair.registry.database as fdp_db
import fair.registry.stores as fdp_stores
from fair.cli import cli

from .registry_database import FRESH, make_install

URI = "http://127.0.0.1:8000/api/"
_HASH = "0123456789abcdef0123456789abcdef01234567"

# What is in the store, by path within it: registered, or not and what a
# clean does with it by default
_REGISTERED = ["PSU/data/1.0.0.csv", "inner/x.csv"]
_DELETED = ["PSU/data/0.9.0.csv", "odd/notes.txt", "inner/y.csv"]
_KEPT = [
    "testing/out/dat-1a2b.csv",
    f"testing/out/{_HASH}.csv",
    "jobs/2021/config.yaml",
]
_REPOSITORY = "my-model/README.md"


@pytest.fixture
def store(tmp_path: pathlib.Path, mocker: pytest_mock.MockerFixture) -> typing.Dict:
    """A registry whose store holds files of each kind, and a store inside it"""
    _store = tmp_path.joinpath("store")
    for _path in _REGISTERED + _DELETED + _KEPT + [_REPOSITORY]:
        _store.joinpath(_path).parent.mkdir(parents=True, exist_ok=True)
        _store.joinpath(_path).write_text(_path)
    _rows = FRESH + [
        f"INSERT INTO data_management_storageroot (root, local) VALUES "
        f"('file://{_store}/', 1)",
        f"INSERT INTO data_management_storageroot (root, local) VALUES "
        f"('file://{_store.joinpath('inner')}/', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('PSU/data/1.0.0.csv', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('PSU/gone/1.0.0.csv', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('my-model', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('x.csv', 2)",
        "INSERT INTO data_management_object (description, storage_location_id) "
        "VALUES ('Remote code repository.', 3)",
    ]
    _registry = make_install(tmp_path.joinpath("registry"), rows=_rows)
    mocker.patch("fair.common.registry_home", lambda: _registry)
    mocker.patch("fair.configuration.get_local_uri", lambda: URI)
    mocker.patch("fair.registry.lifecycle._server_here", lambda *args: None)
    return {"registry": _registry, "store": str(_store)}


# The paths within the store of what a search found
def _within(store: typing.Dict, paths: typing.List[str]) -> typing.List[str]:
    return sorted(
        os.path.relpath(_path, store["store"]).replace(os.sep, "/") for _path in paths
    )


# The files left in the store, by path within it
def _left(store: typing.Dict) -> typing.List[str]:
    return sorted(
        os.path.relpath(os.path.join(_root, _name), store["store"]).replace(os.sep, "/")
        for _root, _, _names in os.walk(store["store"])
        for _name in _names
    )


@pytest.mark.faircli_stores
def test_what_a_clean_finds(store: typing.Dict):
    _found = fdp_stores.find_files(fdp_db.local_stores(store["registry"]))
    assert _within(store, _found["delete"]) == sorted(_DELETED)
    assert _within(store, _found["kept"]) == sorted(_KEPT)
    assert _within(store, _found["missing"]) == ["PSU/gone/1.0.0.csv"]

    _found = fdp_stores.find_files(
        fdp_db.local_stores(store["registry"]), delete_all=True
    )
    assert _within(store, _found["delete"]) == sorted(_DELETED + _KEPT)
    assert not _found["kept"]


@pytest.mark.faircli_stores
def test_a_dry_run_lists_and_deletes_nothing(
    store: typing.Dict, capsys: pytest.CaptureFixture
):
    assert fdp_stores.clean_stores(dry_run=True) == 0
    _out = capsys.readouterr().out
    assert (
        "Would delete 3 files that the registry does not hold, from 2 data stores"
        in _out
    )
    assert f"To delete: {os.path.join(store['store'], 'odd', 'notes.txt')}" in _out
    assert "Kept: 3 files" in _out
    assert "Missing: 1 file the registry holds" in _out
    assert _left(store) == sorted(_REGISTERED + _DELETED + _KEPT + [_REPOSITORY])


@pytest.mark.faircli_stores
def test_a_clean_deletes_and_leaves_the_rest(store: typing.Dict):
    assert fdp_stores.clean_stores(yes=True) == 3
    assert _left(store) == sorted(_REGISTERED + _KEPT + [_REPOSITORY])
    # The folder it emptied goes; the stores' own folders stay
    assert not os.path.exists(os.path.join(store["store"], "odd"))
    assert os.path.isdir(os.path.join(store["store"], "inner"))

    assert fdp_stores.clean_stores(yes=True, delete_all=True) == 3
    assert _left(store) == sorted(_REGISTERED + [_REPOSITORY])


@pytest.mark.faircli_stores
def test_with_no_terminal_a_clean_asks_for_yes(store: typing.Dict):
    with pytest.raises(fdp_exc.FAIRCLIException, match="no terminal") as e:
        fdp_stores.clean_stores()
    assert "'--yes'" in e.value.hint and "'--dry-run'" in e.value.hint
    assert len(_left(store)) == len(_REGISTERED + _DELETED + _KEPT) + 1


@pytest.mark.faircli_stores
def test_a_running_registry_is_not_cleaned(
    store: typing.Dict, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.registry.lifecycle._server_here", lambda *args: True)
    with pytest.raises(fdp_exc.RegistryError, match="must be stopped") as e:
        fdp_stores.clean_stores(yes=True)
    assert "'fair registry stop'" in e.value.hint
    assert len(_left(store)) == len(_REGISTERED + _DELETED + _KEPT) + 1


@pytest.mark.faircli_stores
def test_a_store_that_is_the_home_folder_is_not_cleaned(
    store: typing.Dict, mocker: pytest_mock.MockerFixture, capsys: pytest.CaptureFixture
):
    mocker.patch("os.path.expanduser", lambda path: store["store"])
    assert fdp_stores.clean_stores(yes=True) == 1
    _out = capsys.readouterr().out
    assert f"Not cleaned: the data store '{os.path.join(store['store'], '')}'" in _out
    assert "inner/y.csv" not in _left(store)
    assert "odd/notes.txt" in _left(store)


@pytest.mark.faircli_stores
@pytest.mark.skipif(
    platform.system() == "Windows", reason="a symbolic link needs a privilege"
)
def test_a_link_is_deleted_and_not_what_it_points_to(
    store: typing.Dict, tmp_path: pathlib.Path
):
    _outside = tmp_path.joinpath("outside")
    _outside.mkdir()
    _outside.joinpath("kept.txt").write_text("kept")
    os.symlink(_outside, os.path.join(store["store"], "link"))
    fdp_stores.clean_stores(yes=True)
    assert not os.path.lexists(os.path.join(store["store"], "link"))
    assert _outside.joinpath("kept.txt").exists()


@pytest.mark.faircli_stores
def test_registry_clean_command(store: typing.Dict):
    _result = click.testing.CliRunner().invoke(cli, ["registry", "clean", "--dry-run"])
    assert _result.exit_code == 0, _result.output
    assert "Would delete 3 files" in _result.output
    _result = click.testing.CliRunner().invoke(cli, ["registry", "clean"])
    assert _result.exit_code == 1
    assert "'--yes'" in _result.output
