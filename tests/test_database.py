import os
import pathlib
import typing

import pytest

import fair.exceptions as fdp_exc
import fair.registry.database as fdp_db

from .registry_database import FRESH, SCHEMA, make_install


@pytest.mark.faircli_database
def test_a_database_that_cannot_be_read_names_reset(tmp_path: pathlib.Path):
    _install = tmp_path.joinpath("registry")
    _install.mkdir()
    _install.joinpath(fdp_db.DATABASE_FILE).write_bytes(b"not a database" * 100)
    with pytest.raises(fdp_exc.RegistryError, match="cannot be read") as e:
        fdp_db.registry_tables(str(_install))
    assert "'fair registry reset'" in e.value.hint


# The schema with one statement changed
def _changed(old: str, new: str) -> typing.List[str]:
    _schema = [_statement.replace(old, new) for _statement in SCHEMA]
    assert _schema != SCHEMA
    return _schema


@pytest.mark.faircli_database
@pytest.mark.parametrize(
    "schema, same",
    [
        # The same tables written another way
        (SCHEMA, True),
        (
            _changed(
                '"name" varchar(1024) NULL, "identifier" varchar(255) NULL',
                '"identifier" varchar(255) NULL, "name" varchar(1024) NULL',
            ),
            True,
        ),
        (_changed("storage_location_id_9f2e", "storage_location_id_0000"), True),
        (_changed('"name" varchar(255) NOT NULL)', '"name" text NOT NULL)'), True),
        # Other tables
        (
            _changed('"local" bool NOT NULL)', '"local" bool NOT NULL, "x" int NULL)'),
            False,
        ),
        (
            _changed(
                '"name" varchar(1024) NOT NULL UNIQUE', '"name" varchar(1024) NOT NULL'
            ),
            False,
        ),
        (
            _changed(
                '"local" bool NOT NULL)', '"local" bool NOT NULL CHECK ("local" >= 0))'
            ),
            False,
        ),
        (
            _changed('"name" varchar(1024) NULL,', '"name" varchar(1024) NOT NULL,'),
            False,
        ),
    ],
)
def test_tables_compare_by_what_they_store(
    tmp_path: pathlib.Path, schema: typing.List[str], same: bool
):
    _old = make_install(tmp_path.joinpath("old"))
    _new = make_install(tmp_path.joinpath("new"), schema=schema, rows=[])
    assert (fdp_db.registry_tables(_old) == fdp_db.registry_tables(_new)) is same


@pytest.mark.faircli_database
def test_the_local_stores_and_their_registered_files(tmp_path: pathlib.Path):
    _store = f"file://{tmp_path.joinpath('store')}/"
    _rows = FRESH + [
        f"INSERT INTO data_management_storageroot (root, local) VALUES ('{_store}', 1)",
        f"INSERT INTO data_management_storageroot (root, local) VALUES "
        f"('file://{tmp_path.joinpath('remote')}/', 0)",
        "INSERT INTO data_management_storageroot (root, local) VALUES "
        "('https://github.com/', 0)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('PSU/data/1.0.0.csv', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('my-model', 1)",
        "INSERT INTO data_management_storagelocation (path, storage_root_id) "
        "VALUES ('FAIRDataPipeline/FAIR-CLI', 3)",
        "INSERT INTO data_management_object (description, storage_location_id) "
        "VALUES ('Remote code repository.', 2)",
    ]
    _install = make_install(tmp_path.joinpath("registry"), rows=_rows)
    _folder = _store[len("file://") :]
    assert fdp_db.local_stores(_install) == [
        {
            "root": _store,
            "folder": _folder,
            "registered": {"PSU/data/1.0.0.csv"},
            "repositories": {os.path.normpath(os.path.join(_folder, "my-model"))},
        }
    ]
