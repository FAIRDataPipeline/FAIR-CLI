#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Database
=================

What the local registry's database holds, read from its file: the structure
of the registry's own tables, and the data stores it knows with the files
registered in each. Reading the file needs no server, so each can be asked of
a registry that is not running.

Contents
========

Functions
---------

    database_file - the file of an install's database
    registry_tables - the structure of the registry's own tables
    local_stores - the data stores the registry knows, with their registered files

"""

__date__ = "2026-11-10"

import contextlib
import os
import pathlib
import platform
import re
import sqlite3
import typing

import fair.exceptions as fdp_exc

DATABASE_FILE = "db.sqlite3"

# The registry's own applications' tables; Django's and its libraries' are the
# same in every install of one Django
REGISTRY_TABLE_PREFIXES = ("custom_user_", "data_management_")

# The description of an object that is a code repository at a commit: its
# location is a folder, not a file
REPOSITORY_DESCRIPTION = "Remote code repository."


def database_file(registry_dir: str) -> str:
    """Return the file of an install's database

    Parameters
    ----------
    registry_dir : str
        directory of the registry install

    Returns
    -------
    str
        path of the install's SQLite database
    """
    return os.path.join(registry_dir, DATABASE_FILE)


# Read an install's database for the block, never writing it. A database that
# cannot be read is refused, naming the command that builds it again
@contextlib.contextmanager
def _reading(registry_dir: str) -> typing.Iterator[sqlite3.Connection]:
    _file = os.path.abspath(database_file(registry_dir))
    try:
        _db = sqlite3.connect(f"{pathlib.Path(_file).as_uri()}?mode=ro", uri=True)
    except sqlite3.Error as e:
        raise _unreadable(registry_dir, e) from e
    try:
        yield _db
    except sqlite3.Error as e:
        raise _unreadable(registry_dir, e) from e
    finally:
        _db.close()


# The refusal of a database that cannot be read
def _unreadable(registry_dir: str, error: Exception) -> fdp_exc.RegistryError:
    return fdp_exc.RegistryError(
        f"The database of the local registry installed in '{registry_dir}' "
        f"cannot be read: {error}",
        hint="Empty it with 'fair registry reset', which builds it again",
    )


# The registry's own tables in a database
def _registry_table_names(db: sqlite3.Connection) -> typing.List[str]:
    return [
        _name
        for (_name,) in db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )
        if _name.startswith(REGISTRY_TABLE_PREFIXES)
    ]


# The CHECK constraints of a table's definition, each with its whitespace
# collapsed. The table's other parts are compared through SQLite's own
# account of them, which does not give these
def _checks(sql: str) -> typing.List[str]:
    _found = []
    for _match in re.finditer(r"\bCHECK\s*\(", sql or "", re.IGNORECASE):
        _depth, _start = 0, _match.end() - 1
        for _end in range(_start, len(sql)):
            _depth += {"(": 1, ")": -1}.get(sql[_end], 0)
            if not _depth:
                _found.append(" ".join(sql[_start : _end + 1].split()))
                break
    return sorted(_found)


# One table's structure: its columns, indexes, foreign keys and checks, as
# SQLite gives them, leaving out the names Django derives and the order of
# columns, which change nothing that is stored
def _table_structure(db: sqlite3.Connection, table: str) -> typing.Dict:
    _columns = sorted(
        tuple(_row[1:]) for _row in db.execute(f'PRAGMA table_info("{table}")')
    )
    _indexes = sorted(
        (
            _unique,
            _origin,
            _partial,
            tuple(
                _column[2] for _column in db.execute(f'PRAGMA index_info("{_name}")')
            ),
        )
        for _, _name, _unique, _origin, _partial in db.execute(
            f'PRAGMA index_list("{table}")'
        )
    )
    _foreign_keys = sorted(
        tuple(_row[2:7]) for _row in db.execute(f'PRAGMA foreign_key_list("{table}")')
    )
    (_sql,) = db.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return {
        "columns": _columns,
        "indexes": _indexes,
        "foreign_keys": _foreign_keys,
        "checks": _checks(_sql),
    }


def registry_tables(registry_dir: str) -> typing.Dict[str, typing.Dict]:
    """Return the structure of the registry's own tables in an install's database

    Two databases whose registry tables have the same structure hold the same
    records in the same way, so either install's code can use either's.

    Parameters
    ----------
    registry_dir : str
        directory of the registry install

    Returns
    -------
    typing.Dict[str, typing.Dict]
        for each table of the registry's own, its columns, indexes, foreign
        keys and checks
    """
    with _reading(registry_dir) as db:
        return {
            _table: _table_structure(db, _table) for _table in _registry_table_names(db)
        }


# A path within a store as a walk of its folder is compared with it: a
# location registered on Windows may give it with either separator
def _store_path(path: str) -> str:
    return path.replace("\\", "/") if platform.system() == "Windows" else path


def local_stores(registry_dir: str) -> typing.List[typing.Dict[str, typing.Any]]:
    """Return the data stores an install's registry knows, with their registered files

    A data store is a storage root on this machine: a 'file://' root marked
    local. A code repository's location under one is a folder, not a file.

    Parameters
    ----------
    registry_dir : str
        directory of the registry install

    Returns
    -------
    typing.List[typing.Dict[str, typing.Any]]
        for each store, its root ("root"), its folder ("folder"), the paths
        registered in it, relative to the folder with '/' between their
        parts ("registered"), and the folders in it that are repositories
        ("repositories")
    """
    if not os.path.exists(database_file(registry_dir)):
        return []
    with _reading(registry_dir) as db:
        _roots = db.execute(
            "SELECT id, root FROM data_management_storageroot "
            "WHERE local AND root LIKE 'file://%' ORDER BY root"
        ).fetchall()
        _repositories = {
            _location
            for (_location,) in db.execute(
                "SELECT storage_location_id FROM data_management_object "
                "WHERE description = ? AND storage_location_id IS NOT NULL",
                (REPOSITORY_DESCRIPTION,),
            )
        }
        _stores = []
        for _root_id, _root in _roots:
            _folder = _root[len("file://") :]
            _store = {
                "root": _root,
                "folder": _folder,
                "registered": set(),
                "repositories": set(),
            }
            for _location, _path in db.execute(
                "SELECT id, path FROM data_management_storagelocation "
                "WHERE storage_root_id = ?",
                (_root_id,),
            ):
                if _location in _repositories:
                    _store["repositories"].add(
                        os.path.normpath(os.path.join(_folder, _path))
                    )
                else:
                    _store["registered"].add(_store_path(_path))
            _stores.append(_store)
    return _stores
