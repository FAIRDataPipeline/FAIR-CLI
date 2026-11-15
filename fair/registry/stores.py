#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Data Stores
===========

The files in this machine's data stores, those the local registry holds and
those it does not, and their deletion. A data store is a folder on this
machine that the registry has as a storage root, and a file in one is
registered where a storage location under that root has its path. A code
repository's folder in a store is left whole, and a store inside another is
walked as one of its own.

Contents
========

Functions
---------

    find_files - the files in data stores that no storage location names
    held_files - the files in data stores that storage locations name
    delete_files - delete files, and the folders they leave empty
    erase_unknown_data - erase the files the local registry does not hold

"""

__date__ = "2026-11-10"

import os
import re
import stat
import sys
import typing

import click

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.registry.database as fdp_db
import fair.registry.lifecycle as fdp_life

# The name an output has until it is registered: 'dat-' and a random part
UNFINISHED_PREFIX = "dat-"

# The folder of a store that holds the jobs 'fair run' prepares
JOBS_FOLDER = "jobs"

# The name an output is given as it is registered: the SHA-1 of its contents,
# with its extension
_HASH_NAME = re.compile(r"[0-9a-f]{40}(\..*)?")


# A folder as stores are compared: absolute and normalised, ending in a
# separator
def _folder_key(folder: str) -> str:
    return os.path.join(os.path.normpath(os.path.abspath(folder)), "")


# Whether a file could be one a run in progress is still writing: an output
# not yet registered, under the name it is written to or the one it is given
# as it is registered, or a file of a job
def _could_be_running(relative: str) -> bool:
    _name = relative.rsplit("/", 1)[-1]
    return (
        _name.startswith(UNFINISHED_PREFIX)
        or bool(_HASH_NAME.fullmatch(_name))
        or relative.startswith(f"{JOBS_FOLDER}/")
    )


# Whether a store's folder is the user's home or a folder above it, which no
# clean walks: a storage root registered there by mistake would otherwise cost
# the user every file they have
def _too_wide(folder_key: str) -> bool:
    _home = os.path.normcase(_folder_key(os.path.expanduser("~")))
    return _home.startswith(os.path.normcase(folder_key))


# Every file under a folder, a symbolic link being one whatever it points to,
# but for the folders in 'pruned', as _folder_key gives them
def _files(folder: str, pruned: typing.Set[str]) -> typing.Iterator[str]:
    for _root, _dirs, _names in os.walk(folder):
        for _dir in list(_dirs):
            _path = os.path.join(_root, _dir)
            if os.path.islink(_path):
                _dirs.remove(_dir)
                yield _path
            elif _folder_key(_path) in pruned:
                _dirs.remove(_dir)
        for _name in _names:
            yield os.path.join(_root, _name)


def find_files(
    stores: typing.List[typing.Dict[str, typing.Any]], delete_all: bool = False
) -> typing.Dict[str, typing.List[str]]:
    """Return the files in data stores that no storage location names

    Parameters
    ----------
    stores : typing.List[typing.Dict[str, typing.Any]]
        the stores, as 'fair.registry.database.local_stores' gives them
    delete_all : bool, optional
        whether the files a run in progress could still be writing are to be
        deleted too - outputs named 'dat-...' or by their hash, and files under
        a store's 'jobs/' - by default False

    Returns
    -------
    typing.Dict[str, typing.List[str]]
        the stores' folders ("stores"), those not found ("absent"), those too
        wide to walk ("too_wide"), the files to delete ("delete"), those kept
        as a run's ("kept"), and the registered files not found ("missing")
    """
    _registered: typing.Dict[str, typing.Set[str]] = {}
    _pruned: typing.Dict[str, typing.Set[str]] = {}
    for _store in stores:
        _key = _folder_key(_store["folder"])
        _registered.setdefault(_key, set()).update(_store["registered"])
        _pruned.setdefault(_key, set()).update(
            _folder_key(_repository) for _repository in _store["repositories"]
        )
    _found = {
        _part: []
        for _part in ("stores", "absent", "too_wide", "delete", "kept", "missing")
    }
    for _key in sorted(_registered):
        if not os.path.isdir(_key):
            _found["absent"].append(_key)
            continue
        if _too_wide(_key):
            _found["too_wide"].append(_key)
            continue
        _found["stores"].append(_key)
        _inner = {_other for _other in _registered if _other != _key}
        for _path in _files(_key, _inner | _pruned[_key]):
            _relative = os.path.relpath(_path, _key).replace(os.sep, "/")
            if _relative in _registered[_key]:
                continue
            if not delete_all and _could_be_running(_relative):
                _found["kept"].append(_path)
            else:
                _found["delete"].append(_path)
        _found["missing"].extend(
            os.path.join(_key, _relative)
            for _relative in sorted(_registered[_key])
            if not os.path.lexists(os.path.join(_key, _relative))
        )
    return _found


def held_files(
    stores: typing.List[typing.Dict[str, typing.Any]],
) -> typing.Dict[str, typing.List[str]]:
    """Return the files in data stores that storage locations name

    A code repository's folder is not among them, nor is anything a location's
    path would put outside its store.

    Parameters
    ----------
    stores : typing.List[typing.Dict[str, typing.Any]]
        the stores, as 'fair.registry.database.local_stores' gives them

    Returns
    -------
    typing.Dict[str, typing.List[str]]
        the stores' folders ("stores"), those too wide to touch ("too_wide"),
        and the files there that a location names ("files")
    """
    _found = {"stores": [], "too_wide": [], "files": []}
    _files = set()
    for _store in stores:
        _key = _folder_key(_store["folder"])
        if not os.path.isdir(_key):
            continue
        if _too_wide(_key):
            if _key not in _found["too_wide"]:
                _found["too_wide"].append(_key)
            continue
        if _key not in _found["stores"]:
            _found["stores"].append(_key)
        for _relative in _store["registered"]:
            _path = os.path.normpath(os.path.join(_key, *_relative.split("/")))
            if _path.startswith(_key) and (
                os.path.isfile(_path) or os.path.islink(_path)
            ):
                _files.add(_path)
    _found["files"] = sorted(_files)
    return _found


# Remove a file, making it writable first where it is read-only, as Windows
# requires
def _remove_file(path: str) -> None:
    try:
        os.remove(path)
    except PermissionError:
        os.chmod(path, stat.S_IWRITE)
        os.remove(path)


def delete_files(
    paths: typing.List[str], stores: typing.List[str]
) -> typing.List[typing.Tuple[str, str]]:
    """Delete files, and the folders they leave empty within their stores

    A file that cannot be deleted is passed over, and the rest deleted.

    Parameters
    ----------
    paths : typing.List[str]
        the files to delete
    stores : typing.List[str]
        the folders of the stores they are in, which are never removed

    Returns
    -------
    typing.List[typing.Tuple[str, str]]
        each file that could not be deleted, with why
    """
    _stores = {_folder_key(_store) for _store in stores}
    _failed = []
    _emptied = set()
    for _path in paths:
        try:
            _remove_file(_path)
        except OSError as e:
            _failed.append((_path, e.strerror or str(e)))
            continue
        _emptied.add(os.path.dirname(_path))
    for _folder in sorted(_emptied, key=len, reverse=True):
        while _folder_key(_folder) not in _stores and any(
            _folder_key(_folder).startswith(_store) for _store in _stores
        ):
            try:
                os.rmdir(_folder)
            except OSError:
                break
            _folder = os.path.dirname(_folder)
    return _failed


# Say what a search of the stores found, and list each file where asked
def _report(found: typing.Dict[str, typing.List[str]], listed: bool) -> None:
    for _store in found["too_wide"]:
        click.echo(
            f"Not erased: the data store '{_store}' is your home folder or one "
            "above it"
        )
    if listed:
        for _path in sorted(found["delete"]):
            click.echo(f"To erase: {_path}")
    if found["kept"]:
        click.echo(
            f"Kept: {_count(len(found['kept']))} that a run in progress could "
            "still be writing - outputs named 'dat-...' or by their hash, and "
            "files under 'jobs/'. 'fair registry erase-unknown-data "
            "--delete-all' erases them too"
        )
        if listed:
            for _path in sorted(found["kept"]):
                click.echo(f"Kept: {_path}")
    if found["missing"]:
        click.echo(
            f"Missing: {_count(len(found['missing']))} the registry holds "
            "are not in their stores"
            + (
                ""
                if listed
                else "; 'fair registry erase-unknown-data --dry-run' lists them"
            )
        )
        if listed:
            for _path in found["missing"]:
                click.echo(f"Missing: {_path}")


# A number of files in words
def _count(number: int) -> str:
    return f"{number} file{'' if number == 1 else 's'}"


# Whether the user can be asked: standard input is a terminal
def _has_terminal() -> bool:
    return sys.stdin.isatty()


# Whether a path the user typed names a store's folder, as _folder_key gives
# it: the same absolute path, with or without its final separator, and on
# Windows in any case. A relative path or a '~' names none
def _names_store(typed: str, folder_key: str) -> bool:
    _typed = typed.strip()
    if not _typed or not os.path.isabs(_typed):
        return False
    return os.path.normcase(
        os.path.join(os.path.normpath(_typed), "")
    ) == os.path.normcase(folder_key)


# The files to erase, by the store each is in: the innermost store holding it,
# since a store inside another is walked as one of its own
def _by_store(
    paths: typing.List[str], stores: typing.List[str]
) -> typing.Dict[str, typing.List[str]]:
    _files: typing.Dict[str, typing.List[str]] = {}
    for _path in paths:
        _store = max((_s for _s in stores if _path.startswith(_s)), key=len)
        _files.setdefault(_store, []).append(_path)
    return _files


def erase_unknown_data(
    registry_dir: str = None,
    delete_all: bool = False,
    dry_run: bool = False,
) -> int:
    """Erase the files in the local registry's data stores that it does not hold

    The registry must be stopped, so that no run is using it, and it is held
    meanwhile, so that none starts. Each store is named, with a warning that
    another registry using it may hold some of the files, and its files are
    erased only once the user types the store's path.

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    delete_all : bool, optional
        erase too the files a run in progress could still be writing, by
        default False
    dry_run : bool, optional
        list what would be erased, erasing nothing, by default False

    Returns
    -------
    int
        how many files were erased
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    if not os.path.isdir(registry_dir):
        raise fdp_exc.RegistryError(
            f"No local registry is installed in '{registry_dir}'",
            hint="Install one with 'fair registry install'",
        )
    with fdp_life.held(registry_dir, "fair registry erase-unknown-data"):
        fdp_life.require_stopped(registry_dir, "its data stores are erased")
        _found = find_files(fdp_db.local_stores(registry_dir), delete_all)
        _report(_found, listed=dry_run)
        _number = len(_found["delete"])
        if not _number:
            click.echo("Nothing to erase")
            return 0
        _files = _by_store(_found["delete"], _found["stores"])
        _what = (
            f"{_count(_number)} that the registry does not hold, from "
            f"{len(_files)} data store{'' if len(_files) == 1 else 's'}"
        )
        if dry_run:
            click.echo(f"Would erase {_what}")
            return 0
        if not _has_terminal():
            raise fdp_exc.FAIRCLIException(
                f"There is no terminal to ask whether to erase {_what}",
                hint="Run 'fair registry erase-unknown-data' in a terminal, or "
                "give '--dry-run' to list what it would erase",
            )
        _erased = 0
        for _store, _paths in _files.items():
            click.echo(
                f"The data store '{_store}' holds {_count(len(_paths))} that "
                "this registry does not hold. Warning: another registry using "
                "this store may hold them"
            )
            _typed = click.prompt(
                "To erase them, type the store's path; anything else leaves "
                "them",
                default="",
                show_default=False,
            )
            if not _names_store(_typed, _store):
                click.echo(f"Left '{_store}' as it is")
                continue
            _failed = delete_files(_paths, _found["stores"])
            for _path, _why in _failed:
                click.echo(
                    f"Warning: '{_path}' could not be erased: {_why}", err=True
                )
            _erased += len(_paths) - len(_failed)
        click.echo(f"Erased {_count(_erased)}")
        return _erased
