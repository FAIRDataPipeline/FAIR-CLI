#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Data Stores
===========

The files in this machine's data stores that the local registry does not
hold, and their deletion. A data store is a folder on this machine that the
registry has as a storage root, and a file in one is registered where a
storage location under that root has its path. A code repository's folder in
a store is left whole, and a store inside another is walked as one of its
own.

Contents
========

Functions
---------

    find_files - the files in data stores that no storage location names
    delete_files - delete files, and the folders they leave empty
    clean_stores - delete the files the local registry does not hold

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
            f"Not cleaned: the data store '{_store}' is your home folder or one "
            "above it"
        )
    _paths = list(found["delete"])
    if listed:
        _paths.sort()
        for _path in _paths:
            click.echo(f"To delete: {_path}")
    if found["kept"]:
        click.echo(
            f"Kept: {_count(len(found['kept']))} that a run in progress could "
            "still be writing - outputs named 'dat-...' or by their hash, and "
            "files under 'jobs/'. 'fair registry clean --delete-all' deletes "
            "them too"
        )
        if listed:
            for _path in sorted(found["kept"]):
                click.echo(f"Kept: {_path}")
    if found["missing"]:
        click.echo(
            f"Missing: {_count(len(found['missing']))} the registry holds "
            "are not in their stores"
            + ("" if listed else "; 'fair registry clean --dry-run' lists them")
        )
        if listed:
            for _path in found["missing"]:
                click.echo(f"Missing: {_path}")


# A number of files in words
def _count(number: int) -> str:
    return f"{number} file{'' if number == 1 else 's'}"


def clean_stores(
    registry_dir: str = None,
    delete_all: bool = False,
    dry_run: bool = False,
    yes: bool = False,
) -> int:
    """Delete the files in the local registry's data stores that it does not hold

    The registry must be stopped, so that no run is using it, and it is held
    meanwhile, so that none starts. The user is asked first, unless 'yes'.

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    delete_all : bool, optional
        delete too the files a run in progress could still be writing, by
        default False
    dry_run : bool, optional
        list what would be deleted, deleting nothing, by default False
    yes : bool, optional
        delete without asking, by default False

    Returns
    -------
    int
        how many files were deleted
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    if not os.path.isdir(registry_dir):
        raise fdp_exc.RegistryError(
            f"No local registry is installed in '{registry_dir}'",
            hint="Install one with 'fair registry install'",
        )
    with fdp_life.held(registry_dir, "fair registry clean"):
        fdp_life.require_stopped(registry_dir, "its data stores are cleaned")
        _found = find_files(fdp_db.local_stores(registry_dir), delete_all)
        _report(_found, listed=dry_run)
        _number = len(_found["delete"])
        if not _number:
            click.echo("Nothing to delete")
            return 0
        _what = (
            f"{_count(_number)} that the registry does not hold, from "
            f"{len(_found['stores'])} data store"
            f"{'' if len(_found['stores']) == 1 else 's'}"
        )
        if dry_run:
            click.echo(f"Would delete {_what}")
            return 0
        if not yes:
            if not sys.stdin.isatty():
                raise fdp_exc.FAIRCLIException(
                    f"There is no terminal to ask whether to delete {_what}",
                    hint="Give '--yes' to delete them, or '--dry-run' to list them",
                )
            if not click.confirm(f"Delete {_what}?", default=False):
                return 0
        _failed = delete_files(_found["delete"], _found["stores"])
        for _path, _why in _failed:
            click.echo(f"Warning: '{_path}' could not be deleted: {_why}", err=True)
        _deleted = _number - len(_failed)
        click.echo(f"Deleted {_count(_deleted)}")
        return _deleted
