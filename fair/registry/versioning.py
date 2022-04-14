#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Versioning
==========

Methods for handling semantic versioning and parsing version formats

Contents
========

Functions
---------
    parse_incrementer - parse version increment format

"""


import contextlib

__date__ = "2021-08-05"

import re
import typing

import semver

import fair.exceptions as fdp_exc

BUMP_FUNCS = {
    "LATEST": None,
    "MINOR": "bump_minor",
    "MAJOR": "bump_major",
    "PATCH": "bump_patch",
    "BUILD": "bump_build",
    "PRERELEASE": "bump_prerelease",
}

DEFAULT_WRITE_VERSION = "PATCH"
DEFAULT_READ_VERSION = "LATEST"


def parse_incrementer(incrementer: str) -> str:
    """Convert an incrementer string in a config to the relevant bump function

    Parameters
    ----------
        incrementer : str
            config.yaml variable to describe how version increases are handled

    Returns
    -------
        str
            relevant member method of VersionInfo for 'bumping' the semantic version

    """
    # Sanity check to confirm all methods are still present in semver module
    for func in BUMP_FUNCS.values():
        if func and func not in dir(semver.VersionInfo):
            raise fdp_exc.InternalError(
                f"Unrecognised 'semver.VersionInfo' method '{func}'"
            )

    for component in BUMP_FUNCS:
        try:
            if re.findall(r"\$\{\{\s*" + component + r"\s*\}\}", incrementer):
                return BUMP_FUNCS[component]
        except TypeError as e:
            raise fdp_exc.InternalError(
                f"Failed to parse incrementer '{incrementer}' expected string"
            ) from e

    raise fdp_exc.UserConfigError(
        f"Unrecognised version incrementer variable '{incrementer}'"
    )


def undo_incrementer(incrementer: str) -> str:
    """Convert an incrementer string to just return the latest value

    Parameters
    ----------
        incrementer : str
            config.yaml variable to describe how version increases are handled for register

    Returns
    -------
        str
            correct value for version string for read

    """
    for component in BUMP_FUNCS:
        if re.findall(r"\$\{\{\s*" + component + r"\s*\}\}", incrementer):
            return re.sub(
                r"\$\{\{\s*" + component + r"\s*\}\}",
                "${{ LATEST }}",
                incrementer,
            )

    return incrementer


def get_latest_version(results_list: typing.List = None) -> semver.VersionInfo:
    if not results_list:
        return semver.VersionInfo.parse("0.0.0")

    _versions = [
        semver.VersionInfo.parse(i["version"])
        for i in results_list
        if "version" in i
    ]

    if not _versions:
        return semver.VersionInfo.parse("0.0.0")

    return max(_versions)


def get_correct_version(
    version: str, results_list: typing.List = None, free_write: bool = True
) -> semver.VersionInfo:

    # Version is already specified
    if isinstance(version, semver.VersionInfo):
        return version

    with contextlib.suppress(ValueError):
        return semver.VersionInfo.parse(version)
    _zero = semver.VersionInfo.parse("0.0.0")

    if results_list:
        _versions = [
            semver.VersionInfo.parse(i["version"])
            for i in results_list
            if "version" in i
        ]
    else:
        _versions = []

    try:
        _bump_func = parse_incrementer(version)
        if free_write:
            _versions.append(_zero)

        if not _versions:
            raise fdp_exc.InternalError(
                f"Version parsing failed for version={version}, free_write={free_write}"
            )
        _max_ver = max(_versions)

        _new_version = (
            getattr(_max_ver, _bump_func)() if _bump_func else _max_ver
        )
    except fdp_exc.UserConfigError:  # Not a command, try an exact version
        _new_version = semver.VersionInfo.parse(version)

    if _new_version in _versions and free_write:
        raise fdp_exc.UserConfigError(
            f"Trying to create existing version: {_new_version}"
        )
    elif _new_version not in _versions and not free_write:
        raise fdp_exc.UserConfigError(
            f"Trying to read non-existing version: {_new_version}"
        )
    elif _new_version == _zero:
        raise fdp_exc.UserConfigError(f"Trying to work with version {_zero}")

    return _new_version


def default_bump(version: semver.VersionInfo) -> semver.VersionInfo:
    """Perform default version bump

    For FAIR-CLI the default version increment is patch

    Parameters
    ----------
        version: semver.VersionInfo

    Returns
    -------
        new version
    """
    return getattr(version, BUMP_FUNCS[DEFAULT_WRITE_VERSION])()
