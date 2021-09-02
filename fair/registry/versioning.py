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

__date__ = "2021-08-05"

import re
import typing

import semver

import fair.exceptions as fdp_exc


BUMP_FUNCS = {
    "MINOR": "bump_minor",
    "MAJOR": "bump_major",
    "PATCH": "bump_patch",
    "BUILD": "bump_build",
    "PRERELEASE": "bump_prerelease"
}

DEFAULT_INCREMENT = "PATCH"


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
        if func not in dir(semver.VersionInfo):
            raise fdp_exc.InternalError(f"Unrecognised 'semver.VersionInfo' method '{func}'")

    for component in BUMP_FUNCS:
        if re.findall(r'\$\{\{\s*'+component+r'\s*\}\}', incrementer):
            return BUMP_FUNCS[component]
    
    raise fdp_exc.UserConfigError(
        f"Unrecognised version incrementer variable '{incrementer}'"
    )


def get_latest_version(results_list: typing.List = None) -> semver.VersionInfo:
    if not results_list:
        return semver.VersionInfo.parse("0.0.0")

    _versions = [
        semver.VersionInfo.parse(i['version']) for i in results_list
        if 'version' in i
    ]

    if not _versions:
        return semver.VersionInfo.parse("0.0.0")

    return max(_versions)


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
    return getattr(version, BUMP_FUNCS[DEFAULT_INCREMENT])()
