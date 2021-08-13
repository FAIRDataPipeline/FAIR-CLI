#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Common Paths
============

Functions and constant strings related to the location of directories
and files for executing a CLI session.


Contents
========

Members
-------
    USER_FAIR_DIR   - user FAIR directory
    REGISTRY_HOME   - location of local registry
    FAIR_CLI_CONFIG - name of the FAIR-CLI configuration file
    FAIR_FOLDER     - name for FAIR local repository directory

Functions
-------

    find_fair_root      - returns the closest '.fair' directory in the upper hierarchy
    find_git_root       - returns the closest '.git' directory
    staging_cache       - returns the current repository staging cache directory
    default_data_dir    - returns the default data store
    local_fdpconfig     - returns path of FAIR-CLI local repository config
    local_user_config   - returns the path of the user config in the given folder
    default_jobs_dir    - returns the default jobs folder
    global_config_dir   - returns the FAIR-CLI global config directory
    global_fdpconfig    - returns path of FAIR-CLI global config
    session_cache_dir   - returns location of session cache folder

"""
__date__ = "2021-06-28"

import os
import pathlib

import yaml
import git

import fair.exceptions as fdp_exc

USER_FAIR_DIR = os.path.join(pathlib.Path.home(), ".fair")
REGISTRY_HOME = os.path.join(USER_FAIR_DIR, "registry")
FAIR_CLI_CONFIG = "cli-config.yaml"
FAIR_FOLDER = ".fair"
JOBS_DIR = "jobs"


def find_fair_root(start_directory: str = os.getcwd()) -> str:
    """Locate the .fair folder within the current hierarchy

    Parameters
    ----------

    start_directory : str, optional
        starting point for local FAIR folder search

    Returns
    -------
    str
        absolute path of the .fair folder
    """
    _current_dir = start_directory

    # Keep upward searching until you find '.fair', stop at the level of
    # the user's home directory.
    _top_level = os.path.abspath(".").split(os.path.sep)[0] + os.path.sep
    while _current_dir != pathlib.Path.home():
        # If the current directory is '/' or 'C:\' it means the given path
        # was not in the user area, and no repository was found. This is not
        # allowed except in test cases where a temporary directory in '/tmp'
        # is used.
        if _current_dir == _top_level:
            raise fdp_exc.FDPRepositoryError(
                "The specified path must be in a user home area"
            )
        _fair_dir = os.path.join(_current_dir, FAIR_FOLDER)
        if os.path.exists(_fair_dir):
            return os.path.dirname(_fair_dir)
        _current_dir = pathlib.Path(_current_dir).parent
    return ""


def staging_cache(user_loc: str) -> str:
    """Location of staging cache for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, "staging")


def default_data_dir(location: str = 'local') -> str:
    """Location of the default data store"""
    _glob_conf = yaml.safe_load(global_fdpconfig())
    if 'data_store' in _glob_conf:
        return _glob_conf['data_store'][location]
    return os.path.join(USER_FAIR_DIR, "data")


def local_fdpconfig(user_loc: str) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, FAIR_CLI_CONFIG)


def local_user_config(user_loc: str) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), "config.yaml")


def default_jobs_dir() -> str:
    """Default location to place job outputs"""
    return os.path.join(default_data_dir(), JOBS_DIR)


def global_config_dir() -> str:
    """Directory of global CLI configuration"""
    return os.path.join(USER_FAIR_DIR, "cli")


def session_cache_dir() -> str:
    """Location of run files used to determine if server is being used"""
    return os.path.join(global_config_dir(), "sessions")


def global_fdpconfig() -> str:
    """Location of global CLI configuration"""
    return os.path.join(global_config_dir(), FAIR_CLI_CONFIG)


def find_git_root(start_directory: str = os.getcwd()) -> str:
    """Locate the .git folder within the current hierarchy

    Parameters
    ----------

    start_directory : str, optional
        starting point for local git folder search

    Returns
    -------
    str
        absolute path of the .git folder
    """
    _repository = git.Repo(start_directory, search_parent_directories=True)
    return _repository.git.rev_parse("--show-toplevel").strip()
