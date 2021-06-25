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
    REGISTRY_HOME   - location of local registry
    FAIR_CLI_CONFIG - name of the FAIR-CLI configuration file
    FAIR_FOLDER     - name for FAIR local repository directory

Functions
-------

    find_fair_root      - returns the closest '.faircli' directory in the upper hierarchy
    staging_cache       - returns the current repository staging cache directory
    default_data_dir    - returns the default data store
    local_fdpconfig     - returns path of FAIR-CLI local repository config
    local_user_config   - returns the path of the user config in the given folder
    default_coderun_dir - returns the default code run folder
    global_config_dir   - returns the FAIR-CLI global config directory
    global_fdpconfig    - returns path of FAIR-CLI global config
    session_cache_dir   - returns location of session cache folder

"""
__date__ = "2021-06-24"

import os
import pathlib


REGISTRY_HOME = os.path.join(pathlib.Path.home(), ".scrc")
FAIR_CLI_CONFIG = "cli-config.yaml"
FAIR_FOLDER = ".faircli"
CODERUN_DIR = "coderun"


def find_fair_root(start_directory: str = os.getcwd()) -> str:
    """Locate the .faircli folder within the current hierarchy

    Parameters
    ----------

    start_directory : str, optional
        starting point for local FAIR folder search

    Returns
    -------
    str
        absolute path of the .faircli folder
    """
    _current_dir = start_directory

    # Keep upward searching until you find '.faircli', stop at the level of
    # the user's home directory
    while _current_dir != pathlib.Path.home():
        _fair_dir = os.path.join(_current_dir, FAIR_FOLDER)
        if os.path.exists(_fair_dir):
            return os.path.dirname(_fair_dir)
        _current_dir = pathlib.Path(_current_dir).parent
    return ""


def staging_cache(user_loc: str) -> str:
    """Location of staging cache for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, "staging")


def default_data_dir() -> str:
    """Location of the default data store"""
    return os.path.join(REGISTRY_HOME, "data")


def local_fdpconfig(user_loc: str) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, FAIR_CLI_CONFIG)


def local_user_config(user_loc: str) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), "config.yaml")


def default_coderun_dir() -> str:
    return os.path.join(default_data_dir(), CODERUN_DIR)


def global_config_dir() -> str:
    return os.path.join(REGISTRY_HOME, "cli")


def session_cache_dir() -> str:
    return os.path.join(global_config_dir(), "sessions")


def global_fdpconfig() -> str:
    return os.path.join(global_config_dir(), FAIR_CLI_CONFIG)
