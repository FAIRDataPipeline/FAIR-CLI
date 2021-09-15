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
FAIR_CLI_CONFIG = "cli-config.yaml"
FAIR_FOLDER = ".fair"
JOBS_DIR = "jobs"


def registry_home() -> str:
    _glob_conf = yaml.safe_load(open(global_fdpconfig()))
    if 'registries' not in _glob_conf:
        raise fdp_exc.CLIConfigurationError(
            f"Expected key 'registries' in global CLI configuration"
        )
    if 'local' not in _glob_conf['registries']:
        raise fdp_exc.CLIConfigurationError(
            f"Expected 'local' registry in global CLI configuration registries"
        )
    if 'directory' not in _glob_conf['registries']['local']:
        raise fdp_exc.CLIConfigurationError(
            f"Expected directory of local registry in global CLI configuration"
        )
    return _glob_conf['registries']['local']['directory']


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
    _current_dir = os.path.abspath(start_directory)

    while _current_dir:
        # If the home directory has been reached then abort as upper file system
        # is outside user area, also we do not want to return global FAIR folder
        if str(_current_dir) == str(pathlib.Path().home()):
            return ""

        if os.path.exists(os.path.join(_current_dir, FAIR_FOLDER)):
            return _current_dir
        _current_dir, _directory = os.path.split(_current_dir)

        # If there is no directory component this means the top of the file
        # system has been reached
        if not _directory:
            return ""            


def staging_cache(user_loc: str) -> str:
    """Location of staging cache for the given repository"""
    return os.path.abspath(
        os.path.join(find_fair_root(user_loc), FAIR_FOLDER, "staging")
    )


def default_data_dir(location: str = 'local') -> str:
    """Location of the default data store"""
    if not os.path.exists(global_fdpconfig()):
        raise fdp_exc.InternalError(
            f"Failed to read CLI global config file '{global_fdpconfig()}'"
        )
    _glob_conf = yaml.safe_load(open(global_fdpconfig()))
    if 'data_store' in _glob_conf['registries'][location]:
        return _glob_conf['registries'][location]['data_store']
    if location == 'local':
        return os.path.join(USER_FAIR_DIR, f"data{os.path.sep}")
    else:
        raise fdp_exc.UserConfigError('Cannot guess remote data store location')


def local_fdpconfig(user_loc: str = os.getcwd()) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, FAIR_CLI_CONFIG)


def local_user_config(user_loc: str = os.getcwd()) -> str:
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
    try:
        _repository = git.Repo(
            start_directory,
            search_parent_directories=True
        )
    except git.InvalidGitRepositoryError:
        raise fdp_exc.UserConfigError(
            f"Failed to retrieve git repository for current configuration"
        )
    return _repository.git.rev_parse("--show-toplevel").strip()
