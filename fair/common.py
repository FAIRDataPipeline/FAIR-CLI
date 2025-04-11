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
    FAIR_CLI_CONFIG - name of the FAIR-CLI configuration file
    FAIR_FOLDER     - name for FAIR local repository directory

Functions
-------
    registry_home       - returns the location of the local data registry
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

import enum
import os
import pathlib
import logging
import stat

import git
import yaml

import fair.exceptions as fdp_exc

_logger = logging.getLogger("FAIRDataPipeline.Common")

USER_FAIR_DIR = os.path.join(pathlib.Path.home(), ".fair")
FAIR_CLI_CONFIG = "cli-config.yaml"
USER_CONFIG_FILE = "config.yaml"
FAIR_FOLDER = ".fair"
JOBS_DIR = "jobs"

FAIR_REGISTRY_REPO = "https://github.com/FAIRDataPipeline/data-registry.git"

DEFAULT_REGISTRY_DOMAIN = "https://data.fairdatapipeline.org/"
REGISTRY_INSTALL_URL = "https://data.fairdatapipeline.org/static/localregistry.sh"

DEFAULT_REGISTRY_LOCATION = os.path.join(pathlib.Path().home(), FAIR_FOLDER, "registry")

DEFAULT_LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api/"


class CMD_MODE(enum.Enum):
    RUN = 1
    PULL = 2
    PUSH = 3
    PASS = 4


def registry_home() -> str:
    if not os.path.exists(global_fdpconfig()):
        if "FAIR_REGISTRY_DIR" in os.environ:
            return os.environ["FAIR_REGISTRY_DIR"]
        else:
            return DEFAULT_REGISTRY_LOCATION
    _glob_conf = yaml.safe_load(open(global_fdpconfig(), encoding="utf-8"))
    if not _glob_conf:
        return DEFAULT_REGISTRY_LOCATION
    if "registries" not in _glob_conf:
        raise fdp_exc.CLIConfigurationError(
            "Expected key 'registries' in global CLI configuration"
        )

    if "local" not in _glob_conf["registries"]:
        raise fdp_exc.CLIConfigurationError(
            "Expected 'local' registry in global CLI configuration registries"
        )

    if "directory" not in _glob_conf["registries"]["local"]:
        raise fdp_exc.CLIConfigurationError(
            "Expected directory of local registry in global CLI configuration"
        )

    return _glob_conf["registries"]["local"]["directory"]


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


def registry_session_port_file(registry_dir: str = None) -> str:
    """Retrieve the location of the registry session port file

    Parameters
    ----------
    registry_dir : str, optional
        registry directory, by default None

    Returns
    -------
    str
        path to registry port session file
    """
    if not registry_dir:
        registry_dir = registry_home()
    return os.path.join(registry_dir, "session_port.log")


def registry_session_address_file(registry_dir: str = None) -> str:
    """Retrieve the location of the registry session port file

    Parameters
    ----------
    registry_dir : str, optional
        registry directory, by default None

    Returns
    -------
    str
        path to registry port session file
    """
    if not registry_dir:
        registry_dir = registry_home()
    return os.path.join(registry_dir, "session_address.log")


def registry_session_port(registry_dir: str = None) -> int:
    """Retrieve the registry session port

    Unlike 'get_local_port' within the configuration module, this retrieves the
    port number from the file generated by the registry itself

    Parameters
    ----------
    registry_dir : str, optional
        registry directory, by default None

    Returns
    -------
    int
        current/most recent port used to launch the registry
    """
    return int(
        open(registry_session_port_file(registry_dir), encoding="utf-8").read().strip()
    )


def registry_session_address(registry_dir: str = None) -> str:
    """Retrieve the registry session address

    Unlike 'get_local_address' within the configuration module, this retrieves the
    port number from the file generated by the registry itself

    Parameters
    ----------
    registry_dir : str, optional
        registry directory, by default None

    Returns
    -------
    str
        current/most recent address used to launch the registry
    """
    if not os.path.exists(registry_session_address_file(registry_dir)):
        _logger.warning(
            "Session Address file not found, please make sure your registry is up-to-date"
        )
        _logger.info("Using 127.0.0.1")
        return "127.0.0.1"

    _address = (
        open(registry_session_address_file(registry_dir), encoding="utf-8")
        .read()
        .strip()
    )
    if _address != "0.0.0.0":
        return _address
    else:
        return "127.0.0.1"


def staging_cache(user_loc: str) -> str:
    """Location of staging cache for the given repository"""
    return os.path.abspath(
        os.path.join(find_fair_root(user_loc), FAIR_FOLDER, "staging")
    )


def default_data_dir(location: str = "local") -> str:
    """Location of the default data store"""
    if not os.path.exists(global_fdpconfig()):
        raise fdp_exc.InternalError(
            f"Failed to read CLI global config file '{global_fdpconfig()}'"
        )
    _glob_conf = yaml.safe_load(open(global_fdpconfig(), encoding="utf-8"))
    if "data_store" in _glob_conf["registries"][location]:
        return _glob_conf["registries"][location]["data_store"]
    if location == "local":
        return os.path.join(USER_FAIR_DIR, f"data{os.path.sep}")
    else:
        raise fdp_exc.UserConfigError("Cannot guess remote data store location")


def local_fdpconfig(user_loc: str = os.getcwd()) -> str:
    """Location of the FAIR-CLI configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), FAIR_FOLDER, FAIR_CLI_CONFIG)


def local_user_config(user_loc: str = os.getcwd()) -> str:
    """Location of the default user configuration file for the given repository"""
    return os.path.join(find_fair_root(user_loc), USER_CONFIG_FILE)


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
        _repository = git.Repo(start_directory, search_parent_directories=True)
    except git.InvalidGitRepositoryError as e:
        raise fdp_exc.UserConfigError(
            "Failed to retrieve git repository for current configuration"
            f" in location '{start_directory}'"
        ) from e

    return _repository.git.rev_parse("--show-toplevel").strip()


def set_file_permissions(path: str):
    for root, dirs, files in os.walk(path, topdown=False):
        for dir in [os.path.join(root, d) for d in dirs]:
            os.chmod(dir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
        for file in [os.path.join(root, f) for f in files]:
            os.chmod(file, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)


def remove_readonly(fn, path, excinfo):
    try:
        _ = excinfo  # Required to avoid unused variable warning
        # Remove readonly bit
        os.chmod(path, stat.S_IWRITE)
        # Call the provided function again
        fn(path)
    except Exception as exc:
        # Log the error
        print("Skipped:", path, "because:\n", exc)
