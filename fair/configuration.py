#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Configuration
=============

Module contains methods relating to configuration of the CLI itself.

Contents
========

Functions
---------

    read_local_fdpconfig - read the contents of the local CLI config file

"""

__date__ = "2021-07-02"

import os
import pathlib
import uuid
from typing import MutableMapping, Any, Dict, Tuple

import yaml
import click

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id
import fair.server as fdp_serv
import fair.registry.requests as fdp_req


def read_local_fdpconfig(repo_loc: str) -> MutableMapping:
    """Read contents of repository level FAIR-CLI configurations.

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository directory

    Returns
    -------
    MutableMapping
        configurations as a mapping
    """
    _local_config: MutableMapping = {}

    # Retrieve the location of this repositories CLI config file
    _local_config_file_addr = fdp_com.local_fdpconfig(repo_loc)
    if os.path.exists(_local_config_file_addr):
        _local_config = yaml.safe_load(open(_local_config_file_addr))

    return _local_config


def read_global_fdpconfig() -> MutableMapping:
    """Read contents of the global FAIR-CLI configurations.

    Returns
    -------
    MutableMapping
        configurations as a mapping
    """
    _global_config: MutableMapping = {}

    # Retrieve the location of the global CLI config file
    _global_config_addr = fdp_com.global_fdpconfig()

    if os.path.exists(_global_config_addr):
        _global_config = yaml.safe_load(open(_global_config_addr))

    return _global_config


def set_email(repo_loc: str, email: str, is_global: bool = False) -> None:
    """Update the email address for the user

    Parameters
    ----------
    repo_loc : str
        repository directory path
    email : str
        new email address to set
    is_global : bool, optional
        whether to also override the global settings, by default False
    """
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["email"] = email
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if is_global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf["user"]["email"] = email
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))


def set_user(repo_loc: str, name: str, is_global: bool = False) -> None:
    """Update the name for the user

    Parameters
    ----------
    repo_loc : str
        repository directory path
    name : str
        new user full name
    is_global : bool, optional
        whether to also override the global settings, by default False
    """
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["name"] = name
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if is_global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf["user"]["name"] = name
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))


def get_current_user_name(repo_loc: str) -> Tuple[str]:
    """Retrieves the name of the current session user as defined in the config

    Returns
    -------
    str
        user name
    """
    _local_conf = read_local_fdpconfig(repo_loc)

    if not _local_conf:
        raise fdp_exc.CLIConfigurationError("Cannot retrieve current user from empty CLI config")

    _given = _local_conf["user"]["given_names"]
    if "family_name" in _local_conf["user"]:
        _family = _local_conf["user"]["family_name"]
    else:
        _family = ""
    return (_given, _family)


def get_local_uri(repo_loc: str) -> str:
    """Retrieves the URI of the local registry
    
    Returns
    -------
    str
        local URI path
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    return _local_conf["remotes"]["local"]


def get_remote_uri(repo_loc: str, remote_label: str = 'origin') -> str:
    """Retrieves the URI of the remote registry

    Parameters
    ----------
    repo_loc : str
        local FAIR repository directory
    remote_label : str, optional
        label of remote to retrieve, default is 'origin' 
    
    Returns
    -------
    str
        remote URI path
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    return _local_conf["remotes"][remote_label]


def get_current_user_orcid(repo_loc: str) -> str:
    """Retrieves the ORCID of the current session user as defined in the config

    Returns
    -------
    str
        user ORCID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    try:
        _orcid =_local_conf["user"]["orcid"]
    except KeyError:
        _orcid = None
    if not _orcid or _orcid == "None":
        raise fdp_exc.CLIConfigurationError("No ORCID defined.")
    return _orcid


def get_current_user_uuid(repo_loc: str) -> str:
    """Retrieves the UUID of the current session user as defined in the config

    Returns
    -------
    str
        user ORCID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    return _local_conf["user"]["uuid"]

def check_registry_exists() -> bool:
    """Checks if fair registry is set up on users machine

    Returns
    -------
    bool
        True if registry exists, else False
    """

    directory = os.path.join(pathlib.Path.home(), '.fair/registry')
    return os.path.isdir(directory)


def _get_user_info_and_namespaces() -> Dict[str, Dict]:
    _user_email = click.prompt("Email")
    _user_orcid = click.prompt("ORCID", default="None")

    if _user_orcid != "None":
        _user_info = fdp_id.check_orcid(_user_orcid)

        while not _user_info:
            click.echo("Invalid ORCID given.")
            _user_orcid = click.prompt("ORCID")
            _user_info = fdp_id.check_orcid(_user_orcid)

        click.echo(
            f"Found entry: {_user_info['given_names']} "
            f"{_user_info['family_name']}"
        )

        _def_ospace = _user_info["given_names"][0]

        if len(_user_info["family_name"].split()) > 1:
            _def_ospace += _user_info["family_name"].split()[-1]
        else:
            _def_ospace += _user_info["family_name"]

    else:
        _uuid = str(uuid.uuid4())
        _full_name = click.prompt("Full Name")
        _def_ospace = ""
        _user_info = {}
        if len(_full_name.split()) > 1:
            _given_name, _family_name = _full_name.split(" ", 1)
            _def_ospace = _full_name.lower().strip()[0]
            _def_ospace += _full_name.lower().split()[-1]
            _user_info["given_names"] = _given_name.strip()
            _user_info["family_name"] = _family_name.strip()
        else:
            _def_ospace += _full_name
            _user_info["given_names"] = _full_name
            _user_info["family_name"] = None

        _user_info["uuid"] = _uuid

        _def_ospace = _def_ospace.lower().replace(" ", "").strip()

        _def_ispace = click.prompt("Default input namespace", default="None")
        _def_ispace = _def_ispace if _def_ispace != "None" else None
        _def_ospace = click.prompt(
            "Default output namespace", default=_def_ospace
        )

        _namespaces = {"input": _def_ispace, "output": _def_ospace}

        _user_info["email"] = _user_email
        _user_info["orcid"] = _user_orcid

    return {"user": _user_info, "namespaces": _namespaces}


def global_config_query() -> Dict[str, Any]:
    """Ask user question set for creating global FAIR config"""

    click.echo("Checking for local registry")
    if check_registry_exists():
        click.echo("Local registry found")
    else:
        click.confirm(
            "Local registry not found, would you like to install now?",
            abort = True
        )
        fdp_serv.install_registry()

    _def_local = "http://localhost:8000/api/"

    _remote_url = click.prompt("Remote API URL")
    _local_url = click.prompt("Local API URL", default=_def_local)

    if not fdp_serv.check_server_running(_local_url):
        _run_server = click.confirm(
            "Local registry is offline, would you like to start it?",
            default=False
        )
        if _run_server:
            fdp_serv.launch_server(_local_url)
        else:
            click.echo("Temporarily launching server to retrieve API token.")
            fdp_serv.launch_server(_local_url)
            fdp_serv.stop_server(_local_url)
            try:
                fdp_req.local_token()
            except fdp_exc.FileNotFoundError:
                raise fdp_exc.RegistryError(
                    "Failed to retrieve local API token from registry."
                )

    _def_data_store = click.prompt(
        "Default Data Store: ", 
        default=os.path.join(fdp_com.USER_FAIR_DIR, 'data')
    )

    _glob_conf_dict = _get_user_info_and_namespaces()
    _glob_conf_dict["remotes"] = {"local": _local_url, "origin": _remote_url}
    _glob_conf_dict["data_store"] = _def_data_store

    return _glob_conf_dict


def local_config_query(
    global_config: Dict[str, Any] = read_global_fdpconfig(),
    first_time_setup: bool = False,
) -> Dict[str, Any]:
    """Ask user questions to create local user config

    Parameters
    ----------
    global_config : Dict[str, Any], optional
        global configuration dictionary
    first_time_setup : bool, optional
        if first time need to setup globals as well, by default False

    Returns
    -------
    Dict[str, Any]
        dictionary of local configurations
    """
    # Try extracting global configurations. If any keys do not exist re-run
    # setup for creation of these, then try again.
    try:
        _def_remote = global_config["remotes"]["origin"]
        _def_local = global_config["remotes"]["local"]
        _def_ospace = global_config["namespaces"]["output"]
        _def_user = global_config["user"]
    except KeyError:
        click.echo(
            "Error: Failed to read global configuration,"
            " re-running global setup."
        )
        first_time_setup = True
        global_config = global_config_query()
        _def_remote = global_config["remotes"]["origin"]
        _def_local = global_config["remotes"]["local"]
        _def_ospace = global_config["namespaces"]["output"]
        _def_user = global_config["user"]

    # Allow the user to continue without an input namespace as some
    # functionality does not require this.
    if "input" not in global_config["namespaces"]:
        click.echo(
            "Warning: No global input namespace declared,"
            " in order to use the registry you will need to specify one"
            " within this local configuration."
        )
        _def_ispace = None
    else:
        _def_ispace = global_config["namespaces"]["input"]

    _desc = click.prompt("Project description")

    # If this is not the first setup it means globals are available so these
    # can be suggested as defaults during local setup
    if not first_time_setup:
        _def_remote = click.prompt("Remote API URL", default=_def_remote)
        _def_local = click.prompt("Local API URL", default=_def_local)
        _def_ospace = click.prompt(
            "Default output namespace", default=_def_ospace
        )
        _def_ispace = click.prompt(
            "Default input namespace", default=_def_ispace
        )

    _local_config: Dict[str, Any] = {}

    _local_config['data_store'] = global_config['data_store']

    _local_config["namespaces"] = {
        "output": _def_ospace,
        "input": _def_ispace,
    }

    _local_config["remotes"] = {
        "origin": _def_remote,
        "local": _def_local,
    }
    _local_config["description"] = _desc
    _local_config["user"] = _def_user

    return _local_config
