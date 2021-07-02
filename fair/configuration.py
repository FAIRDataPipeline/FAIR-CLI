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
import uuid
from typing import MutableMapping, Any, Dict, Tuple

import yaml
import click

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id


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


def set_email(repo_loc: str, email: str, Global: bool = False) -> None:
    """Update the email address for the user

    Parameters
    ----------
    repo_loc : str
        repository directory path
    email : str
        new email address to set
    Global : bool, optional
        whether to also override the global settings, by default False
    """
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["email"] = email
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if Global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf["user"]["email"] = email
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))


def set_user(repo_loc: str, name: str, Global: bool = False) -> None:
    """Update the name for the user

    Parameters
    ----------
    repo_loc : str
        repository directory path
    name : str
        new user full name
    Global : bool, optional
        whether to also override the global settings, by default False
    """
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["name"] = name
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if Global:
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
    _given = _local_conf["user"]["given_name"]
    if "family_name" in _local_conf["user"]:
        _family = _local_conf["user"]["family_name"]
    else:
        _family = ""
    return (_given, _family)


def get_current_user_orcid(repo_loc: str) -> str:
    """Retrieves the ORCID of the current session user as defined in the config

    Returns
    -------
    str
        user ORCID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    return _local_conf["user"]["orcid"]


def get_current_user_uuid(repo_loc: str) -> str:
    """Retrieves the UUID of the current session user as defined in the config

    Returns
    -------
    str
        user ORCID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    return _local_conf["user"]["uuid"]


def global_config_query() -> Dict[str, Any]:
    """Ask user question set for creating global FAIR config"""
    _def_local = "http://localhost:8000/api/"

    _remote_url = click.prompt(f"Remote API URL")
    _local_url = click.prompt(f"Local API URL", default=_def_local)

    _user_email = click.prompt("Email")
    _user_orcid = click.prompt("ORCID", default="None")

    _orcid_info = None
    _uuid = None

    if _user_orcid != "None":
        _orcid_info = fdp_id.check_orcid(_user_orcid)

        while not _orcid_info:
            click.echo("Invalid ORCID given.")
            _user_orcid = click.prompt("ORCID")
            _user_info = fdp_id.check_orcid(_user_orcid)

        click.echo(
            f"Found entry: {_user_info['given_name']} {_user_info['family_name']}"
        )

        _def_ospace = _user_info["given_name"][0]

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
            _user_info["given_name"] = _given_name.strip()
            _user_info["family_name"] = _family_name.strip()
        else:
            _def_ospace += _full_name
            _user_info["given_name"] = _full_name
            _user_info["family_name"] = None

        _user_info["uuid"] = _uuid

    _def_ospace = _def_ospace.lower().replace(" ", "").strip()

    _def_ispace = click.prompt("Default input namespace", default="None")
    _def_ispace = _def_ispace if _def_ispace != "None" else None
    _def_ospace = click.prompt("Default output namespace", default=_def_ospace)

    _user_info["email"] = _user_email

    return {
        "user": _user_info,
        "remotes": {"local": _local_url, "origin": _remote_url},
        "namespaces": {"input": _def_ispace, "output": _def_ospace},
    }


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
        _def_remote = click.prompt(f"Remote API URL", default=_def_remote)
        _def_local = click.prompt(f"Local API URL", default=_def_local)
        _def_ospace = click.prompt(
            "Default output namespace", default=_def_ospace
        )
        _def_ispace = click.prompt(
            "Default input namespace", default=_def_ispace
        )

    _local_config: Dict[str, Any] = {}

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
