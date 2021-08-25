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
import copy
import re
from typing import MutableMapping, Any, Dict, Tuple

import yaml
import click
import git

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id
import fair.registry.server as fdp_serv
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
    _loc_conf['user']['email'] = email
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if is_global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf['user']['email'] = email
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
    _loc_conf['user']['name'] = name
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if is_global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf['user']['name'] = name
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

    _given = _local_conf['user']['given_names']
    if "family_name" in _local_conf['user']:
        _family = _local_conf['user']['family_name']
    else:
        _family = ""
    return (_given, _family)


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
    return _local_conf['registries'][remote_label]['uri']


def get_session_git_repo(repo_loc: str) -> str:
    """Retrieves the local repository git directory

    Parameters
    ----------
    repo_loc : str
        Location of session CLI config

    Returns
    -------
    str
        the root git repository directory
    """
    _local_conf = read_local_fdpconfig(repo_loc)

    try:
        return _local_conf['git']['local_repo']
    except KeyError:
        raise fdp_exc.InternalError(
            "Failed to retrieve project git repository directory"
        )


def get_remote_token(repo_dir: str, remote: str = 'origin') -> str:
    _local_config = read_local_fdpconfig(repo_dir)
    if remote not in _local_config['registries']:
        raise fdp_exc.CLIConfigurationError(
            f"Cannot find remote registry '{remote}' in local CLI configuration"
        )
    if 'token' not in _local_config['registries'][remote]:
        raise fdp_exc.CLIConfigurationError(
            f"Cannot find token for registry '{remote}', no token file provided"
        )
    _token_file = _local_config['registries'][remote]['token']

    if not os.path.exists(_token_file):
        raise fdp_exc.FileNotFoundError(
            f"Cannot read token for registry '{remote}', no such token file"
        )

    _token = open(_token_file).read().strip()

    if not _token:
        raise fdp_exc.CLIConfigurationError(
            f"Cannot read token from file '{_token_file}', file is empty."
        )

    return _token


def get_session_git_remote(repo_loc: str) -> str:
    """Retrieves the local repository git remote

    Parameters
    ----------
    repo_loc : str
        Location of session CLI config

    Returns
    -------
    str
        the remote of the git repository
    """
    _local_conf = read_local_fdpconfig(repo_loc)

    try:
        return _local_conf['git']['remote']
    except KeyError:
        raise fdp_exc.InternalError(
            "Failed to retrieve project git repository remote"
        )


def get_current_user_orcid(repo_loc: str) -> str:
    """Retrieves the ORCID of the current session user as defined in the config

    Parameters
    ----------
    repo_loc : str
        Location of session CLI config

    Returns
    -------
    str
        user ORCID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    try:
        _orcid =_local_conf['user']['orcid']
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
    return _local_conf['user']['uuid']


def check_registry_exists(registry: str = None) -> bool:
    """Checks if fair registry is set up on users machine

    Returns
    -------
    bool
        True if registry exists, else False
    """
    if not registry:
        registry = os.path.join(pathlib.Path.home(), fdp_com.FAIR_FOLDER, 'registry')
    return os.path.isdir(registry)


def get_local_uri() -> str:
    _cfg = read_global_fdpconfig()

    try:
        return _cfg['registries']['local']['uri']
    except KeyError:
        raise fdp_exc.CLIConfigurationError(
            f"Expected key 'registries:local:uri' in local CLI configuration"
        )


def get_local_port(local_uri: str = None) -> str:
    if not local_uri:
        local_uri = get_local_uri()
    _port_res = re.findall(r'localhost:([0-9]+)', local_uri)
    if not _port_res:
        raise fdp_exc.InternalError(
            "Failed to determine port number from local registry URL"
        )
    return _port_res[0]



def _get_user_info_and_namespaces() -> Dict[str, Dict]:
    _user_email = click.prompt("Email")
    _user_orcid = click.prompt("ORCID", default="None")
    _user_uuid = None

    if _user_orcid != "None":
        _user_info = fdp_id.check_orcid(_user_orcid.strip())

        while not _user_info:
            click.echo("Invalid ORCID given.")
            _user_orcid = click.prompt("ORCID")
            _user_info = fdp_id.check_orcid(_user_orcid)

        click.echo(
            f"Found entry: {_user_info['given_names']} "
            f"{_user_info['family_name']}"
        )

        _def_ospace = _user_info['given_names'][0]

        if len(_user_info['family_name'].split()) > 1:
            _def_ospace += _user_info['family_name'].split()[-1]
        else:
            _def_ospace += _user_info['family_name']

    else:
        _user_orcid = None
        _user_uuid = str(uuid.uuid4())
        _full_name = click.prompt("Full Name")
        _def_ospace = ""
        _user_info = {}
        if len(_full_name.split()) > 1:
            _given_name, _family_name = _full_name.split(" ", 1)
            _def_ospace = _full_name.lower().strip()[0]
            _def_ospace += _full_name.lower().split()[-1]
            _user_info['given_names'] = _given_name.strip()
            _user_info['family_name'] = _family_name.strip()
        else:
            _def_ospace += _full_name
            _user_info['given_names'] = _full_name
            _user_info['family_name'] = None

    _user_info['uuid'] = _user_uuid

    _user_info['email'] = _user_email
    _user_info['orcid'] = _user_orcid

    _def_ospace = _def_ospace.lower().replace(" ", "").strip()

    _def_ispace = click.prompt("Default input namespace", default="None")
    _def_ispace = _def_ispace if _def_ispace != "None" else None
    _def_ospace = click.prompt(
        "Default output namespace", default=_def_ospace
    )

    _namespaces = {"input": _def_ispace, "output": _def_ospace}

    return {"user": _user_info, "namespaces": _namespaces}


def global_config_query(registry: str = None) -> Dict[str, Any]:
    """Ask user question set for creating global FAIR config"""

    if not registry:
        registry = os.path.join(
            pathlib.Path().home(), fdp_com.FAIR_FOLDER, 'registries'
        )

    click.echo("Checking for local registry")
    if check_registry_exists(registry):
        click.echo("Local registry found")
    else:
        click.confirm(
            "Local registry not found, would you like to install now?",
            abort = True
        )
        fdp_serv.install_registry()

    _default_url = 'http://localhost:8000/api/'
    _local_uri = click.prompt("Local Registry URL", default=_default_url)

    _remote_url = click.prompt("Remote API URL")

    _rem_data_store = click.prompt(
        "Remote Data Storage Root",
        default=_remote_url.replace("api", "data")
    )

    _rem_key_file = click.prompt("Remote API Token File")
    _rem_key_file = os.path.expandvars(_rem_key_file)

    while (
        not os.path.exists(_rem_key_file)
        or not open(_rem_key_file).read().strip()
        ):
        click.echo(
            f"Token file '{_rem_key_file}' does not exist or is empty, "
            "please provide a valid token file."
        )
        _rem_key_file = click.prompt("Remote API Token File")
        _rem_key_file = os.path.expandvars(_rem_key_file)

    if not fdp_serv.check_server_running(_local_uri):
        _run_server = click.confirm(
            "Local registry is offline, would you like to start it?",
            default=False
        )
        if _run_server:
            fdp_serv.launch_server(_local_uri, registry_dir=registry)

            # Keep server running by creating user run cache file
            _cache_addr = os.path.join(
                fdp_com.session_cache_dir(), f"user.run"
            )
            pathlib.Path(_cache_addr).touch()

        else:
            click.echo("Temporarily launching server to retrieve API token.")
            fdp_serv.launch_server(_local_uri, registry_dir=registry)
            fdp_serv.stop_server()
            try:
                fdp_req.local_token()
            except fdp_exc.FileNotFoundError:
                raise fdp_exc.RegistryError(
                    "Failed to retrieve local API token from registry."
                )

    _loc_data_store = click.prompt(
        "Default Data Store: ",
        default=os.path.join(fdp_com.USER_FAIR_DIR, 'data')
    )

    _glob_conf_dict = _get_user_info_and_namespaces()
    _glob_conf_dict['registries']  = {
        'local': {
            'uri': _local_uri,
            'directory': os.path.abspath(registry),
            'data_store': _loc_data_store
        },
        'origin': {
            'uri': _remote_url,
            'token': _rem_key_file,
            'data_store': _rem_data_store
        }
    }

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
        _def_remote = global_config['registries']['origin']['uri']
        _def_rem_key = global_config['registries']['origin']['token']
        _def_ospace = global_config['namespaces']['output']
        _def_user = global_config['user']
    except KeyError:
        click.echo(
            "Error: Failed to read global configuration,"
            " re-running global setup."
        )
        first_time_setup = True
        global_config = global_config_query()
        _def_remote = global_config['registries']['origin']['uri']
        _def_rem_key = global_config['registries']['origin']['token']
        _def_ospace = global_config['namespaces']['output']
        _def_user = global_config['user']

    # Allow the user to continue without an input namespace as some
    # functionality does not require this.
    if "input" not in global_config['namespaces']:
        click.echo(
            "Warning: No global input namespace declared,"
            " in order to use the registry you will need to specify one"
            " within this local configuration."
        )
        _def_ispace = None
    else:
        _def_ispace = global_config['namespaces']['input']

    #_desc = click.prompt("Project description")

    # Try checking to see if the current location is a git repository and
    # suggesting this as a default
    try:
        _git_repo = fdp_com.find_git_root(os.getcwd())
    except fdp_exc.UserConfigError:
        _git_repo = None

    _git_repo = click.prompt("Local Git repository", default=_git_repo)
    _invalid_repo = True

    # Check this is indeed a git repository
    while _invalid_repo:
        try:
            git.Repo(_git_repo)
            _invalid_repo = False
        except (git.InvalidGitRepositoryError, git.NoSuchPathError):
            _invalid_repo = True
            click.echo(
                "Invalid directory, location is not the head of a local"
                " git repository."
            )
            _git_repo = click.prompt("Local Git repository", default=_git_repo)

    # Set the remote to use within git by label, by default 'origin'
    # check the remote label is valid before proceeding
    try:
        git.Repo(_git_repo).remotes['origin']
        _def_rem = 'origin'
    except IndexError:
        _def_rem = None

    _git_remote = click.prompt("Git remote name", default=_def_rem)
    _invalid_rem = True

    while _invalid_rem:
        try:
            git.Repo(_git_repo).remotes[_git_remote]
            _invalid_rem = False
        except IndexError:
            _invalid_rem = True
            click.echo("Invalid remote name for git repository")
            _git_remote = click.prompt("Git remote name", default=_def_rem)

    _git_remote_repo = git.Repo(_git_repo).remotes[_git_remote].url

    click.echo(
        f"Using git repository remote '{_git_remote}': "
        f"{_git_remote_repo}"
    )

    # If this is not the first setup it means globals are available so these
    # can be suggested as defaults during local setup
    if not first_time_setup:
        _def_remote = click.prompt("Remote API URL", default=_def_remote)
        _def_rem_key = click.prompt("Remote API Token File", default=_def_rem_key)
        _def_rem_key = os.path.expandvars(_def_rem_key)
        while (
            not os.path.exists(_def_rem_key)
            or not open(_def_rem_key).read().strip()
        ):
            click.echo(
                f"Token file '{_def_rem_key}' does not exist or is empty, "
                "please provide a valid token file."
            )
            _def_rem_key = click.prompt("Remote API Token File")
            _def_rem_key = os.path.expandvars(_def_rem_key)
        _def_ospace = click.prompt(
            "Default output namespace", default=_def_ospace
        )
        _def_ispace = click.prompt(
            "Default input namespace", default=_def_ispace
        )

    _local_config: Dict[str, Any] = {}

    _local_config['namespaces'] = {
        "output": _def_ospace,
        "input": _def_ispace,
    }

    _local_config['git'] = {
        "remote": _git_remote,
        "local_repo": _git_repo,
        "remote_repo": _git_remote_repo
    }

    # Copy the global configuration then substitute updated
    # configurations
    _local_config['registries'] = copy.deepcopy(global_config['registries'])

    # Local registry is a globally defined entity
    del _local_config['registries']['local']

    _local_config['registries']['origin']['uri'] =  _def_remote

    #_local_config["description"] = _desc
    _local_config['user'] = _def_user

    return _local_config
