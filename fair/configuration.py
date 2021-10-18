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
    read_global_fdpconfig - read the contents of the global CLI config file
    set_email - set the user's email in the configuration
    set_user - set the user's name in the configuration
    get_current_user_name - retrieve name of the current user
    get_remote_uri - retrieve URL for the remote registry
    local_git_repo - retrieve path for the current project Git repository
    remote_git_repo - retrieve URL for the remote Git repository
    get_remote_token - retrieve the token for the remote registry
    get_session_git_remote - get the current git remote label or URL
    get_current_user_orcid - retrieve the ORCID of the current user if specified
    get_current_user_uuid - retrieve the uuid of the current user if specified
    check_registry_exists - check that the specified local registry directory exists
    get_local_uri - retrieve the URL of the local registry
    get_local_port - retrieve the port number of the local registry
    global_config_query - setup the global configuration using prompts
    local_config_query - setup the local configuration using prompts
    fdp_config_value - retrieve a value from either local or global configurations
    write_data_store - retrieve the current write data store
    input_namespace - retrieve current input namespace
    output_namespace - retrieve current output namespace
    is_public - retrieve the current data is_public label
    read_version - retrieve the current data read version
    write_version - retrieve the current data write
    registry_url - retrieves either local or remote registry URL
    update_metadata - updates the metadata within a use configuration file
"""

__date__ = "2021-07-02"

import os
import pathlib
import uuid
import copy
import logging
import typing
import yaml
import click
import git

from urllib.parse import urlparse

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id
import fair.registry.server as fdp_serv
import fair.registry.requests as fdp_req
import fair.registry.versioning as fdp_ver


def read_local_fdpconfig(repo_loc: str) -> typing.MutableMapping:
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
    _local_config: typing.MutableMapping = {}

    # Retrieve the location of this repositories CLI config file
    _local_config_file_addr = fdp_com.local_fdpconfig(repo_loc)
    if os.path.exists(_local_config_file_addr):
        _local_config = yaml.safe_load(open(_local_config_file_addr))

    return _local_config


def read_global_fdpconfig() -> typing.MutableMapping:
    """Read contents of the global FAIR-CLI configurations.

    Returns
    -------
    MutableMapping
        configurations as a mapping
    """
    _global_config: typing.MutableMapping = {}

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
    if len(name.split()) > 1:
        _given_name, _family_name = name.rsplit(" ", 1)
        _loc_conf['user']['given_names'] = _given_name.title().strip()
        _loc_conf['user']['family_name'] = _family_name.title().strip()
        if is_global:
            _glob_conf = read_global_fdpconfig()
            _glob_conf['user']['given_names'] = _given_name.title().strip()
            _glob_conf['user']['family_name'] = _family_name.title().strip()
            yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))
    else:
        _loc_conf['user']['given_names'] = name.title().strip()
        _loc_conf['user']['family_name'] = None
        if is_global:
            _glob_conf = read_global_fdpconfig()
            _glob_conf['user']['given_names'] = name.title().strip()
            _glob_conf['user']['family_name'] = None
            yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if is_global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf['user']['name'] = name
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))          


def get_current_user_name(repo_loc: str) -> typing.Tuple[str]:
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


def local_git_repo(cfg: typing.Dict = None) -> str:
    """Retrieves the local repository git directory

    Parameters
    ----------
    cfg : Dict
        Dict containing CLI config

    Returns
    -------
    str
        the root git repository directory
    """

    try:
        return fdp_config_value(
            cfg, "local_repo", ["git", "local_repo"]
        )
    except KeyError:
        raise fdp_exc.InternalError(
            "Failed to retrieve local project git repository directory"
        )


def remote_git_repo(cfg: typing.Dict = None) -> str:
    """Retrieves the remote repository git directory

    Parameters
    ----------
    cfg : Dict
        Dict containing CLI config

    Returns
    -------
    str
        the root git repository directory
    """

    try:
        _remote = fdp_config_value(
            cfg, "remote", ["git", "remote"]
        )
        _local = local_git_repo(cfg)
        return git.Repo(_local).remote(_remote).url

    except KeyError:
        raise fdp_exc.InternalError(
            "Failed to retrieve remote project git repository directory"
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


def get_session_git_remote(repo_loc: str, url: bool = False) -> str:
    """Retrieves the local repository git remote

    Parameters
    ----------
    repo_loc : str
        Location of session CLI config
    url : optional, bool
        If True return the URL for the remote instead

    Returns
    -------
    str
        the remote label or URL of the git repository
    """
    _local_conf = read_local_fdpconfig(repo_loc)

    try:
        _remote_label = _local_conf['git']['remote']
    except KeyError:
        raise fdp_exc.InternalError(
            "Failed to retrieve project git repository remote"
        )
    
    if not url:
        return _remote_label
    
    _repo_root = fdp_com.find_git_root(repo_loc)

    try:
        return git.Repo(_repo_root).remote(_remote_label).url
    except ValueError:
        raise fdp_exc.CLIConfigurationError(
            f"Failed to retrieve URL for git remote '{_remote_label}'"
        )


def get_current_user_orcid(repo_loc: str) -> str:
    """Retrieves the ORCID of the current session user if defined in the config

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
    """Retrieves the UUID of the current session user if defined in the config

    Returns
    -------
    str
        user UUID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    try:
        _uuid = _local_conf['user']['uuid']
    except KeyError:
        _uuid = None
    if not _uuid or _uuid == "None":
        raise fdp_exc.CLIConfigurationError("No UUID defined.")
    return _uuid


def get_current_user_rorid(repo_loc: str) -> str:
    """Retrieves the ROR ID of the current session user if defined in the config

    Returns
    -------
    str
        user ROR ID
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    try:
        _rorid = _local_conf['user']['ror']
    except KeyError:
        _rorid = None
    if not _rorid or _rorid == "None":
        raise fdp_exc.CLIConfigurationError("No ROR ID defined.")
    return _rorid


def get_current_user_uri(repo_loc: str) -> str:
    """Retrieves the URI identifier for the current user

    Returns
    -------
    str
        user URI identifier
    """
    _local_conf = read_local_fdpconfig(repo_loc)
    try:
        _uri = _local_conf['user']['uri']
    except KeyError:
        _uri = None
    if not _uri or _uri == "None":
        raise fdp_exc.CLIConfigurationError("No user URI identifier defined.")
    return _uri


def check_registry_exists(registry: str = None) -> bool:
    """Checks if fair registry is set up on users machine

    Returns
    -------
    bool
        True if registry exists, else False
    """
    if not registry:
        registry = fdp_serv.DEFAULT_REGISTRY_LOCATION
    return os.path.isdir(registry)


def get_local_uri() -> str:
    """Retrieve the local registry URI"""
    _cfg = read_global_fdpconfig()

    try:
        return _cfg['registries']['local']['uri']
    except KeyError:
        raise fdp_exc.CLIConfigurationError(
            "Expected key 'registries:local:uri' in global CLI configuration"
        )


def get_local_port(local_uri: str = None) -> int:
    """Retrieve the local port from the local URI"""
    if not local_uri:
        local_uri = get_local_uri()
    _port = urlparse(local_uri).port
    if not _port:
        raise fdp_exc.InternalError(
            "Failed to determine port number from local registry URL"
        )
    return _port

def _handle_orcid(user_orcid: str) -> typing.Tuple[typing.Dict, str]:
    """Extract the name information from an ORCID selection

    Parameters
    ----------
    user_orcid : str
        ORCID to search

    Returns
    -------
    user_info : typing.Dict
        dictionary of extracted name metadata
    str
        default output namespace
    """
    _user_info = fdp_id.check_orcid(user_orcid.strip())

    while not _user_info:
        click.echo("Invalid ORCID given.")
        user_orcid = click.prompt("ORCID")
        _user_info = fdp_id.check_orcid(user_orcid.strip())
    
    _user_info['orcid'] = user_orcid

    click.echo(
        f"Found entry: {_user_info['given_names']} "
        f"{_user_info['family_name']}"
    )

    _def_ospace = _user_info['given_names'][0]

    if len(_user_info['family_name'].split()) > 1:
        _def_ospace += _user_info['family_name'].split()[-1]
    else:
        _def_ospace += _user_info['family_name']

    return _user_info, _def_ospace


def _handle_ror(user_ror: str) -> typing.Tuple[typing.Dict, str]:
    """Extract institution name information from an ROR ID

    Parameters
    ----------
    user_ror : str
        ROR ID for an institution

    Returns
    -------
    user_info : typing.Dict
        dictionary of extracted name metadata
    str
        default output namespace
    """
    _user_info = fdp_id.check_ror(user_ror.strip())

    while not _user_info:
        click.echo("Invalid ROR ID given.")
        user_ror = click.prompt("ROR ID")
        _user_info = fdp_id.check_ror(user_ror.strip())

    _user_info['ror'] = user_ror

    click.echo(
        f"Found entry: {_user_info['family_name']} "
    )

    _def_ospace = _user_info['family_name'].lower().replace(' ', '_')

    return _user_info, _def_ospace


def _handle_grid(user_grid: str) -> typing.Tuple[typing.Dict, str]:
    """Extract institution name information from an GRID ID

    Parameters
    ----------
    user_grid : str
        GRID ID for an institution

    Returns
    -------
    user_info : typing.Dict
        dictionary of extracted name metadata
    str
        default output namespace
    """
    _user_info = fdp_id.check_grid(user_grid.strip())

    while not _user_info:
        click.echo("Invalid GRID ID given.")
        user_grid = click.prompt("GRID ID")
        _user_info = fdp_id.check_grid(user_grid.strip())

    _user_info['grid'] = user_grid

    click.echo(
        f"Found entry: {_user_info['family_name']} "
    )

    _def_ospace = _user_info['family_name'].lower().replace(' ', '_')

    return _user_info, _def_ospace


def _handle_uuid() -> typing.Tuple[typing.Dict, str]:
    """Obtain metadata for user where no ID provided

    Returns
    -------
    user_info : typing.Dict
        dictionary of extracted name metadata
    str
        default output namespace
    """
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

    return _user_info, _def_ospace


def _get_user_info_and_namespaces() -> typing.Dict[str, typing.Dict]:
    _user_email = click.prompt("Email")

    _invalid_input = True

    while _invalid_input:
        _id_type = click.prompt("Use ID (ORCID/ROR/GRID)", default="None")

        if _id_type.upper() == "ORCID":
            _user_orcid = click.prompt("ORCID")
            _user_info, _def_ospace = _handle_orcid(_user_orcid)
            _invalid_input = False
        elif _id_type.upper() == "ROR":
            _user_ror = click.prompt("ROR ID")
            _user_info, _def_ospace = _handle_ror(_user_ror)
            _invalid_input = False
        elif _id_type.upper() == "GRID":
            _user_grid = click.prompt("GRID ID")
            _user_info, _def_ospace = _handle_grid(_user_grid)
            _invalid_input = False
        elif _id_type.upper() == "NONE":
            _user_info, _def_ospace = _handle_uuid()
            _user_uuid = str(uuid.uuid4())
            _user_info['uuid'] = _user_uuid
            _invalid_input = False

    _user_info['email'] = _user_email

    _def_ospace = _def_ospace.lower().replace(" ", "").strip()

    _def_ispace = click.prompt("Default input namespace", default="None")
    _def_ispace = _def_ispace if _def_ispace != "None" else None
    _def_ospace = click.prompt(
        "Default output namespace", default=_def_ospace
    )

    _namespaces = {"input": _def_ispace, "output": _def_ospace}

    return {"user": _user_info, "namespaces": _namespaces}


def global_config_query(registry: str = None) -> typing.Dict[str, typing.Any]:
    """Ask user question set for creating global FAIR config"""
    if not registry:
        registry = fdp_serv.DEFAULT_REGISTRY_LOCATION
    click.echo("Checking for local registry")
    if check_registry_exists(registry):
        click.echo("Local registry found")
    else:
        _install_reg = click.confirm(
            "Local registry not found, would you like to install now?",
            default=True
        )
        if not _install_reg:
            raise fdp_exc.CLIConfigurationError(
                "FAIR repository initialisation requires a local registry installation, aborting."
            )
        fdp_serv.install_registry()

    _local_uri = click.prompt("Local Registry URL", default=fdp_serv.DEFAULT_LOCAL_REGISTRY_URL)

    _default_rem = 'https://data.scrc.uk/api/'
    _remote_url = click.prompt("Remote API URL", default=_default_rem)

    _rem_data_store = click.prompt(
        "Remote Data Storage Root",
        default=_remote_url.replace("api", "data")
    )

    _default_token = os.path.join(registry, "remote-token")
    _rem_key_file = click.prompt(
        "Remote API Token File",
        default=_default_token
    )
    _rem_key_file = os.path.expandvars(_rem_key_file)

    #TODO fix search for valid token
    while (
        False and
        (not os.path.exists(_rem_key_file)
        or not open(_rem_key_file).read().strip()
        )):
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
            _cache_addr = os.path.join(fdp_com.session_cache_dir(), 'user.run')
            pathlib.Path(_cache_addr).touch()

        else:
            click.echo("Temporarily launching server to retrieve API token.")
            fdp_serv.launch_server(_local_uri, registry_dir=registry)
            fdp_serv.stop_server(registry_dir=registry, local_uri=_local_uri)
            try:
                fdp_req.local_token(registry_dir=registry)
            except fdp_exc.FileNotFoundError:
                raise fdp_exc.RegistryError(
                    "Failed to retrieve local API token from registry."
                )

    _loc_data_store = click.prompt(
        "Default Data Store",
        default=os.path.join(fdp_com.USER_FAIR_DIR, 'data/')
    )
    if _loc_data_store[-1] != os.path.sep:
        _loc_data_store += os.path.sep

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
    global_config: typing.Dict[str, typing.Any] = read_global_fdpconfig(),
    first_time_setup: bool = False,
) -> typing.Dict[str, typing.Any]:
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

    while _invalid_rem and _def_rem:
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

    _local_config: typing.Dict[str, typing.Any] = {}

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
    
    _local_config['user'] = _def_user

    return _local_config

def fdp_config_value(
        cfg: typing.Dict,
        user_key: str,
        central_key: list,
        default = None
    ):
    """configuration value from user config or local or global fair config"""
    if cfg:
        if "run_metadata" in cfg and user_key in cfg["run_metadata"]:
            return cfg["run_metadata"][user_key]

        for key in ["local", "global"]:
            _conf = cfg['config_files'][key]
            _found = True
            for term in central_key:
                if _found and term in _conf:
                    _conf = _conf[term]
                else:
                    _found = False
            if _found:
                return _conf
    elif not default:
        fdp_exc.InternalError(
            "No config yaml and no default provided"
        )

    if default != None:
        return default

    raise fdp_exc.CLIConfigurationError(
            f"Failed to find key '{central_key}' in 'cli-config.yaml's"
        )


def write_data_store(cfg: typing.Dict) -> str:
    """Location of the default data store"""
    _store = fdp_config_value(
        cfg, "write_data_store", ["registries", "local", "data_store"]
    )
    if _store[-1] != os.path.sep:
        _store += os.path.sep
    return _store


def input_namespace(cfg: typing.Dict) -> str:
    """input namespace"""
    return fdp_config_value(
        cfg, "default_input_namespace", ["namespaces", "input"]
    )


def output_namespace(cfg: typing.Dict) -> str:
    """output namespace"""
    return fdp_config_value(
        cfg, "default_output_namespace", ["namespaces", "input"]
    )


def is_public(cfg: typing.Dict) -> str:
    """default read version"""
    return fdp_config_value(
        cfg,
        "public",
        ["data", "public"],
        True
    )


def read_version(cfg: typing.Dict) -> str:
    """default read version"""
    return fdp_config_value(
        cfg,
        "default_read_version",
        ["version", "read"],
        f"${{{{ {fdp_ver.DEFAULT_READ_VERSION} }}}}"
    )


def write_version(cfg: typing.Dict) -> str:
    """default write version"""
    return fdp_config_value(
        cfg,
        "default_write_version",
        ["version", "write"],
        f"${{{{ {fdp_ver.DEFAULT_WRITE_VERSION} }}}}"
    )


def registry_url(registry_type: str, cfg: typing.Dict) -> str:
    """url of "local" or "remote" registry"""
    target = "origin" if registry_type == "remote" else registry_type
    return fdp_config_value(
        cfg, f"{registry_type}_data_registry_url", ["registries", target, "uri"]
    )


def add_site_configs(cfg: typing.Dict, repo_dir: str) -> None:
    cfg['config_files'] = {}

    _local_conf_path = fdp_com.local_fdpconfig(repo_dir)
    if not os.path.exists(_local_conf_path):
        raise fdp_exc.InternalError(
            f"Failed to read local CLI configuration '{_local_conf_path}'"
        )
    with open(_local_conf_path) as f:
        cfg['config_files']['local'] = yaml.safe_load(f)

    _global_conf_path = fdp_com.global_fdpconfig()
    if not os.path.exists(_global_conf_path):
        raise fdp_exc.InternalError(
            f"Failed to read global CLI configuration '{_global_conf_path}'"
        )
    with open(_global_conf_path) as f:
        cfg['config_files']['global'] = yaml.safe_load(f)

def update_metadata(cfg: typing.Dict) -> None:
    _logger = logging.getLogger("FAIRDataPipeline.Run")
    _cfg_meta = cfg['run_metadata']

    # Insert 'public: True' if absent from user config
    if 'public' not in _cfg_meta:
        _cfg_meta['public'] = is_public(cfg)
        _modified_config = True

    # Insert 'write_data_store' if absent from user config
    _wds = write_data_store(cfg)
    if 'write_data_store' not in _cfg_meta or _cfg_meta['write_data_store'] != _wds:
        _cfg_meta['write_data_store'] = _wds
        _modified_config = True

    # Insert 'default_input_namespace' if absent from user config
    if 'default_input_namespace' not in _cfg_meta:
        _cfg_meta['default_input_namespace'] = input_namespace(cfg)
        _modified_config = True

    # Insert 'default_output_namespace' if absent from user config
    if 'default_output_namespace' not in _cfg_meta:
        _cfg_meta['default_output_namespace'] = output_namespace(cfg)
        _modified_config = True

    # Insert 'local_registry_url' if absent from user config
    if 'local_data_registry_url' not in _cfg_meta:
        _cfg_meta['local_data_registry_url'] = registry_url("local", cfg)
        _modified_config = True

    # Insert 'remote_registry_url' if absent from user config
    if 'remote_data_registry_url' not in _cfg_meta:
        _cfg_meta['remote_data_registry_url'] = registry_url("origin", cfg)
        _modified_config = True

    # Insert 'local_repo' if absent from user config
    if 'local_repo' not in _cfg_meta:
        _cfg_meta['local_repo'] = local_git_repo(cfg)
        _modified_config = True

    # Insert 'remote_repo' if absent from user config
    if 'remote_repo' not in _cfg_meta:
        _cfg_meta['remote_repo'] = remote_git_repo(cfg)
        _modified_config = True

    # Insert default read version if absent from user config
    if 'default_read_version' not in _cfg_meta:
        _cfg_meta['default_read_version'] = read_version(cfg)
        _modified_config = True

    # Insert default write version if absent from user config
    if 'default_write_version' not in _cfg_meta:
        _cfg_meta['default_write_version'] = write_version(cfg)
        _modified_config = True

    if _modified_config:
        cfg['run_metadata'] = _cfg_meta
        _logger.debug("New metadata, replacing user config")
