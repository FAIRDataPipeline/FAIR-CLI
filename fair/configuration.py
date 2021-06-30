import os
import socket
from typing import MutableMapping, Any, Dict

import yaml
import click

import fair.common as fdp_com
import fair.exceptions as fdp_exc


def read_local_fdpconfig(repo_loc: str) -> MutableMapping:
    _local_config: MutableMapping = {}
    _local_config_file_addr = fdp_com.local_fdpconfig(repo_loc)

    if os.path.exists(_local_config_file_addr):
        _local_config = yaml.safe_load(open(_local_config_file_addr))

    return _local_config


def read_global_fdpconfig() -> MutableMapping:
    _global_config: MutableMapping = {}
    _global_config_addr = fdp_com.global_fdpconfig()

    if os.path.exists(_global_config_addr):
        _global_config = yaml.safe_load(open(_global_config_addr))

    return _global_config


def _get_config_property(config_data: MutableMapping, *args) -> Any:
    _object: Any = config_data
    for key in args:
        try:
            _object = _object[key]
        except KeyError:
            raise fdp_exc.CLIConfigurationError(
                "Failed to retrieve property "
                f"'{'/'.join(args)}' from configuration"
            )
    return _object


def set_email(repo_loc: str, email: str, Global: bool = False) -> None:
    """Update the email address for the user"""
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["email"] = email
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if Global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf["user"]["email"] = email
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))


def set_user(repo_loc: str, name: str, Global: bool = False) -> None:
    """Update the name of the user"""
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["name"] = name
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))
    if Global:
        _glob_conf = read_global_fdpconfig()
        _glob_conf["user"]["name"] = name
        yaml.dump(_glob_conf, open(fdp_com.global_fdpconfig(), "w"))


def get_current_user() -> str:
    """Retrieves the name of the current session user as defined in the config

    Returns
    -------
    str
        user name
    """
    return _get_config_property(read_global_fdpconfig(), "user", "name")


def global_config_query() -> Dict[str, Any]:
    """Ask user question set for creating global FAIR config"""
    _def_local = "http://localhost:8000/api/"

    _remote_url = click.prompt(f"Remote API URL")
    _local_url = click.prompt(f"Local API URL", default=_def_local)

    _def_name = socket.gethostname()
    _user_name = click.prompt("Full name", default=_def_name)
    _user_email = click.prompt("Email")
    _user_orcid = click.prompt("ORCID", default="None")
    _user_orcid = _user_orcid if _user_orcid != "None" else None

    if len(_user_name.strip().split()) == 2:
        _def_ospace = _user_name.strip().lower().split()
        _def_ospace = _def_ospace[0][0] + _def_ospace[1]
    else:
        _def_ospace = _user_name.lower().replace(" ", "")

    _def_ispace = click.prompt("Default input namespace", default="None")
    _def_ispace = _def_ispace if _def_ispace != "None" else None
    _def_ospace = click.prompt("Default output namespace", default=_def_ospace)

    return {
        "user": {
            "name": _user_name,
            "email": _user_email,
            "orcid": _user_orcid,
        },
        "remotes": {"local": _local_url, "origin": _remote_url},
        "namespaces": {"input": _def_ispace, "output": _def_ospace},
    }


def local_config_query(
    global_config: Dict[str, Any] = {},
    first_time_setup: bool = False,
) -> Dict[str, Any]:
    """Ask user questions to create local user config"""
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
