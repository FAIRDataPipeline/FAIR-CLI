import os
import sys
from typing import MutableMapping, Any, List

import yaml
import click

import fair.common as fdp_com


def read_local_fdpconfig(repo_loc: str) -> MutableMapping:
    _local_config: MutableMapping = {}
    _local_config_file_addr = fdp_com.local_fdpconfig(repo_loc)

    if os.path.exists(_local_config_file_addr):
        _local_config = yaml.load(
            open(_local_config_file_addr), Loader=yaml.SafeLoader
        )

    return _local_config


def read_global_fdpconfig() -> MutableMapping:
    _global_config: MutableMapping = {}
    _global_config_addr = fdp_com.global_fdpconfig()

    if os.path.exists(_global_config_addr):
        _global_config = yaml.load(
            open(_global_config_addr), Loader=yaml.SafeLoader
        )

    return _global_config


def _get_config_property(config_data: MutableMapping, *args) -> Any:
    _object: Any = config_data
    for key in args:
        try:
            _object = _object[key]
        except Keyerror:
            click.echo(
                "Failed to retrieve property "
                f"'{'/'.join(args)}' from configuration"
            )
            sys.exit(1)
    return _object


def set_email(repo_loc: str, email: str) -> None:
    """Update the email address for the user"""
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["email"] = email
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))


def set_user(repo_loc: str, name: str) -> None:
    """Update the name of the user"""
    _loc_conf = read_local_fdpconfig(repo_loc)
    _loc_conf["user"]["name"] = name
    yaml.dump(_loc_conf, open(fdp_com.local_fdpconfig(repo_loc), "w"))


def get_current_user() -> str:
    """Retrieves the name of the current session user as defined in the config

    Returns
    -------
    str
        user name
    """
    return _get_config_property(read_global_fdpconfig(), "user", "name")
