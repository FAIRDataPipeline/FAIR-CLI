#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Parse User Config
=================

Perform parsing of the user updated `config.yaml` file.


Contents
========

Functions
-------

    glob_read_write - swap glob expressions for registry entries
    subst_cli_vars  - substitute recognised FAIR CLI variables 

"""

__date__ = "2021-08-04"

import datetime
import typing
import collections.abc
import os
import re

import git
import yaml

import fair.registry.requests as fdp_reg_req
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.utilities as fdp_util
import fair.common as fdp_com


def glob_read_write(
    local_repo: str,
    config_dict_sub: typing.List,
    search_key: str = 'name',
    local_glob: bool = False) -> typing.List:
    """Substitute glob expressions in the 'read' or 'write' part of a user config 
    
    Parameters
    ----------
    local_repo : str
        local FAIR repository directory
    config_dict_sub : List[Dict]
        entries to read/write from registry
    search_key : str, optional
        key to search under, default is 'name'
    local_glob : bool, optional
        whether to search the local or remote registry,
        default is False.
    """
    _parsed: typing.List[typing.Dict] = []
    
    # Check whether to glob the local or remote registry
    # retrieve the URI from the repository CLI config
    if local_glob:
        _uri = fdp_conf.get_local_uri(local_repo) 
    else:
        _uri = fdp_conf.get_remote_uri(local_repo)

    # Iterate through all entries in the section looking for any
    # key-value pairs that contain glob statements.
    for entry in config_dict_sub:
        _glob_vals = [(k, v) for k, v in entry.items() if '*' in v]
        if len(_glob_vals) > 1:
            # For now only allow one value within the dictionary to have them
            raise fdp_exc.NotImplementedError(
                "Only one key-value pair in a 'read' list entry may contain a"
                " globbable value"
            )
        elif len(_glob_vals) == 0:
            # If no globbables keep existing statement
            _parsed.append(entry)
            continue

        _key_glob, _globbable = _glob_vals[0]

        # Send a request to the relevant registry using the search string
        # and the selected search key
        _results = fdp_reg_req.get(
            _uri,
            (_key_glob,),
            params = {search_key: _globbable}
        )

        # Iterate through all results, make a copy of the entry and swap
        # the globbable statement for the result statement appending this
        # to the output list
        for result in _results:
            _entry_dict = entry.copy()
            _entry_dict[_key_glob] = result[search_key]
            _parsed.append(_entry_dict)

    # Before returning the list of dictionaries remove any duplicates    
    return fdp_util.remove_dictlist_dupes(_parsed)
  

def subst_cli_vars(run_dir: str, run_time: datetime.datetime, config_yaml: str) -> typing.Dict:
    """Load configuration and substitute recognised FAIR CLI variables for their values

    Parameters
    ----------
    run_dir : str
        location of code run directory (not to be confused with
        local FAIR project repository)

    run_time : datetime.datetime
        time of run execution

    config_yaml : str
        location of `config.yaml` file

    Returns
    -------
    Dict
        new user configuration dictionary with substitutions    
    """
    if not os.path.exists(config_yaml):
        raise fdp_exc.FileNotFoundError(
            f"Cannot open user configuration file '{config_yaml}', "
            "file does not exist"
        )

    def _get_id(run_dir):
        try:
            return fdp_conf.get_current_user_orcid(run_dir)
        except fdp_exc.CLIConfigurationError:
            return fdp_conf.get_current_user_uuid(run_dir)

    # Substitutes are defined as functions for which particular cases
    # can be given as arguments, e.g. for DATE the format depends on if
    # the key is a version key or not.
    # Tags in config.yaml are specified as ${{ CLI.VAR }}

    def _tag_check(*args, **kwargs):
        _repo = git.Repo(
            fdp_com.find_fair_root(os.path.dirname(config_yaml))
        )
        if len(_repo.tags) < 1:
            fdp_exc.UserConfigError("Cannot use GIT_TAG variable, no git tags found.")
        return _repo.tags[-1].name

    _substitutes: collections.abc.Mapping = {
        "DATE": run_time.strftime("%Y%m%d"),
        "DATETIME": run_time.strftime("%Y-%m-%s %H:%M:%S"),
        "USER": fdp_conf.get_current_user_name(os.path.dirname(config_yaml)),
        "USER_ID": _get_id(run_dir),
        "REPO_DIR": fdp_com.find_fair_root(
            os.path.dirname(config_yaml)
        ),
        "CONFIG_DIR": run_dir,
        "SOURCE_CONFIG": config_yaml,
        "GIT_BRANCH": git.Repo(
            fdp_com.find_fair_root(os.path.dirname(config_yaml))
        ).active_branch.name,
        "GIT_REMOTE_ORIGIN": git.Repo(
            fdp_com.find_fair_root(os.path.dirname(config_yaml))
        )
        .remotes["origin"]
        .url,
        "GIT_TAG": _tag_check,
    }

    # Quickest to substitute all in one go by opening config as a string
    with open(config_yaml) as f:
        _conf_str = f.read()

    # Additional parser for formatted datetime
    _regex_dt_fmt = re.compile(r'\$\{\{\s*DATETIME\-.+\s*\}\}')
    _regex_fmt = re.compile(r'\$\{\{\s*DATETIME\-(.+)\s*\}\}')

    _dt_fmt_res = _regex_dt_fmt.findall(_conf_str)
    _fmt_res = _regex_fmt.findall(_conf_str)

    # The two regex searches should match lengths
    if len(_dt_fmt_res) != len(_fmt_res):
        raise fdp_exc.UserConfigError("Failed to parse formatted datetime variable")

    if _dt_fmt_res:
        for i, _ in enumerate(_dt_fmt_res):
            _time_str = run_time.strftime(_fmt_res[i].strip())
            _conf_str = _conf_str.replace(_dt_fmt_res[i], _time_str)

    _regex_dict = {
        var: r'\$\{\{\s*'+f'{var}'+r'\s*\}\}'
        for var in _substitutes
    }
    
    # Perform string substitutions
    for var, subst in _regex_dict.items():
        _conf_str = re.sub(subst, str(_substitutes[var]), _conf_str)

    # Load the YAML (this also verifies the write was successful)
    _user_conf = yaml.safe_load(_conf_str)

    return _user_conf
