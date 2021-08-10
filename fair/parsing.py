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
import semver

import fair.registry.requests as fdp_reg_req
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.utilities as fdp_util
import fair.common as fdp_com
import fair.registry.versioning as fdp_ver


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
  

def subst_cli_vars(
    local_uri: str,
    run_dir: str,
    run_time: datetime.datetime,
    config_yaml: str
    ) -> typing.Dict:
    """Load configuration and substitute recognised FAIR CLI variables

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry
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

    _fair_head = fdp_com.find_fair_root(os.path.dirname(config_yaml))

    def _repo_check(directory: str):
        # Currently only supports FAIR repository setup matching that of git
        if not os.path.exists(os.path.join(directory, '.git')):
            raise fdp_exc.NotImplementedError(
                "Expected FAIR repository working tree head to match that of "
                "git when using GIT_* CLI variables",
                hint="Did you run 'fair init' in the same location as 'git init'?"
            )
        return git.Repo(directory)

    def _tag_check(*args, **kwargs):
        _repo = _repo_check(_fair_head)
        if len(_repo.tags) < 1:
            fdp_exc.UserConfigError(
                "Cannot use GIT_TAG variable, no git tags found."
            )
        return _repo.tags[-1].name


    _substitutes: collections.abc.Mapping = {
        "DATE": lambda : run_time.strftime("%Y%m%d"),
        "DATETIME": lambda : run_time.strftime("%Y-%m-%s %H:%M:%S"),
        "USER": lambda : fdp_conf.get_current_user_name(os.path.dirname(config_yaml)),
        "USER_ID": lambda : _get_id(run_dir),
        "REPO_DIR": lambda : fdp_com.find_fair_root(
            os.path.dirname(config_yaml)
        ),
        "CONFIG_DIR": lambda : run_dir,
        "SOURCE_CONFIG": lambda : config_yaml,
        "GIT_BRANCH": lambda : _repo_check(_fair_head).active_branch.name,
        "GIT_TAG": _tag_check,
    }

    # Quickest to substitute all in one go by opening config as a string
    with open(config_yaml) as f:
        _conf_str = f.read()

    # Additional parser for formatted datetime
    _regex_dt_fmt = re.compile(r'\$\{\{\s*DATETIME\-.+\s*\}\}')
    _regex_fmt = re.compile(r'\$\{\{\s*DATETIME\-(.+)\s*\}\}')

    # Additional parse for branch choice
    _regex_git_remote = re.compile(r'\$\{\{\s*GIT_REMOTE_[A-Z]+\s*\}\}')
    _regex_git_remote_name = re.compile(r'\$\{\{\s*GIT_REMOTE_([A-Z]+)\s*\}\}')

    _dt_fmt_res = _regex_dt_fmt.findall(_conf_str)
    _fmt_res = _regex_fmt.findall(_conf_str)

    _git_remote_res = _regex_git_remote.findall(_conf_str)
    _git_rem_name_res = _regex_git_remote_name.findall(_conf_str)

    # The two regex searches should match lengths
    if len(_git_remote_res) != len(_git_rem_name_res):
        raise fdp_exc.UserConfigError("Failed to parse git remote variable")

    # The two regex searches should match lengths
    if len(_dt_fmt_res) != len(_fmt_res):
        raise fdp_exc.UserConfigError(
            "Failed to parse formatted datetime variable"
        )

    if _dt_fmt_res:
        for i, _ in enumerate(_dt_fmt_res):
            _time_str = run_time.strftime(_fmt_res[i].strip())
            _conf_str = _conf_str.replace(_dt_fmt_res[i], _time_str)

    if _git_remote_res:
        for i, _ in enumerate(_git_remote_res):
            _name_str = _git_rem_name_res[i]

            try:
                _rem_str = _repo_check(_fair_head).remotes[_name_str.lower()]
                _rem_str = _rem_str.url
            except KeyError:
                raise fdp_exc.UserConfigError(
                    f"Failed to find URL for git remote '{_name_str.lower()}'"
                )
            
            _conf_str = _conf_str.replace(_git_remote_res[i], _rem_str)

    _regex_dict = {
        var: r'\$\{\{\s*'+f'{var}'+r'\s*\}\}'
        for var in _substitutes
    }
    
    # Perform string substitutions
    for var, subst in _regex_dict.items():
        # Only execute functions in var substitutions that are required
        if re.findall(subst, _conf_str):
            _conf_str = re.sub(subst, str(_substitutes[var]()), _conf_str)

    # Load the YAML (this also verifies the write was successful)
    _user_conf = yaml.safe_load(_conf_str)

    # Parse 'write' block for any versioning
    _user_conf = subst_versions(local_uri, _user_conf)    

    return _user_conf


def subst_versions(local_uri: str, config_yaml_dict: typing.Dict) -> typing.Dict:
    # dynamic versionables only present in write statement
    if 'write' not in config_yaml_dict:
        return config_yaml_dict
    
    _out_dict = config_yaml_dict.copy()
    _obj_type = 'data_product'

    _write_statements = config_yaml_dict['write']

    for i, item in enumerate(_write_statements):
        if _obj_type not in item:
            raise fdp_exc.UserConfigError(
                f"Expected '{_obj_type}' key in object '{item}'"
            )

        _params = {"name": item[_obj_type]}
        _results = None

        try:
            _results = fdp_reg_req.get(local_uri, (_obj_type,), params=_params)
            if not _results:
                raise AssertionError
        except (AssertionError, fdp_exc.RegistryAPICallError):
            # Object does not yet exist on the local registry
            pass

        _latest_version = fdp_ver.get_latest_version(_results)

        if 'version' in item.keys():
            # Check if 'version' is an incrementer definition
            # if     not then try using it as a hard set version
            try:
                _incrementer = fdp_ver.parse_incrementer(item['version'])
                _new_version = getattr(_latest_version, _incrementer)()
            except fdp_exc.UserConfigError:
                _new_version = semver.VersionInfo.parse(item['version'])
        else:       
            _new_version = _latest_version.bump_minor()
        _write_statements[i]['version'] = str(_new_version)
    
    _out_dict['write'] = _write_statements

    return _out_dict



        

        
            


        
    