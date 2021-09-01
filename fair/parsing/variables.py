#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Variable Insert
===============

Substitute CLI variables in the `config.yaml` file

Contents
========

Functions
-------

    subst_cli_vars - substitute CLI variables
    subst_versions - substitute version increment identifiers

"""

__date__ = "2021-08-16"

import datetime
import copy
import typing
import collections.abc
import os
import re

import git
import yaml
import semver

import fair.common as fdp_com
import fair.registry.versioning as fdp_ver
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.registry.requests as fdp_reg_req

def subst_cli_vars(
    local_uri: str,
    job_dir: str,
    job_time: datetime.datetime,
    config_yaml: str
    ) -> typing.Dict:
    """Load configuration and substitute recognised FAIR CLI variables

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry

    job_dir : str
        location of code job directory (not to be confused with
        local FAIR project repository)

    job_time : datetime.datetime
        time of job commencement

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

    def _get_id(directory):
        try:
            return fdp_conf.get_current_user_orcid(directory)
        except fdp_exc.CLIConfigurationError:
            return fdp_conf.get_current_user_uuid(directory)

    _yaml_dict = yaml.safe_load(open(config_yaml))

    if 'local_repo' not in _yaml_dict['run_metadata']:
        raise fdp_exc.InternalError(
            "Expected 'local_repo' definition in user configuration file"
        )

    _local_repo = _yaml_dict['run_metadata']['local_repo']

    _fair_head = fdp_com.find_fair_root(_local_repo)

    def _tag_check(*args, **kwargs):
        _repo = git.Repo(fdp_conf.get_session_git_repo(_local_repo))
        if len(_repo.tags) < 1:
            fdp_exc.UserConfigError(
                "Cannot use GIT_TAG variable, no git tags found."
            )
        return _repo.tags[-1].name

    _substitutes: collections.abc.Mapping = {
        "DATE": lambda : job_time.strftime("%Y%m%d"),
        "DATETIME": lambda : job_time.strftime("%Y-%m-%dT%H:%M:%S%Z"),
        "USER": lambda : fdp_conf.get_current_user_name(_local_repo),
        "USER_ID": lambda : _get_id(job_dir),
        "REPO_DIR": lambda : _fair_head,
        "CONFIG_DIR": lambda : job_dir,
        "SOURCE_CONFIG": lambda : config_yaml,
        "GIT_BRANCH": lambda : git.Repo(
                fdp_conf.get_session_git_repo(_local_repo)
            ).active_branch.name,
        "GIT_REMOTE": lambda : git.Repo(
            fdp_conf.get_session_git_repo(_local_repo)
        ).refs[fdp_conf.get_session_git_remote(_local_repo)].url,
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
        raise fdp_exc.UserConfigError(
            "Failed to parse formatted datetime variable"
        )

    if _dt_fmt_res:
        for i, _ in enumerate(_dt_fmt_res):
            _time_str = job_time.strftime(_fmt_res[i].strip())
            _conf_str = _conf_str.replace(_dt_fmt_res[i], _time_str)

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

    # Parse 'read' block and get data product versions
    _user_conf = get_read_version(local_uri, _user_conf)

    return _user_conf


def subst_versions(local_uri: str, config_yaml_dict: typing.Dict) -> typing.Dict:
    # dynamic versionables only present in write statement
    if 'write' not in config_yaml_dict:
        return config_yaml_dict

    _out_dict = copy.deepcopy(config_yaml_dict)
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
            _results = fdp_reg_req.get(local_uri, _obj_type, params=_params)
            if not _results:
                raise AssertionError
        except (AssertionError, fdp_exc.RegistryAPICallError):
            # Object does not yet exist on the local registry
            pass

        _latest_version = fdp_ver.get_latest_version(_results)

        # Check for whether format is ['use']['version'] or just ['version']
        if 'use' in item and 'version' in item['use']:
            _new_version = _bump_version(item['use']['version'], _latest_version)
        elif 'version' in item:
            _new_version = _bump_version(item['version'], _latest_version)
            # Remove version entry and add back as use: version later
            del item['version']
        else:
            _new_version = fdp_ver.default_bump(_latest_version)

        # Check write product/version not already in registry
        _params['version'] = str(_new_version)
        _write_product = fdp_reg_req.get(local_uri, _obj_type, params=_params)
        if _write_product:
            raise fdp_exc.UserConfigError(
                f"Data product '{item[_obj_type]} v{str(_new_version)}' already exists in registry"
            )

        if 'use' not in _write_statements[i]:
            _write_statements[i]['use'] = {}
        _write_statements[i]['use']['version'] = str(_new_version)

    _out_dict['write'] = _write_statements

    return _out_dict

# TODO: This should probably be merged as one function with subst_versions
def get_read_version(
    local_uri: str,
    config_yaml_dict: typing.Dict
) -> typing.Dict:
    # Check if read block exists, if not return unaltered dict
    if 'read' not in config_yaml_dict:
        return config_yaml_dict

    _out_dict = copy.deepcopy(config_yaml_dict)
    _obj_type = 'data_product'

    _read_statements = config_yaml_dict['read']

    for i, item in enumerate(_read_statements):
        if _obj_type not in item:
            raise fdp_exc.UserConfigError(
                f"Expected '{_obj_type}' key in object '{item}'"
            )

        _params = {"name": item[_obj_type]}

        # If version is specified, add as parameter for API call
        if 'use' in item and 'version' in item['use']:
            _params['version'] = item['use']['version']
        elif 'version' in item:
            _params['version'] = item['version']
            # Remove version entry and add back as use: version later
            del item['version']

        _results = None

        _results = fdp_reg_req.get(local_uri, _obj_type, params=_params)

        if not _results:
            if 'version' in _params:
                _msg = f"'{item[_obj_type]} v{_params['version']}' does not exist in local registry"
            else:
                _msg = f"'{item[_obj_type]}' does not exist in local registry"

            raise fdp_exc.RegistryAPICallError(
                _msg,
                error_code = 400
            )


        _product_version = fdp_ver.get_latest_version(_results)

        if 'use' not in _read_statements[i]:
            _read_statements[i]['use'] = {}
        _read_statements[i]['use']['version'] = str(_product_version)

    _out_dict['read'] = _read_statements

    return _out_dict

def _bump_version(
    item_version: str,
    latest_version: semver.VersionInfo
) -> semver.VersionInfo:
    # Check if 'version' is an incrementer definition
    # if not then try using it as a hard set version

    try:
        _incrementer = fdp_ver.parse_incrementer(item_version)
        _new_version = getattr(latest_version, _incrementer)()
    except fdp_exc.UserConfigError:
        _new_version = semver.VersionInfo.parse(item_version)

    return _new_version
