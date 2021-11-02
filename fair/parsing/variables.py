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
import re
import logging
import os
import git

import fair.common as fdp_com
import fair.registry.versioning as fdp_ver
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store

logger = logging.getLogger("FAIRDataPipeline.Parsing")

def subst_cli_vars(
    job_dir: str,
    local_repo: str,
    job_time: datetime.datetime,
    user_config_str: str
    ) -> typing.Dict:
    """Lake configuration and substitute recognised FAIR CLI variables

    Parameters
    ----------
    job_dir : str
        location of code job directory (not to be confused with
        local FAIR project repository)

    local_repo : str
        location of FAIR repository

    job_time : datetime.datetime
        time of job commencement

    user_config_str : str
        user configurations from config.yaml file

    Returns
    -------
    Dict
        new user configuration dictionary with substitutions
    """
    logger.debug("Searching for CLI variables")

    def _get_id(directory):
        try:
            return fdp_conf.get_current_user_uri(directory)
        except fdp_exc.CLIConfigurationError:
            return fdp_conf.get_current_user_uuid(directory)

    def _tag_check(*args, **kwargs):
        _repo = git.Repo(fdp_conf.local_git_repo(local_repo))
        if len(_repo.tags) < 1:
            fdp_exc.UserConfigError(
                "Cannot use GIT_TAG variable, no git tags found."
            )
        return _repo.tags[-1].name

    _substitutes: collections.abc.Mapping = {
        "DATE": lambda : job_time.strftime("%Y%m%d"),
        "DATETIME": lambda : job_time.strftime("%Y-%m-%dT%H:%M:%S%Z"),
        "USER": lambda : fdp_conf.get_current_user_name(local_repo),
        "USER_ID": lambda : _get_id(job_dir),
        "REPO_DIR": lambda : local_repo,
        "CONFIG_DIR": lambda : job_dir + os.path.sep,
        "LOCAL_TOKEN": lambda : fdp_req.local_token(),
        "GIT_BRANCH": lambda : git.Repo(
                fdp_conf.local_git_repo(local_repo)
            ).active_branch.name,
        "GIT_REMOTE": lambda : fdp_conf.remote_git_repo(local_repo),
        "GIT_TAG": _tag_check,
    }

    # Additional parser for formatted datetime
    _regex_dt_fmt = re.compile(r'\$\{\{\s*DATETIME\-[^}${\s]]+\s*\}\}')
    _regex_fmt = re.compile(r'\$\{\{\s*DATETIME\-([^}${\s]+)\s*\}\}')

    _dt_fmt_res = _regex_dt_fmt.findall(user_config_str)
    _fmt_res = _regex_fmt.findall(user_config_str)

    logging.debug(
        "Found datetime substitutions: %s %s",
        _dt_fmt_res or "",
        _fmt_res or ""
    )

    # The two regex searches should match lengths
    if len(_dt_fmt_res) != len(_fmt_res):
        raise fdp_exc.UserConfigError(
            "Failed to parse formatted datetime variable"
        )

    if _dt_fmt_res:
        for i, _ in enumerate(_dt_fmt_res):
            _time_str = job_time.strftime(_fmt_res[i].strip())
            user_config_str = user_config_str.replace(_dt_fmt_res[i], _time_str)

    _regex_dict = {
        var: r'\$\{\{\s*'+f'{var}'+r'\s*\}\}'
        for var in _substitutes
    }

    # Perform string substitutions
    for var, subst in _regex_dict.items():
        # Only execute functions in var substitutions that are required
        if re.findall(subst, user_config_str):
            _value = _substitutes[var]()
            if not _value:
                raise fdp_exc.InternalError(
                    f"Expected value for substitution of '{var}' but returned None",
                )
            user_config_str = re.sub(subst, str(_value), user_config_str)
            logger.debug("Substituting %s: %s", var, str(_value))
    
    # Load the YAML (this also verifies the write was successful) and return it
    return user_config_str


def pull_metadata(cfg: typing.Dict, blocktype: str) -> None:
    logger.info(
        "Not currently pulling from remote registry"
    )

def pull_data(cfg: typing.Dict, blocktype: str = "read") -> None:
    if blocktype == "read":
        logger.info(
            "Not currently pulling from remote registry"
        )


def register_to_read(register_block: typing.Dict) -> typing.Dict:
    """Construct 'read' block entries from 'register' block entries
    
    Parameters
    ----------
    register_block : typing.Dict
        register type entries within a config.yaml
    
    Returns
    -------
    typing.Dict
        new read entries extract from register statements
    """
    _read_block: typing.List[typing.Dict] = []

    for item in register_block:
        _readable = {}
        if 'use' in item:
            _readable['use'] = copy.deepcopy(item['use'])
        if 'external_object' in item:
            _readable['data_product'] = item['external_object']
        elif 'data_product' in item:
            _readable['data_product'] = item['data_product']
        elif 'namespace' in item:
            fdp_store.store_namespace(**item)
        else: # unknown
            raise fdp_exc.UserConfigError(
                f"Found registration for unknown item with keys {[*item]}"
            )
        _readable['use']['version'] = fdp_ver.undo_incrementer(_readable['use']['version'])
        
        _read_block.append(_readable)

    return _read_block
