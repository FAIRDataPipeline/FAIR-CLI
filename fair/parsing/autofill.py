#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Autofill User Config
=====================

In the case of partial user configurations these methods will append any
additional metadata from the local/global CLI config

"""

__date__ = "2021-09-03"

import typing
import copy

import fair.configuration as fdp_conf


def append_missing_info(config_dict: typing.Dict, repo_dir: str) -> typing.Dict:
    """Append any missing metadata to the configuration

    Parameters
    ----------
    config_dict : typing.Dict
        user configuration dictionary
    repo_dir : str
        local FAIR repository

    Returns
    -------
    typing.Dict
        user configuration after autofill
    """

    _out_dict = copy.deepcopy(config_dict)

    if 'run_metadata' not in config_dict:
        _out_dict['run_metadata'] = {}

    _metadata = config_dict['run_metadata']

    if 'local_data_registry_url' not in _metadata:
        _metadata['local_data_registry_url'] = fdp_conf.get_local_uri()

    if 'remote_data_registry_url' not in _metadata:
        _metadata['remote_data_registry_url'] = fdp_conf.get_remote_uri(repo_dir)

    if 'write_data_store' not in _metadata:
        _metadata['write_data_store'] = fdp_conf.get_local_uri()

    if 'local_repo' not in _metadata:
        _metadata['local_repo'] = repo_dir
    
    if 'remote_repo' not in _metadata:
        _metadata['remote_repo'] = fdp_conf.get_session_git_remote(repo_dir)

    if 'default_input_namespace' not in _metadata:
        _metadata['default_input_namespace'] = fdp_conf.get_input_namespace(repo_dir)

    if 'default_output_namespace' not in _metadata:
        _metadata['default_output_namespace'] = fdp_conf.get_output_namespace(repo_dir)

    _out_dict['run_metadata'] = _metadata

    return _out_dict
