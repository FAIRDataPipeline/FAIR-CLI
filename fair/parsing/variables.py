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

import git
import yaml
import semver
import logging

import fair.common as fdp_com
import fair.registry.versioning as fdp_ver
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.registry.requests as fdp_reg_req

def subst_cli_vars(
    job_dir: str,
    job_time: datetime.datetime,
    cfg: typing.Dict
    ) -> typing.Dict:
    """Lake configuration and substitute recognised FAIR CLI variables

    Parameters
    ----------
    job_dir : str
        location of code job directory (not to be confused with
        local FAIR project repository)

    job_time : datetime.datetime
        time of job commencement

    cfg : typing.Dict
        current config.yaml Dict

    Returns
    -------
    Dict
        new user configuration dictionary with substitutions
    """

    def _get_id(directory):
        try:
            return fdp_conf.get_current_user_orcid(directory)
        except fdp_exc.CLIConfigurationError:
            return fdp_conf.get_current_user_uuid(directory)

    if 'local_repo' not in cfg['run_metadata']:
        raise fdp_exc.InternalError(
            "Expected 'local_repo' definition in user configuration file"
        )

    _local_repo = cfg['run_metadata']['local_repo']

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

    # Quickest to substitute all in one go by editing config as a string
    _conf_str = yaml.dump(cfg)

    # Additional parser for formatted datetime
    _regex_dt_fmt = re.compile(r'\$\{\{\s*DATETIME\-[^}${\s]]+\s*\}\}')
    _regex_fmt = re.compile(r'\$\{\{\s*DATETIME\-([^}${\s]+)\s*\}\}')

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
    
    # Load the YAML (this also verifies the write was successful) and return it
    return yaml.safe_load(_conf_str)

def fill_block(cfg: typing.Dict, blocktype: str) -> bool:
    """Fill in all of elements of the blocks that are present in the local and global config files

    Parameters
    ----------
    cfg : typing.Dict
        user config yaml

    blocktype : str
        key name of block to fill in entries for ('read', 'write' or 'register')

    Returns
    -------
    bool
        whether any of the names of entries had wildcards in them
  
    """
    _logger = logging.getLogger("FAIRDataPipeline.Run")
    _logger.debug("Filling a '%s' block", blocktype)
    _modified_configs = 0
    _found_wildcard = False
    _modified_item = False
    _block_cfg = cfg[blocktype]

    for i, item in enumerate(_block_cfg):
        _modified_item = False

        if 'use' not in item:
            item['use'] = {}
            _modified_item = True

        if blocktype == 'register' and 'external_object' in item:
            if 'data_product' not in item:
                item['data_product'] = item['external_object']
                _modified_item = True

        if 'data_product' in item:
            if '*' in item['data_product']:
                _found_wildcard = True

            if 'namespace' in item:
                item['use']['namespace'] = item['namespace']
                item.pop('namespace')
                _modified_item = True

            if 'namespace' not in item['use']:
                if blocktype == "read":
                    item['use']['namespace'] = fdp_conf.input_namespace(cfg)
                elif blocktype == "write":
                    item['use']['namespace'] = fdp_conf.output_namespace(cfg)
                elif blocktype == "register":
                    item['use']['namespace'] = item['namespace_name']
                else:
                    raise fdp_exc.NotImplementedError(
                        f"Failed to understand block type '{blocktype}'"
                    )
                _modified_item = True

            if 'version' not in item['use']:
                if 'version' in item:
                    item['use']['version'] = item['version']
                    item.pop('version')
                else: # get from default
                    if blocktype == 'read':
                        item['use']['version'] = fdp_conf.read_version(cfg)
                    else: # 'write' or 'register'
                        item['use']['version'] = fdp_conf.write_version(cfg)
                _modified_item = True

            if 'data_product' not in item['use']:
                if '*' not in item['data_product']:
                    item['use']['data_product'] = item['data_product']
                    _modified_item = True

            if blocktype == 'register' and 'external_object' in item:
                item.pop('data_product', None)
                _modified_item = True

        if blocktype == 'write' or blocktype == 'register':
            if 'public' not in item:
                item['public'] = fdp_conf.is_public(cfg)
                _modified_item = True

        if _modified_item:
            _block_cfg[i] = item
            _modified_item = False
            _modified_configs += 1
            _logger.debug("New '%s' block", blocktype)

    if _modified_configs > 0:
        _logger.debug(
            "%d '%s' blocks modified", _modified_configs, blocktype
        )

    if _found_wildcard:
        _logger.debug(
            "Found wildcards in '%s' data products, not filling", blocktype
        )

    return _found_wildcard


def clean_config(cfg: typing.Dict) -> None:
    """Empty all of redundant elements of the config file

    Parameters
    ----------
    cfg : typing.Dict
        user config yaml
  
    """
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    _modified_configs = 0
    _modified_item = False

    cfg.pop('register', None)
    cfg['run_metadata'].pop('default_read_version', None)
    cfg['run_metadata'].pop('default_write_version', None)

    for _blocktype in ['read', 'write']:
        if _blocktype not in cfg:
            continue

        _block_cfg = cfg[_blocktype]
        if _blocktype == 'read':
            _match_ns = fdp_conf.input_namespace(cfg)
        else: # 'write'
            _match_ns = fdp_conf.output_namespace(cfg)
 

        for i, item in enumerate(_block_cfg):
            _modified_item = False

            for use_item in [*item['use']]:
                # Get rid of duplicates
                if use_item in item and item['use'][use_item] == item[use_item]:
                    item['use'].pop(use_item)
                    _modified_item = True
            
            if _blocktype == 'write' and item['public'] == fdp_conf.is_public(cfg):
                item.pop('public')
                _modified_item = True

            if item['use']['namespace'] == _match_ns:
                item['use'].pop('namespace')
                _modified_item = True

            if _modified_item:
                _block_cfg[i] = item
                _modified_item = False
                _modified_configs += 1
                _logger.debug("New '%s' block", _blocktype)

        if _modified_configs > 0:
            _logger.debug(
                "%d '%s' blocks modified", _modified_configs, _blocktype
            )

    # Quickest to substitute all in one go by editing config as a string
    _conf_str = yaml.dump(cfg)

    # Additional parser for formatted datetime
    _regex_fmt = re.compile(r'\$\{\{\s*([^}${\s]+)\s*\}\}')
    _fmt_res = _regex_fmt.findall(_conf_str)
    if _fmt_res != []:
        yaml.dump(cfg, open("4b.yaml", 'w'))

        _logger.debug(_conf_str)
        raise fdp_exc.UserConfigError(
            f"Found unresolved variables ({_fmt_res}) in cleaned working config"
        )


def pull_metadata(cfg: typing.Dict, blocktype: str) -> None:
    _logger = logging.getLogger("FAIRDataPipeline.Run")
    _logger.warning(
        "Cannot currently pull from remote registry"
    )

def pull_data(cfg: typing.Dict, blocktype: str = "read") -> None:
    if blocktype == "read":
        _logger = logging.getLogger("FAIRDataPipeline.Run")
        _logger.warning(
            "Cannot currently pull from remote registry"
        )

def register_to_read(cfg: typing.Dict) -> None:
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    if 'register' in cfg:
        if 'read' not in cfg:
            cfg['read'] = []
        _read_cfg = cfg['read']
        for item in cfg['register']:
            _readable = {}
            _new_item = False
            if 'use' in item:
                _readable['use'] = copy.deepcopy(item['use'])
            if 'external_object' in item:
                _readable['data_product'] = item['external_object']
                _new_item = True
            elif 'data_product' in item:
                _readable['data_product'] = item['data_product']
                _new_item = True
            else: # unknown
                _logger.debug(
                    f"Found registration for unknown item with keys {[*item]}"
                )
            _readable['use']['version'] = fdp_ver.undo_incrementer(_readable['use']['version'])
            
            if _new_item:
                _read_cfg.append(_readable)


def fill_versions(cfg: typing.Dict, blocktype: str) -> typing.Dict:
    """Fill in version numbers, returning Dict of failures if failed"""
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    _modified_configs = 0
    _block_cfg = cfg[blocktype]
    _failures = {'wildcards': [], 'exist': []}

    for i, item in enumerate(_block_cfg):
        # Only fill in versions for external objects and data products
        if 'external_object' not in item and 'data_product' not in item:
            continue

        _version = item['use']['version']
        if 'data_product' not in item['use']:
            if 'external_object' in item and '*' in item['external_object']:
                _name = item['external_object']
            elif 'data_product' in item and '*' in item['data_product']:
                _name = item['data_product']
            else:
                _logger.warning(f'Missing use:data_product in {item}')
            _failures['wildcards'].append(copy.deepcopy(item))
            continue

        _name = item['use']['data_product']
        _namespace = item['use']['namespace']
        _local_uri = fdp_conf.registry_url("local", cfg)
        if '${{' in _version:
            _results = fdp_reg_req.get(_local_uri, 'data_product', params = {
                'name': _name, 'namespace': _namespace
            })
        else:
            _results = fdp_reg_req.get(_local_uri, 'data_product', params = {
                'name': _name, 'namespace': _namespace, 'version': _version
            })

        try:
            _version = fdp_ver.get_correct_version(
                cfg, _results, blocktype != 'read', version = _version
            )
        except fdp_exc.UserConfigError as e:
            if blocktype == 'register':
                _logger.debug(
                    f"Already found this version ({e}), but may be identical"
                )
                _failures['exist'].append(copy.deepcopy(item))
            else:
                _logger.error(str(item))
                raise e
        
        if '${{' in _version:
            _logger.error(
                f"Found a version ({_version}) that needs resolving"
            )

        if str(_version) != item['use']['version']:
            item['use']['version'] = str(_version)
            _block_cfg[i] = item
            _modified_configs += 1
            _logger.debug("New '%s' block", blocktype)

    if _modified_configs > 0:
        _logger.debug(
            "%d '%s' blocks modified", _modified_configs, blocktype
        )
    
    if len(_failures['exist']) == 0 and len(_failures['wildcards']) == 0:
        return None
    else:
        return _failures
    


def expand_wildcards(cfg: typing.Dict, blocktype: str) -> typing.Dict:
    """Expand the wildcards in the config yaml from the local registry

    Parameters
    ----------
    cfg : typing.Dict
        user config yaml

    blocktype : str
        key name of block to fill in entries for ('read', 'write' or 'register')

    """
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    # Check if write block exists, if not return unaltered dict
    _local_uri = fdp_conf.registry_url("local", cfg)

    _modified_configs = 0
    _found_wildcard = False
    _modified_item = False
    _block_cfg = cfg[blocktype]

    for i, item in enumerate(_block_cfg):
        _modified_item = False

        _out_dict = copy.deepcopy(item)
        
    _out_dict['write'] = []
    _obj_type = 'data_product'

    _write_statements = cfg['write']

    for real_item in _write_statements:
        if _obj_type not in real_item:
            raise fdp_exc.UserConfigError(
                f"Expected '{_obj_type}' key in object '{real_item}'"
            )
        
        item = {_obj_type: real_item[_obj_type]}
        item['use'] = real_item['use'] if 'use' in real_item else {}

        if 'version' in real_item:
            item['use']['version'] = real_item['version']

        _params = {"name": item[_obj_type]}
        _results = None

        try:
            _results = fdp_reg_req.get(_local_uri, _obj_type, params=_params)
            if not _results:
                raise AssertionError
        except (AssertionError, fdp_exc.RegistryAPICallError):
            # Object does not yet exist on the local registry
            pass

        # Capture wildcards
        if '*' in _params['name']:
            _glob_item = copy.deepcopy(item)
            _latest_version = fdp_ver.get_latest_version()

            if 'version' in _glob_item['use']:
                _new_version = _bump_version(_glob_item['use']['version'], _latest_version)
            else:
                _new_version = fdp_ver.default_bump(_latest_version)

            _glob_item['use']['version'] = str(_new_version)
            _out_dict['write'].append(_glob_item)

        # Capture normal data and results of wildcard matches
        for result in _results:
            _new_item = copy.deepcopy(item)
            _latest_version = fdp_ver.get_latest_version([result])

            if 'version' in _new_item['use']:
                _new_version = _bump_version(_new_item['use']['version'], _latest_version)
            else:
                _new_version = fdp_ver.default_bump(_latest_version)

            # Check write product/version not already in registry
            _params['version'] = str(_new_version)
            _params['name'] = result['name']
            _write_product = fdp_reg_req.get(_local_uri, _obj_type, params=_params)
            if _write_product:
                raise fdp_exc.UserConfigError(
                    f"Data product '{_new_item[_obj_type]} v{str(_new_version)}' already exists in registry"
                )

            _new_item['use']['version'] = str(_new_version)
            _new_item[_obj_type] = result['name']

            _out_dict['write'].append(_new_item)

    return _out_dict

# TODO: This should probably be merged as one function with subst_versions
def get_read_version(
    local_uri: str,
    cfg: typing.Dict
) -> typing.Dict:
    # Check if read block exists, if not return unaltered dict
    if 'read' not in cfg:
        return cfg

    _out_dict = copy.deepcopy(cfg)
    _out_dict['read'] = []
    _obj_type = 'data_product'

    _read_statements = cfg['read']

    for real_item in _read_statements:
        if _obj_type not in real_item:
            raise fdp_exc.UserConfigError(
                f"Expected '{_obj_type}' key in object '{real_item}'"
            )
        
        item = {_obj_type: real_item[_obj_type]}
        item['use'] = real_item['use'] if 'use' in real_item else {}

        if 'version' in real_item:
            item['use']['version'] = real_item['version']

        _params = {"name": item[_obj_type]}
        if 'version' in item['use']:
            _params['version'] = item['use']['version']
        _results = None

        _results = fdp_reg_req.get(local_uri, _obj_type, params=_params)
        if not _results:
            raise fdp_exc.UserConfigError(
                f"Data product {_params} does not already exist in registry"
            )

        # Capture normal data and results of wildcard matches
        for result in _results:
            _new_item = copy.deepcopy(item)

            # Check read product/version not already in registry
            _params['name'] = result['name']
            _read_product = fdp_reg_req.get(local_uri, _obj_type, params=_params)
            if not _read_product:
                raise fdp_exc.UserConfigError(
                    f"Data product '{_new_item[_obj_type]} v{str(_new_version)}' does not already exist in registry"
                )
            _latest_version = fdp_ver.get_latest_version(_read_product)

            _new_item['use']['version'] = str(_latest_version)
            _new_item[_obj_type] = result['name']

            _out_dict['read'].append(_new_item)

    return _out_dict

def _bump_version(
    item_version: str,
    latest_version: semver.VersionInfo
) -> semver.VersionInfo:
    # Check if 'version' is an incrementer definition
    # if not then try using it as a hard set version

    try:
        _bump_func = fdp_ver.parse_incrementer(item_version)
        if _bump_func:
            _new_version = getattr(latest_version, _bump_func)()
        else:
            _new_version = latest_version
    except fdp_exc.UserConfigError:
        _new_version = semver.VersionInfo.parse(item_version)

    return _new_version
