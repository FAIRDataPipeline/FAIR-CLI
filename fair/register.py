#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Register Key Methods
====================

Substitute/fetch data relating to 'register' entries in the
`config.yaml` user config


Contents
========

Functions
-------

"""

__date__ = "2021-08-16"

import copy
import os
import typing
import urllib.parse
import shutil
import logging
import requests

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.versioning as fdp_ver
import fair.configuration as fdp_conf


def fetch_registrations(
    repo_dir: str,
    cfg: typing.Dict,
    ) -> typing.Dict:
    """As part of running 'pull' fetch all items listed under 'register'

    Parameters
    ----------
    repo_dir : str
        FAIR project repository
    cfg : typing.Dict
        Dict containing working config       

    Returns
    -------
    typing.List[str]
        list of registered object URLs
    """

    _local_uri = fdp_conf.registry_url("local", cfg)

    if 'register' not in cfg:
        return cfg

    _registrations = cfg['register']

    _expected_keys = [
        "root",
        "path",
        "file_type",
        "primary",
        "version",
        "public"
    ]
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    _stored_objects: typing.List[str] = []

    for entry in _registrations:
        for key in _expected_keys:
            if key not in entry and key not in entry['use']:
                raise fdp_exc.UserConfigError(
                    f"Expected key '{key}' in 'register' item"
                )
        
        _identifier: str = entry['identifier'] if 'identifier' in entry else ''
        _unique_name: str = entry['unique_name'] if 'unique_name' in entry else ''

        _data_product = None
        _external_object = None

        _search_data = {}

        if 'data_product' in entry:
            _data_product: str = entry['use']['data_product']
        elif 'external_object' in entry:
            _external_object: str = entry['use']['data_product']

        if not _external_object and not _data_product:
            raise fdp_exc.UserConfigError(
                f"Expected either 'data_product' or "
                "'external_object' in 'register' item"
            )
        elif _external_object and _data_product:
            raise fdp_exc.UserConfigError(
                "Only one type may be provided (data_product/external_object)"
            )
        elif _external_object:
            _name = entry['use']['data_product']
            _obj_type = 'external_object'
            #TODO: This doesn't work because of a mismatch with spaces in alternate_identifier, perhaps?
            if 'unique_name' in entry and 'alternate_identifier_type' in entry:
#                _search_data['alternate_identifier'] = entry['unique_name']
                _search_data['alternate_identifier_type'] = entry['alternate_identifier_type']
            elif 'identifier' in entry:
                _search_data['identifier'] = entry['identifier']
            else:
                raise fdp_exc.UserConfigError(
                    "Expected either 'identifier', or 'unique_name' and "
                    f"'alternate_identifier_type' in external object '{_name}'"
                )
            _search_data['namespace'] = entry['use']['namespace']
            _search_data['data_product'] = entry['use']['data_product']
        else:
            _name = entry['use']['data_product']
            _obj_type = 'data_product'
            _search_data = {"name": _name}

        _search_data['version'] = entry['use']['version']
        _namespace = entry['use']['namespace']

        if not _identifier and not _unique_name:
            raise fdp_exc.UserConfigError(
                f"Expected either 'unique_name' or 'identifier' in 'register' item"
            )
        elif _identifier and _unique_name:
            raise fdp_exc.UserConfigError(
                "Only one unique identifier may be provided (doi/unique_name)"
            )
        
        if 'cache' in entry: # Do we have a local cache already?
            _temp_data_file = entry['cache']
        else: # Need to download it
            _root, _path = entry["root"], entry['path']

            # Encode the path first
            _path = urllib.parse.quote_plus(_path)
            _url = f"{_root}{_path}"
            try:
                _temp_data_file = fdp_req.download_file(_url)
            except requests.HTTPError as r_in:
                raise fdp_exc.UserConfigError(
                    f"Failed to fetch item '{_url}' with exit code "
                    f"{r_in.response}"
                )

        # Need to fix the path for Windows
        if os.path.sep != '/':
            _name = _name.replace('/', os.path.sep)

        _local_dir = os.path.join(
            fdp_conf.write_data_store(cfg), _namespace, _name
        )

        # Check if the object is already present on the local registry
        _is_present = fdp_store.check_if_object_exists(
            cfg, _temp_data_file, _obj_type, _search_data
        )

        _logger.debug(_temp_data_file)
        _logger.debug(str(_is_present))

        # Hash matched version already present
        if _is_present == "hash_match":
            _logger.debug(
                f"Skipping item '{_name}' as a hash matched entry is already"
                " present with this name"
            )
            os.remove(_temp_data_file)
            continue
        
        # Item found but not hash matched retrieve a version number
        elif _is_present != "absent":
            _results = _is_present
            _user_version = fdp_ver.get_correct_version(
                cfg, _results, True, entry['use']['version']
            )
            _logger.debug("Found results for %s", str(_results))
        else:
            _user_version = fdp_ver.get_correct_version(
                cfg, None, True, entry['use']['version']
            )
            _logger.debug("Found nothing for %s", str(_search_data))
        
        # Create object location directory, ignoring if already present
        # as multiple version files can exist
        os.makedirs(_local_dir, exist_ok=True)

        _local_file = os.path.join(
            _local_dir,
            f"{_user_version}.{entry['file_type']}"
        )

        # Copy the temporary file into the data store
        # then remove temporary file to save space
        shutil.copy(_temp_data_file, _local_file)
        if 'cache' not in entry:
            os.remove(_temp_data_file)

        if 'public' in entry:
            _public = entry['public']

        _file_url = fdp_store.store_data_file(
            uri=_local_uri,
            repo_dir=repo_dir,
            data=copy.deepcopy(entry),
            local_file=_local_file,
            cfg=cfg,
            public=_public
        )

        _stored_objects.append(_file_url)
    
#    if 'read' in cfg:
#        cfg['read'] += _stored_objects
#    else:
#        cfg['read'] = _stored_objects
            
def subst_registrations(local_uri: str, input_config: typing.Dict):
    """As part of 'run' substitute listings in config for working configuration

    Parameters
    ----------
    uri: str
        endpoint of the local registry
    input_config : typing.Dict
        input user configuration

    Returns
    -------
    typing.Dict
        new dictionary with 'register' removed and replaced with local objects
    """
    _new_cfg = input_config.copy()

    # If there is no register key then abort substitutions
    if 'register' not in _new_cfg:
        return _new_cfg

    # Remove register as it will be replaced
    del _new_cfg['register']

    if 'read' not in _new_cfg:
        _new_cfg['read'] = []

    _registrations = input_config['register']

    for reg in _registrations:
        if not any(t in reg for t in ['data_product', 'external_object']):
            raise fdp_exc.UserConfigError(
                f"Expected either 'data_product' or "
                "'external_object' in 'register' item"
            )
        if 'version' not in reg:
            raise fdp_exc.InternalError(

            )

        # If an external object fetch the relevant data_product first
        if 'external_object' in reg:
            # TODO Not enough info to make unique
            _ext_data = {
                "version": reg['version'],
                "title": reg['title']
            }
            _results = fdp_req.get(
                local_uri,
                'external_object',
                params=_ext_data
            )
            
            if len(_results) > 1 or not _results:
                raise fdp_exc.InternalError(
                    f"Expected one external_object for '{_ext_data}', "
                    f"got {len(_results)}"
                )

            if 'data_product' not in _results[0]:
                raise fdp_exc.RegistryError(
                    f"Expected external_object '{reg['external_object']}' "
                    "to have a data_product"
                )
            _data_product_url = _results[0]['data_product']
            _results[0] = fdp_req.url_get(_data_product_url)
        else: # data product
            # TODO Not enough info to make unique
            _data = {
                "version": reg['version'],
                "name": reg['data_product']
            }

            if 'namespace_name' in reg:
                _data['namespace'] = reg['namespace_name'] 

            _results = fdp_req.get(
                local_uri,
                'data_product',
                params=_data
            )

            if len(_results) > 1 or not _results:
                raise fdp_exc.InternalError(
                    f"Expected one result for {_data}, got {len(_results)}"
                )

        _namespace_url = _results[0]['namespace']
        _namespace_data = fdp_req.url_get(_namespace_url)

        _object_data = {
            'data_product': _results[0]['name'],
            'use': {
                'version': _results[0]['version'],
                'namespace': _namespace_data['name'],
            }
        }

    _new_cfg['read'].append(_object_data)
        
    return _new_cfg

