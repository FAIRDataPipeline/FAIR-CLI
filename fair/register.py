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
    local_uri: str,
    repo_dir: str,
    write_data_store: str,
    user_config_register: typing.Dict,
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

    for entry in user_config_register:
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
                "Expected either 'data_product' or 'external_object' in 'register' item"
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
                "Expected either 'unique_name' or 'identifier' in 'register' item"
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
            write_data_store, _namespace, _name
        )

        # Check if the object is already present on the local registry
        _is_present = fdp_store.check_if_object_exists(
            local_uri=local_uri,
            file_loc=_temp_data_file,
            obj_type=_obj_type,
            search_data=_search_data
        )

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
                results_list=_results,
                free_write=True,
                version=entry['use']['version']
            )
            _logger.debug("Found results for %s", str(_results))
        else:
            _user_version = fdp_ver.get_correct_version(
                results_list=None,
                free_write=True,
                version=entry['use']['version']
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

        data=copy.deepcopy(entry)

        data['namespace_name'] = entry['use']['namespace']

        _file_url = fdp_store.store_data_file(
            uri=local_uri,
            repo_dir=repo_dir,
            data=data,
            local_file=_local_file,
            write_data_store=write_data_store,
            public=_public
        )

        _stored_objects.append(_file_url)
    
    return _stored_objects
