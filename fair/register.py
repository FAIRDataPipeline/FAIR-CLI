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
import logging
import os
import platform
import shutil
import typing
import urllib.parse

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.sync as fdp_sync
import fair.registry.versioning as fdp_ver
from fair.registry import SEARCH_KEYS

logger = logging.getLogger("FAIRDataPipeline.Register")


def convert_key_value_to_id(
    uri: str, obj_type: str, value: str, token: str
) -> int:
    """Converts a config key value to the relevant URL on the local registry

    Parameters
    ----------
    uri: str
        registry endpoint to use for obtaining URL
    obj_type : str
        object type
    value : str
        search term to use
    token: str
        registry access token

    Returns
    -------
    int
        ID on the local registry matching the entry
    """
    _params = {SEARCH_KEYS[obj_type]: value}
    _result = fdp_req.get(uri, obj_type, token, params=_params)
    if not _result:
        raise fdp_exc.RegistryError(
            f"Failed to obtain result for '{obj_type}' with parameters '{_params}'"
        )
    return fdp_req.get_obj_id_from_url(_result[0]["url"])


# flake8: noqa: C901
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
        "public",
    ]

    _stored_objects: typing.List[str] = []

    for entry in user_config_register:
        for key in _expected_keys:
            if key not in entry and key not in entry["use"]:
                raise fdp_exc.UserConfigError(
                    f"Expected key '{key}' in 'register' item"
                )

        _identifier: str = entry["identifier"] if "identifier" in entry else ""
        _unique_name: str = (
            entry["unique_name"] if "unique_name" in entry else ""
        )

        _data_product = None
        _external_object = None
        _is_present = None

        _search_data = {}

        if "data_product" in entry:
            _data_product: str = entry["use"]["data_product"]
        elif "external_object" in entry:
            _external_object: str = entry["use"]["data_product"]

        if not _external_object and not _data_product:
            raise fdp_exc.UserConfigError(
                "Expected either 'data_product' or 'external_object' in 'register' item"
            )

        elif _external_object and _data_product:
            raise fdp_exc.UserConfigError(
                "Only one type may be provided (data_product/external_object)"
            )
        elif _external_object:
            _name = entry["use"]["data_product"]
            _obj_type = "external_object"
            # TODO: This doesn't work because of a mismatch with spaces in alternate_identifier, perhaps?
            if "unique_name" in entry and "alternate_identifier_type" in entry:
                #                _search_data['alternate_identifier'] = entry['unique_name']
                _search_data["alternate_identifier_type"] = entry[
                    "alternate_identifier_type"
                ]
            elif "identifier" in entry:
                _search_data["identifier"] = entry["identifier"]
            else:
                raise fdp_exc.UserConfigError(
                    "Expected either 'identifier', or 'unique_name' and "
                    f"'alternate_identifier_type' in external object '{_name}'"
                )
            try:
                _data_product_id = convert_key_value_to_id(
                    local_uri,
                    "data_product",
                    entry["use"]["data_product"],
                    fdp_req.local_token(),
                )
                _search_data["data_product"] = _data_product_id
            except fdp_exc.RegistryError:
                _is_present = "absent"

        else:
            _name = entry["use"]["data_product"]
            _obj_type = "data_product"
            _search_data = {"name": _name}

        _search_data["version"] = entry["use"]["version"]
        _namespace = entry["use"]["namespace"]

        if not _identifier and not _unique_name:
            raise fdp_exc.UserConfigError(
                "Expected either 'unique_name' or 'identifier' in 'register' item"
            )

        elif _identifier and _unique_name:
            raise fdp_exc.UserConfigError(
                "Only one unique identifier may be provided (doi/unique_name)"
            )

        if "cache" in entry:
            _temp_data_file = entry["cache"]
        else:
            _local_parsed = urllib.parse.urlparse(local_uri)
            _local_url = f"{_local_parsed.scheme}://{_local_parsed.netloc}"
            _temp_data_file = fdp_sync.download_from_registry(
                _local_url, root=entry["root"], path=entry["path"]
            )

        # Need to fix the path for Windows
        if platform.system() == "Windows":
            _name = _name.replace("/", os.path.sep)

        _local_dir = os.path.join(write_data_store, _namespace, _name)

        # Check if the object is already present on the local registry
        _is_present = fdp_store.check_if_object_exists(
            local_uri=local_uri,
            file_loc=_temp_data_file,
            token=fdp_req.local_token(),
            obj_type=_obj_type,
            search_data=_search_data,
        )

        # Hash matched version already present
        if _is_present == "hash_match":
            logger.debug(
                "Skipping item '%s' as a hash matched entry is already"
                " present with this name, deleting temporary data file",
                _name,
            )
            os.remove(_temp_data_file)
            continue

        # Item found but not hash matched retrieve a version number
        elif _is_present != "absent":
            _results = _is_present
            _user_version = fdp_ver.get_correct_version(
                results_list=_results,
                free_write=True,
                version=entry["use"]["version"],
            )
            logger.debug("Found existing results for %s", _results)
        else:
            _user_version = fdp_ver.get_correct_version(
                results_list=None,
                free_write=True,
                version=entry["use"]["version"],
            )
            logger.debug("No existing results found for %s", _search_data)

        # Create object location directory, ignoring if already present
        # as multiple version files can exist
        os.makedirs(_local_dir, exist_ok=True)

        _local_file = os.path.join(
            _local_dir, f"{_user_version}.{entry['file_type']}"
        )
        # Copy the temporary file into the data store
        # then remove temporary file to save space
        logger.debug("Saving data file to '%s'", _local_file)
        shutil.copy(_temp_data_file, _local_file)

        os.remove(_temp_data_file)

        if "public" in entry:
            _public = entry["public"]

        data = copy.deepcopy(entry)

        data["namespace_name"] = entry["use"]["namespace"]
        if "primary" in entry and _external_object:
            data["primary"] = entry["primary"]

        _file_url = fdp_store.store_data_file(
            uri=local_uri,
            repo_dir=repo_dir,
            token=fdp_req.local_token(),
            data=data,
            local_file=_local_file,
            write_data_store=write_data_store,
            public=_public,
        )

        _stored_objects.append(_file_url)

    return _stored_objects
