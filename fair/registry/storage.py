#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Storage
================

Methods that add objects to the data registry.

Contents
========

Functions
---------

    get_write_storage - retrieve/create root data store
    store_user - create author entry for the current user
    create_file_type - create a file type entry
    store_working_config - create locations/objects for a working config

"""

__date__ = "2021-07-02"

import yaml
import os
import hashlib
import urllib.parse

import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.identifiers as fdp_id
import fair.registry.file_types as fdp_file


def get_write_storage(uri: str, work_cfg_yml: str) -> str:
    """Construct storage root if it does not exist

    Parameters
    ----------
    uri : str
        end point of the RestAPI
    work_cfg_yml : str
        path of the working config file

    Returns
    -------
    str
        URI of the created/retrieved storage root

    Raises
    ------
    fdp_exc.UserConfigError
        If 'write_data_store' not present in the working config
    """

    _work_cfg = yaml.safe_load(open(work_cfg_yml))
    _work_cfg_meta = _work_cfg["run_metadata"]

    if "write_data_store" not in _work_cfg_meta:
        raise fdp_exc.UserConfigError(
            "Cannot create a storage location on the registry for writing,"
            " no local file location specified."
        )

    # Convert local file path to a valid data store path
    _write_store_root = f"file://{_work_cfg_meta['write_data_store']}/"

    # Check if the data store already exists by querying for it
    _search_root = fdp_req.get(
        uri, ("storage_root",), params={"root": _write_store_root}
    )

    # If the data store already exists just return the URI else create it
    # and then do the same
    if not _search_root:
        _post_data = {"root": _write_store_root, "local": True}
        _storage_root = fdp_req.post(uri, ("storage_root",), data=_post_data)
        return _storage_root["url"]
    else:
        return _search_root[0]["url"]


def store_user(repo_dir: str, uri: str) -> str:
    """Creates an Author entry for the user if one does not exist

    Parameters
    ----------
    uri : str
        registry RestAPI endpoint

    Returns
    -------
    str
        URI for created author
    """
    _user = fdp_conf.get_current_user_name(repo_dir)
    _data = {}
    if len(_user) > 1:
        _data['name'] = ' '.join(_user)
    else:
        _data['name'] = _user[0]

    try:
        _orcid = fdp_conf.get_current_user_orcid(repo_dir)
        _orcid = urllib.parse.urljoin(fdp_id.ORCID_URL, _orcid)
        _data["identifier"] = _orcid
        return fdp_req.post_else_get(
            uri, ("author",), data=_data, params={"identifier": _orcid}
        )
    except fdp_exc.CLIConfigurationError:
        _uuid = fdp_conf.get_current_user_uuid(repo_dir)
        _data['uuid'] = _uuid
        return fdp_req.post_else_get(
            uri, ("author",), data=_data, params={"uuid": _uuid}
        )


def create_file_type(uri: str, extension: str) -> str:
    """Creates a new file type on the registry

    Parameters
    ----------
    uri : str
        registry RestAPI end point
    ftype : str
        file extension

    Returns
    -------
    str
        URI for created file type
    """
    _name = fdp_file.FILE_TYPES[extension]
    return fdp_req.post_else_get(
        uri, ("file_type",), data={"name": _name, "extension": extension}
    )


def store_working_config(repo_dir: str, uri: str, work_cfg_yml: str) -> str:
    """Construct a storage location and object for the working config

    Parameters
    ----------
    repo_dir : str
        local FAIR repository
    uri : str
        RestAPI end point
    work_cfg_yml : str
        location of working config yaml

    Returns
    -------
    str
        new URI for the created object

    Raises
    ------
    fair.exceptions.RegistryAPICallError
        if bad status code returned from the registry
    """
    _root_store = get_write_storage(uri, work_cfg_yml)

    _work_cfg = yaml.safe_load(open(work_cfg_yml))
    _work_cfg_data_store = _work_cfg["run_metadata"]["write_data_store"]
    _rel_path = os.path.relpath(work_cfg_yml, _work_cfg_data_store)
    _time_stamp_dir = os.path.basename(os.path.dirname(work_cfg_yml))

    # Construct hash from config contents and time stamp
    _hashable = open(work_cfg_yml).read() + _time_stamp_dir

    _hash = hashlib.sha1(_hashable.encode("utf-8")).hexdigest()

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": False,
        "hash": _hash,
    }

    _post_store_loc = fdp_req.post(
        uri,
        ("storage_location",),
        data=_storage_loc_data
    )

    _user = store_user(repo_dir, uri)

    _yaml_type = create_file_type(uri, "yaml")

    _desc = f"Working configuration file for timestamp {_time_stamp_dir}"
    _object_data = {
        "description": _desc,
        "storage_location": _post_store_loc["url"],
        "file_type": _yaml_type,
        "authors": [_user],
    }

    return fdp_req.post_else_get(
        uri, ("object",), data=_object_data, params={"description": _desc}
    )


def store_working_script(
    repo_dir: str,
    uri: str,
    script_path: str,
    working_config: str
    ) -> str:
    """Construct a storage location and object for the CLI run script

    Parameters
    ----------
    repo_dir : str
        local FAIR repository
    uri : str
        RestAPI end point
    script_path : str
        location of working CLI run script
    data_store : str
        data store path

    Returns
    -------
    str
        new URI for the created object

    Raises
    ------
    fair.exceptions.RegistryAPICallError
        if bad status code returned from the registry
    """
    _work_cfg = yaml.safe_load(open(working_config))
    _root_store = get_write_storage(uri, working_config)
    _data_store = _work_cfg["run_metadata"]["write_data_store"]

    _rel_path = os.path.relpath(script_path, _data_store)

    _time_stamp_dir = os.path.basename(os.path.dirname(working_config))

    # Construct hash from config contents and time 
    _hashable = open(script_path).read() + _time_stamp_dir

    _hash = hashlib.sha1(_hashable.encode("utf-8")).hexdigest()

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": False,
        "hash": _hash,
    }

    _post_store_loc = fdp_req.post(
        uri,
        ("storage_location",),
        data=_storage_loc_data,
    )

    _user = store_user(repo_dir, uri)

    _shell_script_type = create_file_type(
        uri, "Executable script", "sh"
    )

    _time_stamp_dir = os.path.basename(os.path.dirname(script_path))
    _desc = f"Run script for timestamp {_time_stamp_dir}"
    _object_data = {
        "description": _desc,
        "storage_location": _post_store_loc["url"],
        "file_type": _shell_script_type,
        "authors": [_user],
    }

    return fdp_req.post_else_get(
        uri, ("object",), data=_object_data, params={"description": _desc}
    )


def get_storage_root_obj_address(remote_uri: str, remote_token: str, address_str: str) -> str:
    """Retrieve the RestAPI URL for a given storage location on the registry

    Parameters
    ----------
    remote_uri : str
        endpoint of remote registry
    remote_token : str
        token for accessing remote registry
    address_str : str
        path of the storage location

    Returns
    -------
    str
        URL of the RestAPI object representing this address
    """
    try:
        _results = fdp_req.get(
            remote_uri,
            ('storage_root',),
            params={
                'root': address_str
            },
            token=remote_token
        )
        if not _results:
            raise AssertionError
    except (AssertionError, fdp_exc.RegistryAPICallError):
        raise fdp_exc.RegistryAPICallError(
            f"Cannot find a match for path '{address_str}' "
            f"from endpoint '{remote_uri}."
        )

