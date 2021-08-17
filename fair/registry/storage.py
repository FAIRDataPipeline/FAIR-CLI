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

import typing

import yaml
import os
import hashlib
import urllib.parse

import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.identifiers as fdp_id
import fair.registry.file_types as fdp_file
import fair.registry.versioning as fdp_ver


def get_write_storage(uri: str, config_yaml: str) -> str:
    """Construct storage root if it does not exist

    Parameters
    ----------
    uri : str
        end point of the RestAPI
    config_yaml : str
        path of the config file

    Returns
    -------
    str
        URI of the created/retrieved storage root

    Raises
    ------
    fdp_exc.UserConfigError
        If 'write_data_store' not present in the working config
    """

    _cfg = yaml.safe_load(open(config_yaml))
    _cfg_meta = _cfg["run_metadata"]

    if "write_data_store" not in _cfg_meta:
        raise fdp_exc.UserConfigError(
            "Cannot create a storage location on the registry for writing,"
            " no local file location specified."
        )

    # Convert local file path to a valid data store path
    _write_store_root = f"file://{_cfg_meta['write_data_store']}/"

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
    # NOTE: You can have the same file stored N times for N job runs
    # hence the use of a timestamp in the hashing
    _hashable = open(work_cfg_yml).read() + _time_stamp_dir

    _hash = hashlib.sha1(_hashable.encode("utf-8")).hexdigest()

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": _work_cfg['run_metadata']['public'],
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
    # NOTE: You can have the same file stored N times for N job runs
    # hence the use of a timestamp in the hashing
    _hashable = open(script_path).read() + _time_stamp_dir

    _hash = hashlib.sha1(_hashable.encode("utf-8")).hexdigest()

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": _work_cfg['run_metadata']['public'],
        "hash": _hash,
    }

    _post_store_loc = fdp_req.post(
        uri,
        ("storage_location",),
        data=_storage_loc_data,
    )

    _user = store_user(repo_dir, uri)

    _shell_script_type = create_file_type(
        uri, "sh"
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


def store_namespace(uri: str, namespace_label: str, full_name: str = None, website: str = None) -> str:
    """Create a namespace on the 

    Parameters
    ----------
    uri : str
        endpoint of the registry
    namespace_label : str
        name of the Namespace
    full_name : str, optional
        full title of the namespace, by default None
    website : str, optional
        website relating to this namespace, by default None

    Returns
    -------
    str
        URL of the created namespace
    """
    _data = {
        "name": namespace_label,
        "full_name": full_name,
        "website": website
    }
    return fdp_req.post_else_get(
        uri, ("namespace",), data=_data, params={"name": namespace_label}
    )


def store_data_file(
    uri: str,
    repo_dir: str,
    data: typing.Dict,
    local_file: str,
    config_yaml: str,
    public: bool
    ) -> None:
    _cfg = yaml.safe_load(open(config_yaml))
    _root_store = get_write_storage(uri, config_yaml)
    _data_store = _cfg["run_metadata"]["write_data_store"]

    _rel_path = os.path.relpath(local_file, _data_store)

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": public,
        "hash": calculate_file_hash(local_file),
    }

    _post_store_loc = fdp_req.post(
        uri,
        ("storage_location",),
        data=_storage_loc_data,
    )

    _user = store_user(repo_dir, uri)

    if 'file_type' in data:
        _file_type = data['file_type']
    else:
        _file_type = os.path.splitext(local_file)[1]

    _file_type = create_file_type(
        uri, _file_type
    )

    # Allow the user to override the namespace for this particular file
    # if 'default_output_namespace' is also in the entry data
    if 'default_output_namespace' in data:
        _output_namespace = data['default_output_namespace']
    else:
        _output_namespace = _cfg["run_metadata"]["default_output_namespace"]

    _namespace_url = store_namespace(_output_namespace)

    _desc = data['description'] if 'description' in data else None

    _object_data = {
        "description": _desc,
        "file_type": _file_type,
        "storage_location": _post_store_loc,
        "authors": [_user]
    }

    _obj_url = fdp_req.post(uri, ("object",), data=_object_data)['url']

    if 'version' not in data:
        raise fdp_exc.InternalError(
            f"Expected version number for '{local_file}' "
            "registry submission but none found"
        )
    
    # Get the name of the entry
    if 'data_product' in data:
        _name = data['data_product']
    elif 'external_object' in data:
        _name = data['external_object']
    else:
        raise fdp_exc.UserConfigError(
            f"Failed to determine type while storing item '{local_file}'"
            "into registry"
        )

    _data_prod_data = {
        "namespace": _namespace_url,
        "object": _obj_url,
        "version": str(data['version']),
        "name": _name
    }

    _data_prod_url = fdp_req.post(uri, ('data_product',), data=_data_prod_data)

    # If 'data_product' key present finish here and return URL
    # else this is an external object
    if 'data_product' in data:
        return _data_prod_url

    _expected_ext_obj_keys = [
        "release_date",
        "primary",
        "title"
    ]

    for key in _expected_ext_obj_keys:
        if key not in data:
            raise fdp_exc.UserConfigError(
                f"Expected key '{key}' for item '{local_file}'"
            )

    _external_obj_data = {
        "data_product": _data_prod_url,
        "title": data['title'],
        "primary_not_supplement": data['primary'],
        "release_date": data['release_date']
    }

    return fdp_req.post(uri, ('external_object', ), data=_external_obj_data)
    

def calculate_file_hash(file_name: str, buffer_size: int = 64*1024) -> str:
    """Calculates the hash of a data file

    Parameters
    ----------
    file_name : str
        file to calculate

    Returns
    -------
    str
        SHA1 hash for file
    """
    # If the file is large we do not want to hash it in one go
    _input_hasher = hashlib.sha1()

    with open(file_name, 'rb') as in_f:
        _buffer = in_f.read(buffer_size)
        while len(_buffer) > 0:
            _input_hasher.update(_buffer)
            _buffer = in_f.read(buffer_size)
    
    _hash = _input_hasher.hexdigest()

    return _hash


def get_storage_root_obj_address(
    remote_uri: str,
    remote_token: str,
    address_str: str) -> str:
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


def check_match(input_object: str, results_list: typing.List[str]):
    """Check if an object is already on the registry using a hash

    In order to perform this check the results must be a list of storage
    locations for a given list of data_products as SHA1 hashes are present
    for locations only.

    Parameters
    ----------
    input_object : str
        object pending submission to registry
    results_list : typing.List[str]
        list of storage_location results matching search query
    """
    _hash_list = [res['hash'] for res in results_list]
    return calculate_file_hash(input_object) in _hash_list


def check_if_object_exists(
    local_uri: str,
    file_loc: str,
    obj_type: typing.Tuple[str] ,
    name: str,
    token: str = None) -> str:
    """Checks if a data product is already present in the registry

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry
    file_loc : str
        path of file on system
    name : str
        label for the data_product object

    Returns
    -------
    str
        whether object is present with matching hash, present or absent
        if present but not hash matched return the latest version identifier
    """
    # Obtain list of storage_locations for the given data_product
    _results = fdp_req.get(
        local_uri,
        (obj_type,),
        params={"name": name},
        token=token
    )

    if not _results:
        return "absent"

    _object_urls = [res['object'] for res in _results]

    _storage_urls = [
        fdp_req.url_get(obj_url, token=token)['storage_location']
        for obj_url in _object_urls
    ]

    _storage_objs = [
        fdp_req.url_get(store_url, token=token)
        for store_url in _storage_urls
    ]

    if check_match(file_loc, _storage_objs):
        return "hash_match"
    else:
        return fdp_ver.get_latest_version(_results)
