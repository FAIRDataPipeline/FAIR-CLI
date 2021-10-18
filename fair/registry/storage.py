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
    populate_file_type - adds entries for standard file types
    create_file_type - create a file type entry
    store_working_config - create locations/objects for a working config

"""

__date__ = "2021-07-02"

import typing
import os
import hashlib
import urllib.parse
import logging
import yaml

import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.identifiers as fdp_id
import fair.registry.file_types as fdp_file
import fair.registry.versioning as fdp_ver

def get_write_storage(uri: str, cfg: typing.Dict) -> str:
    """Construct storage root if it does not exist

    Parameters
    ----------
    uri : str
        end point of the RestAPI
    str : typing.Dict
        path of the config file

    Returns
    -------
    str
        URI of the created/retrieved storage root

    Raises
    ------
    fdp_exc.UserConfigError
        If 'write_data_store' not present in the working config or global config
    """

    _write_data_store = fdp_conf.write_data_store(cfg)

    # Convert local file path to a valid data store path
    _write_store_root = f"file://{_write_data_store}"
    if _write_store_root[-1] != os.path.sep:
        _write_store_root += os.path.sep

    # Check if the data store already exists by querying for it
    _search_root = fdp_req.get(
        uri, "storage_root", params={"root": _write_store_root}
    )

    # If the data store already exists just return the URI else create it
    # and then do the same
    if _search_root:
        return _search_root[0]["url"]

    _post_data = {"root": _write_store_root, "local": True}
    _storage_root = fdp_req.post(uri, "storage_root", data=_post_data)
    return _storage_root["url"]


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
    _data = {'name': ' '.join(_user) if _user[0] else _user[1]}

    try:
        _id = fdp_conf.get_current_user_uri(repo_dir)
        _data['identifier'] = _id
        return fdp_req.post_else_get(
            uri, "author", data=_data, params={"identifier": _id}
        )
    except fdp_exc.CLIConfigurationError:
        _uuid = fdp_conf.get_current_user_uuid(repo_dir)
        _data['uuid'] = _uuid
        return fdp_req.post_else_get(
            uri, "author", data=_data, params={"uuid": _uuid}
        )


def populate_file_type(uri:str) -> typing.List[typing.Dict]:
    """Populates file_type table with common file file_types

    Parameters
    ----------
    uri: str
        registry RestAPI end point
    """
    _type_objs = []

    for _extension in fdp_file.FILE_TYPES:
        # Use post_else_get in case some file types exist already
        _result = create_file_type(uri, _extension)
        _type_objs.append(_result)
    return _type_objs


def create_file_type(uri: str, extension: str) -> str:
    """Creates a new file type on the registry

    Parameters
    ----------
    uri : str
        registry RestAPI end point
    extension : str
        file extension

    Returns
    -------
    str
        URI for created file type
    """
    _name = fdp_file.FILE_TYPES[extension]
    return fdp_req.post_else_get(
        uri, "file_type",
        data={"name": _name, "extension": extension.lower()},
        params={"extension": extension.lower()}
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

    try:
        _post_store_loc = fdp_req.post(
            uri,
            "storage_location",
            data=_storage_loc_data
        )
    except fdp_exc.RegistryAPICallError as e:
        if not e.error_code == 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post storage_location "
                f"'{_rel_path}' with hash"
                f" '{_hash}', object already exists",
                error_code=409
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
        uri, "object", data=_object_data, params={"description": _desc}
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

    try:
        _post_store_loc = fdp_req.post(
            uri,
            "storage_location",
            data=_storage_loc_data
        )
    except fdp_exc.RegistryAPICallError as e:
        if not e.error_code == 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post storage_location "
                f"'{_rel_path}' with hash"
                f" '{_hash}', object already exists",
                error_code=409
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
        uri, "object", data=_object_data, params={"description": _desc}
    )


def store_namespace(
    uri: str,
    namespace_label: str,
    full_name: str = None,
    website: str = None
) -> str:
    """Create a namespace on the registry

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
        uri, "namespace", data=_data, params={"name": namespace_label}
    )


def update_namespaces(user_cfg: typing.Dict) -> None:
    """Ensure that the namespaces in the user config are created
    
    Parameters
    ----------
    user_cfg : typing.Dict
        job specification from a user 'config.yaml' file
    """

    _local_uri = fdp_conf.registry_url("local", user_cfg)

    _namespaces: typing.List[str] = []
    _new_block = []

    if 'register' in user_cfg:
        _registration_items = user_cfg['register']

        for item in _registration_items:
            if 'external_object' in item or 'data_product' in item:
                if 'namespace_name' in item:
                    _namespaces.append({
                        'name': item['namespace_name'],
                        'full_name': item.get('namespace_full_name', None),
                        'website': item.get('namespace_website', None)
                    })
                elif 'namespace' in item and isinstance(item['namespace'], dict):
                    _namespaces.append(item['namespace'])
                _new_block.append(item)
            elif 'namespace' in item:
                _namespaces.append({
                    'name': item['namespace'],
                    'full_name': item.get('full_name', None),
                    'website': item.get('website', None)
                })
            else:
                _new_block.append(item)

        for namespace in _namespaces:
            store_namespace(
                _local_uri,
                namespace['name'],
                namespace['full_name'],
                namespace['website']
            )
        
    store_namespace(
        _local_uri,
        fdp_conf.input_namespace(user_cfg)
    )

    store_namespace(
        _local_uri,
        fdp_conf.output_namespace(user_cfg)
    )

    if 'register' in user_cfg:
        for item in _new_block:
            if ('external_object' in item or 'data_product' in item) and 'namespace' in item:
                if isinstance(item['namespace'], str):
                    item['namespace_name'] = item['namespace']
                elif isinstance(item['namespace'], dict):
                    item['namespace_name'] = item['namespace']['name']
                item['namespace_full_name'] = None
                item['namespace_website'] = None
                del item['namespace']
        user_cfg['register'] = _new_block


def store_data_file(
    uri: str,
    repo_dir: str,
    data: typing.Dict,
    local_file: str,
    cfg: typing.Dict,
    public: bool
) -> None:

    _root_store = get_write_storage(uri, cfg)
    _data_store = fdp_conf.write_data_store(cfg)

    _rel_path = os.path.relpath(local_file, _data_store)

    if 'version' not in data['use']:
        raise fdp_exc.InternalError(
            f"Expected version number for '{local_file}' "
            "registry submission but none found"
        )

    _hash = calculate_file_hash(local_file)

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": public,
        "hash": _hash,
    }

    _search_data = {
        'hash': _hash
    }

    _post_store_loc = fdp_req.post_else_get(
        uri,
        "storage_location",
        data=_storage_loc_data,
        params=_search_data
    )

    _user = store_user(repo_dir, uri)

    if 'file_type' in data:
        _file_type = data['file_type']
    else:
        _file_type = os.path.splitext(local_file)[1]

    _file_type = create_file_type(
        uri, _file_type
    )

    # Namespace is read from the source information
    if 'namespace_name' not in data:
        raise fdp_exc.UserConfigError(
            f"Expected 'namespace_name' for item '{local_file}'"
        )

    _namespace_args = {
        "uri": uri,
        "namespace_label": data['namespace_name'],
        "full_name": data['namespace_full_name']
            if 'namespace_full_name' in data else None,
        "website": data['namespace_website']
            if 'namespace_website' in data else None
    }

    _namespace_url = store_namespace(**_namespace_args)

    _desc = data['description'] if 'description' in data else None

    _object_data = {
        "description": _desc,
        "file_type": _file_type,
        "storage_location": _post_store_loc,
        "authors": [_user]
    }

    try:
        _obj_url = fdp_req.post(uri, "object", data=_object_data)['url']
    except fdp_exc.RegistryAPICallError as e:
        if not e.error_code == 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post object"
                f"'{_desc}', duplicate already exists",
                error_code=409
            )
    except KeyError:
        raise fdp_exc.InternalError(
            f"Expected key 'url' in local registry API response"
            f" for post object '{_desc}'"
        )

    # Get the name of the entry
    if 'external_object' in data:
        _name = data['external_object']
    elif 'data_product' in data:
        _name = data['data_product']
    else:
        raise fdp_exc.UserConfigError(
            f"Failed to determine type while storing item '{local_file}'"
            "into registry"
        )

    if 'data_product' in data['use']:
        _name = data['use']['data_product']

    _data_prod_data = {
        "namespace": _namespace_url,
        "object": _obj_url,
        "version": str(data['use']['version']),
        "name": _name
    }

    try:
        _data_prod_url = fdp_req.post(uri, 'data_product', data=_data_prod_data)['url']
    except fdp_exc.RegistryAPICallError as e:
        if not e.error_code == 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post data_product "
                f"'{_name}', duplicate already exists",
                error_code=409
            )
    except KeyError:
        raise fdp_exc.InternalError(
            f"Expected key 'url' in local registry API response"
            f" for post object '{_name}'"
        )

    # If 'data_product' key present finish here and return URL
    # else this is an external object
    if 'data_product' in data:
        return _data_prod_url

    _expected_ext_obj_keys = [
        "release_date",
        "primary",
        "title"
    ]

    _identifier = None
    _alternate_identifier = None
    _alternate_identifier_type = None

    if 'identifier' in data:
        _identifier = data['identifier']
        if not fdp_id.check_id_permitted(_identifier):
            raise fdp_exc.UserConfigError(
                f"Identifier '{_identifier}' is not a valid identifier"
            )

    if not _identifier:
        if 'unique_name' not in data:
            raise fdp_exc.UserConfigError(
                "No identifier/alternate_identifier given for "
                f"item '{local_file}'",
                hint="You must provide either a URL 'identifier', or "
                "'unique_name' and 'source_name' keys"
            )
        else:
            _alternate_identifier = data['unique_name']
            if 'alternate_identifier_type' in data:
                _alternate_identifier_type = data['alternate_identifier_type']
            else:
                _alternate_identifier_type = 'local source descriptor'

    for key in _expected_ext_obj_keys:
        if key not in data:
            raise fdp_exc.UserConfigError(
                f"Expected key '{key}' for item '{local_file}'"
            )

    _external_obj_data = {
        "data_product": _data_prod_url,
        "title": data['title'],
        "primary_not_supplement": data['primary'],
        "release_date": data['release_date'],
        "identifier": _identifier,
        "alternate_identifier": _alternate_identifier,
        "alternate_identifier_type": _alternate_identifier_type
    }

    return fdp_req.post(uri, 'external_object', data=_external_obj_data)


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
    print(_input_hasher.hexdigest())

    return _input_hasher.hexdigest()


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
            'storage_root',
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
    cfg: typing.Dict,
    file_loc: str,
    obj_type: str,
    search_data: typing.Dict,
    token: str = None) -> str:
    """Checks if a data product is already present in the registry

    Parameters
    ----------
    cfg : typing.Dict
        config yaml
    file_loc : str
        path of file on system
    obj_type : str
        object type
    search_data : typing.Dict
        data for query
    token : str
        token for registry

    Returns
    -------
    str
        whether object is present with matching hash, present or absent
        if present but not hash matched return the latest version identifier
    """
    _logger = logging.getLogger("FAIRDataPipeline.Run")

    _local_uri = fdp_conf.registry_url("local", cfg)


    _version = None
    if 'version' in search_data:
        _version = search_data['version']
        if '${{' in _version:
            del search_data['version']

    # Obtain list of storage_locations for the given data_product
    _results = fdp_req.get(
        _local_uri,
        obj_type,
        params=search_data,
        token=token
    )

    _logger.debug(search_data)
    _logger.debug(_results)

    try:
        fdp_ver.get_correct_version(cfg, _results, False, _version)
    except fdp_exc.UserConfigError:
        return "absent"

    if not _results:
        return "absent"

    if obj_type == 'external_object':
        _results = [res['data_product'] for res in _results]
        _results = [fdp_req.url_get(r) for r in _results]

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
        return _results
