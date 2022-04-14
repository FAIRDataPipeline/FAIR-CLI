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

import hashlib
import logging
import os
import typing

import yaml

import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id
import fair.registry.file_types as fdp_file
import fair.registry.requests as fdp_req
import fair.registry.versioning as fdp_ver

logger = logging.getLogger("FAIRDataPipeline.Storage")


def get_write_storage(uri: str, write_data_store: str, token: str) -> str:
    """Construct storage root if it does not exist

    Parameters
    ----------
    uri : str
        end point of the RestAPI
    write_data_store : str
        path of the write data store
    token: str
        registry access token

    Returns
    -------
    str
        URI of the created/retrieved storage root

    Raises
    ------
    fdp_exc.UserConfigError
        If 'write_data_store' not present in the working config or global config
    """
    logger.debug("Constructing a storage root for '%s'", write_data_store)

    # Convert local file path to a valid data store path
    _write_store_root = f"file://{write_data_store}"
    if _write_store_root[-1] != os.path.sep:
        _write_store_root += os.path.sep

    if _search_root := fdp_req.get(
        uri, "storage_root", token, params={"root": _write_store_root}
    ):
        return _search_root[0]["url"]

    _post_data = {"root": _write_store_root, "local": True}
    _storage_root = fdp_req.post(uri, "storage_root", token, data=_post_data)
    return _storage_root["url"]


def store_author(
    uri: str, token: str, name: str, identifier: str = None, uuid: str = None
) -> str:
    """Creates an Author entry if one does not exist

    Parameters
    ----------
    uri : str
        registry RestAPI endpoint
    token: str
        registry access token
    data: typing.Dict
        author data to post
    params: typing.Dict, optional
        parameters to search if exists already

    Returns
    -------
    str
        URI for created author
    """
    _data = {"name": name, "identifier": identifier, "uuid": uuid}

    return fdp_req.post_else_get(uri, "author", token, _data, {"name": name})


def store_user(repo_dir: str, uri: str, token: str) -> str:
    """Creates an Author entry for the user if one does not exist

    Parameters
    ----------
    repo_dir: str
        repository directory
    token: str
        registry access token
    uri : str
        registry RestAPI endpoint

    Returns
    -------
    str
        URI for created author
    """

    _user = fdp_conf.get_current_user_name(repo_dir)
    name = " ".join(_user) if _user[1] else _user[0]
    _id = None
    _uuid = None

    logger.debug("Storing user '%s'", name)

    try:
        _id = fdp_conf.get_current_user_uri(repo_dir)
    except fdp_exc.CLIConfigurationError:
        _uuid = fdp_conf.get_current_user_uuid(repo_dir)

    return store_author(uri, token, name, _id, _uuid)


def populate_file_type(uri: str, token: str) -> typing.List[typing.Dict]:
    """Populates file_type table with common file file_types

    Parameters
    ----------
    uri: str
        registry RestAPI end point
    token: str
        registry access token
    """
    logger.debug("Adding file types to storage")

    _type_objs = []

    for _extension in fdp_file.FILE_TYPES:
        # Use post_else_get in case some file types exist already
        _result = create_file_type(uri, _extension, token)
        _type_objs.append(_result)
    return _type_objs


def create_file_type(uri: str, extension: str, token: str) -> str:
    """Creates a new file type on the registry

    Parameters
    ----------
    uri : str
        registry RestAPI end point
    extension : str
        file extension
    token: str
        registry access string

    Returns
    -------
    str
        URI for created file type
    """
    _name = fdp_file.FILE_TYPES[extension]

    logger.debug("Adding file type '%s' with extension '%s'", _name, extension)

    return fdp_req.post_else_get(
        uri,
        "file_type",
        token,
        data={"name": _name, "extension": extension.lower()},
        params={"extension": extension.lower()},
    )


def store_working_config(
    repo_dir: str, uri: str, work_cfg_yml: str, token: str
) -> str:
    """Construct a storage location and object for the working config

    Parameters
    ----------
    repo_dir : str
        local FAIR repository
    uri : str
        RestAPI end point
    work_cfg_yml : str
        location of working config yaml
    token: str
        registry access token

    Returns
    -------
    str
        new URI for the created object

    Raises
    ------
    fair.exceptions.RegistryAPICallError
        if bad status code returned from the registry
    """
    logger.debug("Storing working config on registry")

    _root_store = get_write_storage(uri, work_cfg_yml, token)

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
        "public": _work_cfg["run_metadata"].get("public", True),
        "hash": _hash,
    }

    try:
        _post_store_loc = fdp_req.post(
            uri, "storage_location", token, data=_storage_loc_data
        )
    except fdp_exc.RegistryAPICallError as e:
        if e.error_code != 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post storage_location '{_rel_path}' with hash '{_hash}', object already exists",
                error_code=409,
            ) from e

    _user = store_user(repo_dir, uri, token)

    _yaml_type = create_file_type(uri, "yaml", token)

    _desc = f"Working configuration file for timestamp {_time_stamp_dir}"
    _object_data = {
        "description": _desc,
        "storage_location": _post_store_loc["url"],
        "file_type": _yaml_type,
        "authors": [_user],
    }

    return fdp_req.post_else_get(
        uri, "object", token, data=_object_data, params={"description": _desc}
    )


def store_working_script(
    repo_dir: str, uri: str, script_path: str, working_config: str, token: str
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
    token: str
        registry access token

    Returns
    -------
    str
        new URI for the created object

    Raises
    ------
    fair.exceptions.RegistryAPICallError
        if bad status code returned from the registry
    """
    logger.debug("Storing working script on registry")

    _work_cfg = yaml.safe_load(open(working_config))
    _root_store = get_write_storage(uri, working_config, token)
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
        "public": _work_cfg["run_metadata"].get("public", True),
        "hash": _hash,
    }

    try:
        _post_store_loc = fdp_req.post(
            uri, "storage_location", token, data=_storage_loc_data
        )
    except fdp_exc.RegistryAPICallError as e:
        if e.error_code != 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post storage_location "
                f"'{_rel_path}' with hash"
                f" '{_hash}', object already exists",
                error_code=409,
            ) from e

    _user = store_user(repo_dir, uri, token)

    _shell_script_type = create_file_type(uri, "sh", token)

    _time_stamp_dir = os.path.basename(os.path.dirname(script_path))
    _desc = f"Run script for timestamp {_time_stamp_dir}"
    _object_data = {
        "description": _desc,
        "storage_location": _post_store_loc["url"],
        "file_type": _shell_script_type,
        "authors": [_user],
    }

    return fdp_req.post_else_get(
        uri, "object", token, data=_object_data, params={"description": _desc}
    )


def store_namespace(
    uri: str, token: str, name: str, full_name: str = None, website: str = None
) -> str:
    """Create a namespace on the registry

    Parameters
    ----------
    uri : str
        endpoint of the registry
    namespace_label : str
        name of the Namespace
    token : str
        registry access token
    full_name : str, optional
        full title of the namespace, by default None
    website : str, optional
        website relating to this namespace, by default None

    Returns
    -------
    str
        URL of the created namespace
    """
    logger.debug("Storing namespace '%s' on registry", name)

    _data = {
        "name": name,
        "full_name": full_name,
        "website": website,
    }

    return fdp_req.post_else_get(
        uri, "namespace", token, data=_data, params={"name": name}
    )


def store_data_file(
    uri: str,
    repo_dir: str,
    token: str,
    data: typing.Dict,
    local_file: str,
    write_data_store: str,
    public: bool,
) -> None:

    logger.debug("Storing data file '%s' on registry", local_file)

    _root_store = get_write_storage(uri, write_data_store, token)

    _rel_path = os.path.relpath(local_file, write_data_store)

    if "version" not in data["use"]:
        raise fdp_exc.InternalError(
            f"Expected version number for '{local_file}' "
            "registry submission but none found"
        )

    _post_store_loc = _get_url_from_storage_loc(
        local_file=local_file,
        registry_uri=uri,
        registry_token=token,
        relative_path=_rel_path,
        root_store_url=_root_store,
        is_public=public,
    )

    _user = store_user(repo_dir, uri, token)

    _file_type = _get_url_from_file_type(
        data=data,
        local_file=local_file,
        registry_uri=uri,
        registry_token=token,
    )

    _namespace_url = _get_url_from_namespace(
        data=data, label=local_file, registry_uri=uri, registry_token=token
    )

    _obj_url = _get_url_from_object(
        data=data,
        registry_uri=uri,
        registry_token=token,
        user=_user,
        storage_loc_url=_post_store_loc,
        file_type_url=_file_type,
    )

    _data_prod_url = _get_url_from_data_product(
        data=data,
        label=local_file,
        registry_uri=uri,
        registry_token=token,
        namespace_url=_namespace_url,
        object_url=_obj_url,
    )

    # If 'data_product' key present finish here and return URL
    # else this is an external object
    if "data_product" in data:
        return _data_prod_url

    return _get_url_from_external_obj(
        data=data,
        local_file=local_file,
        registry_uri=uri,
        registry_token=token,
        data_product_url=_data_prod_url,
    )


def _get_url_from_storage_loc(
    local_file: str,
    registry_uri: str,
    registry_token: str,
    relative_path: str,
    root_store_url: str,
    is_public: bool,
) -> str:
    _hash = calculate_file_hash(local_file)

    _storage_loc_data = {
        "path": relative_path,
        "storage_root": root_store_url,
        "public": is_public,
        "hash": _hash,
    }

    _search_data = {"hash": _hash}

    return fdp_req.post_else_get(
        registry_uri,
        "storage_location",
        registry_token,
        data=_storage_loc_data,
        params=_search_data,
    )


def _get_url_from_file_type(
    data: typing.Dict, local_file: str, registry_uri: str, registry_token: str
) -> str:
    if "file_type" in data:
        _file_type = data["file_type"]
    else:
        _file_type = os.path.splitext(local_file)[1]

    return create_file_type(registry_uri, _file_type, registry_token)


def _get_url_from_namespace(
    data: typing.Dict, label: str, registry_uri: str, registry_token: str
) -> str:
    # Namespace is read from the source information
    if "namespace_name" not in data:
        raise fdp_exc.UserConfigError(
            f"Expected 'namespace_name' for item '{label}'"
        )

    _namespace_args = {
        "name": data["namespace_name"],
        "full_name": data["namespace_full_name"]
        if "namespace_full_name" in data
        else None,
        "website": data.get("namespace_website", None),
    }

    return store_namespace(registry_uri, registry_token, **_namespace_args)


def _get_url_from_external_obj(
    data: typing.Dict,
    local_file: str,
    registry_uri: str,
    registry_token: str,
    data_product_url: str,
) -> typing.Dict:
    _expected_ext_obj_keys = ("release_date", "primary", "title")

    for key in _expected_ext_obj_keys:
        if key not in data:
            raise fdp_exc.UserConfigError(
                f"Expected key '{key}' for item '{local_file}'"
            )

    _external_obj_data = {
        "data_product": data_product_url,
        "title": data["title"],
        "primary_not_supplement": data["primary"],
        "release_date": data["release_date"],
    }
    _external_obj_data.update(_get_identifier_from_data(data, local_file))

    return fdp_req.post(
        registry_uri,
        "external_object",
        registry_token,
        data=_external_obj_data,
    )


def _get_url_from_object(
    data: typing.Dict,
    registry_uri: str,
    registry_token: str,
    user: str,
    storage_loc_url: str,
    file_type_url: str,
) -> str:
    _desc = data.get("description", None)

    _object_data = {
        "description": _desc,
        "file_type": file_type_url,
        "storage_location": storage_loc_url,
        "authors": [user],
    }

    try:
        return fdp_req.post(
            registry_uri, "object", registry_token, data=_object_data
        )["url"]
    except fdp_exc.RegistryAPICallError as e:
        if e.error_code != 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post object" f"'{_desc}', duplicate already exists",
                error_code=409,
            ) from e

    except KeyError as e:
        raise fdp_exc.InternalError(
            f"Expected key 'url' in local registry API response"
            f" for post object '{_desc}'"
        ) from e


def _get_url_from_data_product(
    data: typing.Dict,
    label: str,
    registry_uri: str,
    registry_token: str,
    namespace_url: str,
    object_url: str,
) -> str:
    """Retrieve the URL for a given config data product"""
    # Get the name of the entry
    if "external_object" in data:
        _name = data["external_object"]
    elif "data_product" in data:
        _name = data["data_product"]
    else:
        raise fdp_exc.UserConfigError(
            f"Failed to determine type while storing item '{label}'"
            "into registry"
        )

    if "data_product" in data["use"]:
        _name = data["use"]["data_product"]

    _data_prod_data = {
        "namespace": namespace_url,
        "object": object_url,
        "version": str(data["use"]["version"]),
        "name": _name,
    }

    try:
        return fdp_req.post(
            registry_uri, "data_product", registry_token, data=_data_prod_data
        )["url"]
    except fdp_exc.RegistryAPICallError as e:
        if e.error_code != 409:
            raise e
        else:
            raise fdp_exc.RegistryAPICallError(
                f"Cannot post data_product '{_name}', duplicate already exists",
                error_code=409,
            ) from e

    except KeyError as e:
        raise fdp_exc.InternalError(
            f"Expected key 'url' in local registry API response"
            f" for post object '{_name}'"
        ) from e


def _get_identifier_from_data(
    data: typing.Dict, label: str
) -> typing.Dict[str, str]:
    """Retrieve the identifier metadata from the data entry"""
    _identifier: typing.Dict[str, str] = {}

    if data.get("identifier", None):
        if not fdp_id.check_id_permitted(data["identifier"]):
            raise fdp_exc.UserConfigError(
                "Identifier '"
                + data["identifier"]
                + "' is not a valid identifier"
            )
        _identifier["identifier"] = data["identifier"]
    else:
        try:
            _identifier["alternate_identifier"] = data["unique_name"]
        except KeyError as e:
            raise fdp_exc.UserConfigError(
                "No identifier/alternate_identifier given for "
                f"item '{label}'",
                hint="You must provide either a URL 'identifier', or "
                "'unique_name' and 'source_name' keys",
            ) from e

        _identifier["alternate_identifier"] = data.get(
            "alternate_identifier_type", "local source descriptor"
        )

    return _identifier


def calculate_file_hash(file_name: str, buffer_size: int = 64 * 1024) -> str:
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

    with open(file_name, "rb") as in_f:
        _buffer = in_f.read(buffer_size)
        while len(_buffer) > 0:
            _input_hasher.update(_buffer)
            _buffer = in_f.read(buffer_size)

    return _input_hasher.hexdigest()


def get_storage_root_obj_address(
    remote_uri: str, remote_token: str, address_str: str
) -> str:
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
            "storage_root",
            params={"root": address_str},
            token=remote_token,
        )
        if not _results:
            raise AssertionError
    except (AssertionError, fdp_exc.RegistryAPICallError) as e:
        raise fdp_exc.RegistryError(
            f"Cannot find a match for path '{address_str}' "
            f"from endpoint '{remote_uri}."
        ) from e


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
    _hash_list = [res["hash"] for res in results_list]
    return calculate_file_hash(input_object) in _hash_list


def check_if_object_exists(
    local_uri: str,
    file_loc: str,
    obj_type: str,
    search_data: typing.Dict,
    token: str,
) -> str:
    """Checks if a data product is already present in the registry

    Parameters
    ----------
    local_uri : str
        local registry endpoint
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
    _version = None

    if "version" in search_data:
        _version = search_data["version"]
        if "${{" in _version:
            del search_data["version"]

    # Obtain list of storage_locations for the given data_product
    _results = fdp_req.get(
        local_uri, obj_type, params=search_data, token=token
    )

    try:
        fdp_ver.get_correct_version(
            version=_version, results_list=_results, free_write=True
        )
    except fdp_exc.UserConfigError:
        return "absent"

    if not _results:
        return "absent"

    if obj_type == "external_object":
        _results = [res["data_product"] for res in _results]
        _results = [fdp_req.url_get(r, token=token) for r in _results]

    _object_urls = [res["object"] for res in _results]

    _storage_urls = [
        fdp_req.url_get(obj_url, token=token)["storage_location"]
        for obj_url in _object_urls
    ]

    _storage_objs = [
        fdp_req.url_get(store_url, token=token) for store_url in _storage_urls
    ]

    return "hash_match" if check_match(file_loc, _storage_objs) else _results
