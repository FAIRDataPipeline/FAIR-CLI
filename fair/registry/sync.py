#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Sync
=============

Methods for synchronising across registries and allocating versioning

Contents
========

Functions
---------

"""

__date__ = "2021-08-05"

import collections
import logging
from msilib.schema import Component
import os
import re
import shutil
import typing
import urllib.parse
import traceback

import click
import requests

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.utilities as fdp_util
import fair.registry.storage as fdp_store
from fair.registry import SEARCH_KEYS

logger = logging.getLogger("FAIRDataPipeline.Sync")


def get_dependency_chain(object_url: str, token: str) -> collections.deque:
    """Get all objects relating to an object in order of dependency

    For a given URL this function fetches all component URLs ordering them
    in terms of creation order (no dependencies -> most dependencies)
    allowing them to then be re-created in the correct order.

    Parameters
    ----------
    object_url : str
        Full URL of an object within a registry
    token: str
        registry access token

    Returns
    -------
    collections.deque
        ordered iterable of component object URLs
    """
    logger.debug(f"Retrieving dependency chain for '{object_url}'")
    _local_uri, _ = fdp_req.split_api_url(object_url) 

    def _dependency_of(url_list: collections.deque, item: str, _dependency_list:dict):
        if item in url_list:
            return
        url_list.appendleft(item)
        try:
            _results = fdp_req.url_get(item, token)
        except:
            _results = {}
        _type = fdp_req.get_obj_type_from_url(item, token)
        for req, val in _results.items():
            if req in _dependency_list[_type] and val:
                if isinstance(val, list):
                    for url in val:
                        _dependency_of(url_list, url, _dependency_list)
                else:
                    _dependency_of(url_list, val, _dependency_list)

    # Ordering is important so use a deque to preserve
    _object_dependency_list = fdp_req.get_dependency_listing(_local_uri, token)
    _urls = collections.deque()
    _dependency_of(_urls, object_url, _object_dependency_list)

    return _urls


def pull_all_namespaces(
    local_uri: str, remote_uri: str, local_token: str, remote_token: str
) -> typing.List[str]:
    """Pull all namespaces from a remote registry

    This ensures a user does not try to register locally a namespace that
    already exists on the remote and so lowers the risk of conflicting
    metadata when running a pull

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry
    remote_uri : str
        endpoint of the remote registry
    local_token : str
        access token for the local registry
    remote_token : str
        access token for the remote registry

    Returns
    -------
    typing.List[str]
        list of identified namespaces
    """
    logger.debug("Pulling all namespaces to local registry")

    _remote_namespaces = fdp_req.get(remote_uri, "namespace", remote_token)

    logger.debug(
        "Found %s namespace%s on remote",
        len(_remote_namespaces),
        "s" if len(_remote_namespaces) != 1 else "",
    )

    if not _remote_namespaces:
        return

    _writable_fields = fdp_req.get_writable_fields(
        local_uri, "namespace", local_token
    )

    for namespace in _remote_namespaces:
        _writable_data = {
            k: v for k, v in namespace.items() if k in _writable_fields
        }
        logger.debug("Writable local object data: %s", _writable_data)
        fdp_req.post_else_get(
            local_uri, "namespace", local_token, _writable_data
        )


def sync_dependency_chain(
    object_url: str,
    dest_uri: str,
    origin_uri: str,
    dest_token: str,
    origin_token: str,
    public: bool = False,
) -> typing.Dict[str, str]:
    """Push an object and all of its dependencies to the remote registry

    In order to push an object, firstly any dependencies which are also
    registry items must be pushed first.

    Parameters
    ----------
    object_url : str
        object to push
    dest_uri : str
        endpoint of the destination registry
    origin_uri : str
        endpoint of the origin registry
    dest_token : str
        access token for the destination registry
    origin_token : str
        access token for the origin registry

    Returns
    -------
    typing.Dict[str, str]
        dictionary showing conversion from source registry URL to destination
    """
    logger.debug(f"Attempting to push object '{object_url}' to '{dest_uri}'")

    if not origin_token:
        raise fdp_exc.InternalError("Expected an origin token to be provided")

    _dependency_chain: collections.deque = get_dependency_chain(
        object_url, origin_token
    )

    _new_urls: typing.Dict[str, str] = {k: "" for k in _dependency_chain}
    _writable_fields: typing.Dict[str, str] = {}

    # For every object (and the order) in the dependency chain
    # post the object then store the URL so it can be used to assemble those
    # further down the chain
    for object_url in _dependency_chain:
        logger.debug("Preparing object '%s'", object_url)
        # Retrieve the data for the object from the registry
        _obj_data = fdp_req.url_get(object_url, token=origin_token)

        # Deduce the object type from its URL
        _obj_type = fdp_req.get_obj_type_from_url(
            object_url, token=origin_token
        )

        if _obj_type not in _writable_fields:
            _writable_fields[_obj_type] = fdp_req.get_writable_fields(
                origin_uri, _obj_type, origin_token
            )

        # Filter object data to only the writable fields
        _writable_data = {
            k: v
            for k, v in _obj_data.items()
            if k in _writable_fields[_obj_type]
        }

        logger.debug("Writable local object data: %s", _writable_data)

        # If public replace storage root, storage_location
        _new_url = None
        if public:
            _remote_storage_root_url = urllib.parse.urljoin(dest_uri, "storage_root/1/")
            if _obj_type == "storage_root":
                _new_url = _remote_storage_root_url
            if _obj_type == "storage_location":
                _storage_location_data = {
                    "path": _obj_data["hash"],
                    "hash": _obj_data["hash"],
                    "storage_root": _remote_storage_root_url
                }
                _new_url = fdp_req.post_else_get(dest_uri, _obj_type, dest_token, _storage_location_data)
        if not _new_url:
            _new_url = _get_new_url(
                origin_uri=origin_uri,
                origin_token=origin_token,
                dest_uri=dest_uri,
                dest_token=dest_token,
                object_url=object_url,
                new_urls=_new_urls,
                writable_data=_writable_data,
            )

        if not fdp_util.is_api_url(dest_uri, _new_url):
            raise fdp_exc.InternalError(
                f"Expected new URL '{_new_url}' to be compatible with destination URI '{dest_uri}'"
            )

        # Fill the new URLs dictionary with the result
        _new_urls[object_url] = _new_url

    return _new_urls


def _get_new_url(
    origin_uri: str,
    origin_token: str,
    dest_uri: str,
    dest_token: str,
    object_url: str,
    new_urls: typing.Dict,
    writable_data: typing.Dict,
) -> typing.Tuple[typing.Dict, typing.List]:
    _new_obj_data: typing.Dict[str, typing.Any] = {}
    _url_fields: typing.List[str] = []

    # Iterate through the object data, for any values which are URLs
    # substitute the local URL for the created remote ones.
    # For the first object there should be no URL values at all.
    for key, value in writable_data.items():
        # Check if value is URL
        _not_str = not isinstance(value, str)
        _not_url = isinstance(value, str) and not fdp_util.is_api_url(
            origin_uri, value
        )
        if _not_str or _not_url:
            _new_obj_data[key] = value
            continue
        # Store which fields have URLs to use later
        _url_fields.append(key)
        # Make sure that a URL for the component does exist
        if value not in new_urls:
            raise fdp_exc.RegistryError(
                f"Expected URL from remote '{dest_uri}' for component "
                f"'{key}' of local object '{value}' during push."
            )

        # Retrieve from the new URLs the correct value and substitute
        _new_obj_data[key] = new_urls[value]

    # Filters are all variables returned by 'filter_fields' request for a
    # given object minus any variables which have a URL value
    # (as remote URL will never match local)

    _obj_type = fdp_req.get_obj_type_from_url(object_url, token=origin_token)

    _filters = {
        k: v
        for k, v in _new_obj_data.items()
        if k in fdp_req.get_filter_variables(dest_uri, _obj_type, dest_token)
        and isinstance(v, str)
        and k not in _url_fields
    }

    logger.debug(f"Pushing member '{object_url}' to '{dest_uri}'")

    if dest_uri == origin_uri:
        raise fdp_exc.InternalError("Cannot push object to its source address")

    return fdp_req.post_else_get(
        dest_uri,
        _obj_type,
        data=_new_obj_data,
        token=dest_token,
        params=_filters,
    )


def sync_data_products(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    data_products: typing.List[str],
    local_data_store: str = None,
) -> None:
    """Transfer data products from one registry to another

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    dest_token : str
        path to token for destination data registry
    origin_token : str
        path to token for origin data registry
    remote_label : str
        name of remote in listing
    data_products : typing.List[str]
        list of data products to push
    local_data_store : optional, str
        specified when pulling from remote registry to local
    """
    for data_product in data_products:
        namespace, name, version = re.split("[:@]", data_product)

        if _existing_namespace := fdp_req.get(
            dest_uri,
            "namespace",
            params={SEARCH_KEYS["namespace"]: namespace},
            token=dest_token,
        ):
            _namespace_id = fdp_req.get_obj_id_from_url(
                _existing_namespace[0]["url"]
            )
            if _ := fdp_req.get(
                dest_uri,
                "data_product",
                dest_token,
                params={
                    "namespace": _namespace_id,
                    "name": name,
                    "version": version.replace("v", ""),
                },
            ):
                click.echo(
                    f"Data product '{data_product}' already present "
                    f"on remote '{remote_label}', ignoring.",
                )
                continue

        # Convert namespace name to an ID for retrieval
        _namespaces = fdp_req.get(
            origin_uri,
            "namespace",
            params={SEARCH_KEYS["namespace"]: namespace},
            token=origin_token,
        )

        if not _namespaces:
            raise fdp_exc.RegistryError(
                f"Failed to find namespace '{namespace}' on registry {origin_uri}"
            )

        _namespace_id = fdp_req.get_obj_id_from_url(_namespaces[0]["url"])

        query_params = {
            "namespace": _namespace_id,
            "name": name,
            "version": version.replace("v", ""),
        }

        result = fdp_req.get(
            origin_uri, "data_product", params=query_params, token=origin_token
        )

        if not result:
            raise fdp_exc.RegistryError(
                f"Failed to find data product matching descriptor '{data_product}'"
            )
        result = result[0]

        result_object = fdp_req._access(result["object"], "get", token = origin_token)
        result_storage_location = fdp_req._access(result_object["storage_location"], "get", token = origin_token)
        _is_public = result_storage_location["public"]

        if _is_public:
            upload_object(origin_uri, dest_uri, dest_token, origin_token, result_object["url"])
        # if the data_product is an external object sync that first
        if result["external_object"]:
            result = fdp_req._access(result["external_object"], "get", token = origin_token)

        sync_dependency_chain(
            object_url=result["url"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token,
            public= _is_public
        )

        if local_data_store:
            logger.debug("Retrieving files from remote registry data storage")
            fetch_data_product(origin_token, local_data_store, result[0])                    

        for origin_component_url in result_object["components"]:
            origin_input_code_runs = fdp_req.get(
                    origin_uri,
                    "code_run",
                    origin_token,
                    params= {"inputs": fdp_req.get_obj_id_from_url(origin_component_url)}
                )
            for origin_input_code_run in origin_input_code_runs:
                dest_component_url = get_dest_component_url(origin_component_url, dest_uri, dest_token, origin_token)
                sync_code_run(origin_uri, dest_uri, dest_token, origin_token, origin_input_code_run["uuid"], inputs= [dest_component_url])
            
            origin_output_code_runs = fdp_req.get(
                    origin_uri,
                    "code_run",
                    origin_token,
                    params= {"outputs": fdp_req.get_obj_id_from_url(origin_component_url)}
                )
            for origin_output_code_run in origin_output_code_runs:
                dest_component_url = get_dest_component_url(origin_component_url, dest_uri, dest_token, origin_token)
                sync_code_run(origin_uri, dest_uri, dest_token, origin_token, origin_output_code_run["uuid"], outputs= [dest_component_url])
                
def get_dest_component_url(origin_component_url, dest_uri, dest_token, origin_token):
        origin_component = fdp_req._access(origin_component_url, "get", token= origin_token)
        dest_object_url = get_dest_object_url(origin_component["object"], dest_uri, dest_token, origin_token)
        dest_component = fdp_req.get(
            dest_uri,
            "object_component",
            dest_token,
            params= {
                "name": origin_component["name"],
                "object": fdp_req.get_obj_id_from_url(dest_object_url)
            }
        )
        if not dest_component:
            raise fdp_exc.RegistryError(
                f'Failed to access component with object: {fdp_req.get_obj_id_from_url(dest_object_url)} and name {origin_component["name"]} on remote registry'
            )
        return dest_component[0]["url"]

def sync_code_runs(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    code_runs: typing.List[str],
    local_data_store: str = None,
) -> None:
    """Transfer data code_run(s) from one registry to another

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    dest_token : str
        path to token for destination data registry
    origin_token : str
        path to token for origin data registry
    remote_label : str
        name of remote in listing
    code_runs : typing.List[str]
        list of code_run(s) to push
    local_data_store : optional, str
        specified when pulling from remote registry to local
    """
    for code_run_uuid in code_runs:
        _code_run = fdp_req.get(origin_uri, "code_run", origin_token, params = {"uuid": code_run_uuid})
        # Ensure the code run exists on the local registry
        if not _code_run:
            raise fdp_exc.RegistryError(
                f"Failed to find code_run '{code_run_uuid} on local registry'"
            )
        _coderun = _code_run[0]

        _input_components = _coderun["inputs"]
        _output_components = _coderun["outputs"]
        _components = _input_components + _output_components

        _origin_input_data_products = []
        _origin_output_data_products = []

        # get the associated data_product urls
        for component in _components:
            object_url = fdp_req._access(component, "get", origin_token)["object"]
            object = fdp_req._access(object_url, "get", origin_token)
            if component in _input_components:
                _origin_input_data_products += object["data_products"]
            if component in _output_components:
                _origin_output_data_products += object["data_products"]

        _origin_data_products = _origin_input_data_products + _origin_output_data_products
        _inputs_data_products = []
        _outputs_data_products = []
        _dest_inputs = []
        _dest_outputs = []
        # Get and sync the original data products to the remote registry
        for _origin_data_product_url in _origin_data_products:
            _origin_data_product = fdp_req._access(_origin_data_product_url, "get", origin_token)
            if not _origin_data_product:
                raise fdp_exc.RegistryError(
                    f"Failed to access {_origin_data_product} on local registry'"
                )
            _namespace = fdp_req._access(_origin_data_product["namespace"], "get", origin_token)
            if not _namespace:
                raise fdp_exc.RegistryError(
                    f'Failed to access {_origin_data_product["namespace"]} on local registry'
                )
            _data_product_formatted = f'{_namespace["name"]}:{_origin_data_product["name"]}@v{_origin_data_product["version"]}'
            if _origin_data_product_url in _origin_input_data_products:
                _inputs_data_products.append(_data_product_formatted)
            if _origin_data_product_url in _origin_output_data_products:
                _outputs_data_products.append(_data_product_formatted)
        _origin_data_products_formatted = _inputs_data_products + _outputs_data_products
        #logger.info(f"data_products: {_origin_data_products_formatted}")
        # Sync all the data products accosiated with the code run
        sync_data_products(origin_uri,
            dest_uri,
            dest_token, 
            origin_token, 
            remote_label, 
            _origin_data_products_formatted, 
            local_data_store)

        # Iterate through formatted objects and get their new values from the remote registry
        for _origin_data_product_formatted in _origin_data_products_formatted:
            namespace, name, version = re.split("[:@]", _origin_data_product_formatted)
            # Get the destination namespace
            _dest_namespace = fdp_req.get(dest_uri, "namespace", dest_token, params={"name": namespace})
            if not _dest_namespace:
                raise fdp_exc.RegistryError(
                    f'Failed to access {namespace} on remote registry'
                )
            _dest_namespace_url = _dest_namespace[0]["url"]
            # get the data_product
            _dest_data_product = fdp_req.get(dest_uri, "data_product", dest_token, params= {
                "name": name,
                "version": version.replace("v", ""),
                "namespace": fdp_req.get_obj_id_from_url(_dest_namespace_url)
            })
            if not _dest_data_product:
                raise fdp_exc.RegistryError(
                    f'Failed to access data_product: {name} on remote registry'
                )
            _dest_object = fdp_req._access(_dest_data_product[0]["object"], "get", dest_token)
            if _origin_data_product_formatted in _inputs_data_products:
                _dest_inputs += _dest_object["components"]
            if _origin_data_product_formatted in _outputs_data_products:
                _dest_outputs += _dest_object["components"]
        logger.debug(f'attempting to sync coderun {code_run_uuid} with inputs {_dest_inputs} and outputs {_dest_outputs}')
        sync_code_run(origin_uri, dest_uri, dest_token, origin_token, code_run_uuid, _dest_inputs, _dest_outputs)

# Internal function to return the (remote) object associated with a code_run field containing and object url
def get_dest_object_url(origin_object_url, dest_uri, dest_token, origin_token)->str:
    _origin_object_storage_location = fdp_req._access(origin_object_url, "get", origin_token)["storage_location"]
    _model_object_hash = fdp_req._access(_origin_object_storage_location, "get", origin_token)["hash"]
    _dest_object_storage_location = fdp_req.get(dest_uri, "storage_location", dest_token, params={"hash": _model_object_hash})
    if not _dest_object_storage_location:
        raise fdp_exc.RegistryError(
                f'Failed to access {_dest_object_storage_location} on remote registry'
            )
    _dest_object_storage_location_url = _dest_object_storage_location[0]["url"]
    _dest_object = fdp_req.get(dest_uri, "object", dest_token, params= {"storage_location": fdp_req.get_obj_id_from_url(_dest_object_storage_location_url)})
    if not _dest_object:
        raise fdp_exc.RegistryError(
                f'Failed to access {_dest_object} on remote registry'
            )
    return _dest_object[0]["url"]

def sync_code_run(origin_uri, dest_uri, dest_token, origin_token, code_run_uuid, inputs = [], outputs = []):
    code_run = fdp_req.get(origin_uri, "code_run", origin_token, params = {"uuid": code_run_uuid})
    if not code_run:
        raise fdp_exc.RegistryError(
                f'Failed to access code_run {code_run_uuid} on local registry'
            )
    code_run = code_run[0]
    _remote_coderun = fdp_req.get(dest_uri, "code_run", dest_token, params = {"uuid": code_run["uuid"]})
    if _remote_coderun:
        _remote_coderun = _remote_coderun[0]
        inputs += _remote_coderun["inputs"]
        outputs += _remote_coderun["outputs"]
        logger.debug(f'patching code run: {code_run["uuid"]} with inputs: {inputs} and outputs {outputs}')
        fdp_req._access(
            _remote_coderun["url"],
            "patch",
            dest_token,
            data= {
                "inputs": list(set(inputs)),
                "outputs": list(set(outputs))
            }
        )
    else:
        # Get and sync model config
        sync_dependency_chain(
        object_url=code_run["model_config"],
        dest_uri=dest_uri,
        origin_uri=origin_uri,
        dest_token=dest_token,
        origin_token=origin_token,
        public= True
        )
        _dest_code_run_model_config = get_dest_object_url(code_run["model_config"], dest_uri, dest_token, origin_token)
        upload_object(origin_uri, dest_uri, dest_token, origin_token, code_run["model_config"])
        # If theres a code_repo sync it
        _dest_code_run_code_repo = None
        if code_run["code_repo"]:
            sync_dependency_chain(
            object_url=code_run["code_repo"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token,
            public= True
            )
            _dest_code_run_code_repo = get_dest_object_url(code_run["code_repo"],  dest_uri, dest_token, origin_token)
        # Sync Submision Script
        sync_dependency_chain(
        object_url=code_run["submission_script"],
        dest_uri=dest_uri,
        origin_uri=origin_uri,
        dest_token=dest_token,
        origin_token=origin_token,
        public= True
        )
        _dest_code_run_submission_script = get_dest_object_url(code_run["submission_script"], dest_uri, dest_token, origin_token)
        upload_object(origin_uri, dest_uri, dest_token, origin_token, code_run["submission_script"])

        # If the code run is not in the remote registry post the coderun
        dest_code_run = fdp_req.post(dest_uri,
            "code_run",
            dest_token,
            {
                "run_date": code_run["run_date"],
                "description": code_run["description"],
                "model_config": _dest_code_run_model_config,
                "submission_script": _dest_code_run_submission_script,
                "code_repo": _dest_code_run_code_repo,
                "inputs": inputs,
                "outputs": outputs,
                "uuid": code_run["uuid"]
            })
        return dest_code_run

def fetch_data_product(
    remote_token: str, local_data_store: str, data_product: typing.Dict
) -> None:
    """
    Retrieve a file using the given user configuration metadata

    Parameters
    ----------

    remote_uri : str
        remote registry URI
    remote_token : str
        remote registry access token
    config_metadata : typing.Dict
        user configuration file block describing an object
    """
    _object = fdp_req.url_get(data_product["object"], remote_token)

    _endpoint = data_product["object"].split("data_product")[0]

    if not _object.get("storage_location", None):
        logger.debug(
            "Skipping item '%s' for download "
            "as there is no physical storage location",
            data_product,
        )

    _storage_loc = fdp_req.url_get(_object["storage_location"], remote_token)

    _path = _storage_loc["path"]
    _path = urllib.parse.quote(_path)
    _root = fdp_req.url_get(_storage_loc["storage_root"], remote_token)

    _reg_parse = urllib.parse.urlparse(_endpoint)
    _reg_url = f"{_reg_parse.scheme}://{_reg_parse.netloc}"

    _downloaded_file = download_from_registry(_reg_url, _root["root"], _path)

    _namespace = fdp_req.url_get(data_product["namespace"], remote_token)

    if _file_type_url := _object.get("file_type", None):
        _file_type = (
            f'.{fdp_req.url_get(_file_type_url, remote_token)["extension"]}'
        )
    else:
        _file_type = ""

    _local_dir = os.path.join(
        local_data_store, _namespace["name"], data_product["data_product"]
    )

    os.makedirs(_local_dir, exist_ok=True)

    _out_file = os.path.join(
        _local_dir, f'{data_product["version"]}{_file_type}'
    )

    if os.path.exists(_out_file):
        logger.debug("File '%s' already exists skipping download", _out_file)
        return

    shutil.copy(_downloaded_file, _out_file)


def download_from_registry(registry_url: str, root: str, path: str) -> str:
    """
    Download a file from the registry given the storage root and path.

    If the root starts with '/' assume the file exists on the same location as
    the registry itself and try to download from that.

    Parameters
    ----------
    registry_url : str
        net location of the registry (not the endpoint of the API)
    root : str
        storage root
    path : str
        path of file on storage location

    Returns
    -------
    str
        path of downloaded temporary file

    Raises
    ------
    fdp_exc.UserConfigError
        if download failed
    """

    if root.startswith("/"):
        logger.warning(
            "Root of data storage location is '/' assuming data exists"
            " on registry server"
        )

        if not registry_url.endswith("/"):
            registry_url = registry_url[:-1]

        root = f"{registry_url}{root}"

    _download_url = f"{root}{path}"

    try:
        _temp_data_file = fdp_req.download_file(_download_url)
        logger.debug(
            "Downloaded file from '%s' to temporary file", _download_url
        )
    except requests.HTTPError as r_in:
        raise fdp_exc.UserConfigError(
            f"Failed to fetch item '{_download_url}' with exit code {r_in.response}"
        ) from r_in

    return _temp_data_file

def upload_object(origin_uri:str, dest_uri:str, dest_token:str, origin_token:str, object_url: str) -> bool:
    """
    Upload a file from the remote registry given the object url.

    This function only preduces a warning if the file cannot be uploaded

    Parameters
    ----------
    origin_url : str
        url of the local registry
    dest_utl : str
        url of the remote registry
    dest_token : str
        token of the remote registry
    origin_token : str
        token of the local registry
    object_url: str
        url of the the object to be uploded

    Returns
    -------
    bool
        was the file successfully uploaded

    """
    _object = fdp_req._access(object_url, "get", origin_token)
    if not _object["storage_location"]:
        logger.warn(f'File upload error: {object_url} ({_object["description"]}) has no storage_location')
        return False
    _object_storage_location = fdp_req._access(_object["storage_location"], "get", origin_token)
    _object_storage_location_root = fdp_req._access(_object_storage_location["storage_root"], "get", origin_token)
    _file_loc = download_from_registry(origin_uri, _object_storage_location_root["root"], _object_storage_location["path"])
    try:
        fdp_store.upload_remote_file(_file_loc, dest_uri, dest_token)
        return True
    except Exception as e:
        logger.warn(f'File upload error: {_object["description"]} was not uploaded to remote registry please upload the file manually')
        logger.debug(f'{traceback.format_exc()}')
        return False