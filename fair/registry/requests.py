#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Requests
=================

Methods relating to connection with local and remote registries for the purpose
of synchronisation and push/pull.

Contents
========

    local_token   - retrieves the local API token
    post          - post to the registry RestAPI
    get           - retrieve from the registry RestAPI
    post_else_get - if an item already exists retrieve it, else create it

"""

__date__ = "2021-07-02"

import copy
import json
import logging
import os
import re
import tempfile
import typing
import urllib.parse

import requests
import simplejson.errors

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.utilities as fdp_util

SEARCH_KEYS = {
    "data_product": "name",
    "namespace": "name",
    "file_type": "extension",
    "storage_root": "root",
    "storage_location": "hash",
}

logger = logging.getLogger("FAIRDataPipeline.Requests")


def split_api_url(request_url: str, splitter: str = "api") -> typing.Tuple[str]:
    """Split a request URL into endpoint and path

    Parameters
    ----------
    request_url : str
        URL to be split
    splitter : str, optional
        split point, by default 'api'

    Returns
    -------
    typing.Tuple[str]
        endpoint, path
    """
    _root, _path = request_url.split(f"{splitter}/")
    return f"{_root}{splitter}", _path


def local_token(registry_dir: str = None) -> str:
    """Read the local registry token from the relevant file"""
    registry_dir = registry_dir or fdp_com.registry_home()
    _local_token_file = os.path.join(registry_dir, "token")
    if not os.path.exists(_local_token_file):
        raise fdp_exc.FileNotFoundError(
            f"Failed to find local registry token, file '{_local_token_file}'"
            " does not exist.",
            hint="Try creating the file by manually starting the registry "
            "by running 'fair registry start'",
        )
    _file_lines = open(_local_token_file).readlines()

    if not _file_lines:
        raise fdp_exc.FileNotFoundError(
            f"Expected token in file {_local_token_file}, but file is empty"
        )

    return _file_lines[0].strip()


def _access(
    uri: str,
    method: str,
    obj_path: str = None,
    response_codes: typing.List[int] = [201, 200],
    token: str = None,
    headers: typing.Dict[str, typing.Any] = None,
    params: typing.Dict = None,
    data: typing.Dict = None,
    *args,
    **kwargs,
):
    if not headers:
        headers: typing.Dict[str, str] = {}

    if not params:
        params: typing.Dict[str, str] = {}

    if not data:
        data: typing.Dict[str, str] = {}

    if not token:
        token = local_token()

    # Make sure we have the right number of '/' in the components
    _uri = uri
    _uri = fdp_util.check_trailing_slash(_uri)

    _url = urllib.parse.urljoin(_uri, obj_path) if obj_path else uri

    _url = fdp_util.check_trailing_slash(_url)

    _headers = copy.deepcopy(headers)
    _headers["Authorization"] = f"token {token}"

    logger.debug("Sending request of type '%s': %s", method, _url)

    try:
        if method == "get":
            logger.debug("Query parameters: %s", params)
            _request = requests.get(
                _url, headers=_headers, params=params, *args, **kwargs
            )
        elif method == "post":
            logger.debug("Post data: %s", data)
            _request = requests.post(_url, headers=_headers, data=data, *args, **kwargs)
        else:
            _request = getattr(requests, method)(
                _url, headers=_headers, *args, **kwargs
            )
    except requests.exceptions.ConnectionError:
        raise fdp_exc.UnexpectedRegistryServerState(
            f"Failed to make registry API request '{_url}'",
            hint="Is this remote correct and the server running?",
        )

    _info = f"url = {_url}, "
    _info += f' parameters = {kwargs["params"]},' if "params" in kwargs else ""
    _info += f' data = {kwargs["data"]}' if "data" in kwargs else ""

    # Case of unrecognised object
    if _request.status_code == 404:
        raise fdp_exc.RegistryAPICallError(
            f"Attempt to access an unrecognised resource on registry "
            f"using method '{method}' and arguments: " + _info,
            error_code=404,
        )

    # Case of unrecognised object

    if _request.status_code == 403:
        raise fdp_exc.RegistryAPICallError(
            f"Failed to run method '{method}' for url {_url}, " f"request forbidden",
            error_code=403,
        )
    elif _request.status_code == 409:
        _searchable = uri if not obj_path else "/".join(obj_path)
        raise fdp_exc.RegistryAPICallError(
            f"Cannot post object of type '{_searchable}' "
            f"using method '{method}' as it already exists."
            f"Arguments:\n" + _info,
            error_code=409,
        )

    try:
        _json_req = _request.json()
        _result = _json_req["results"] if "results" in _json_req else _json_req
    except (json.JSONDecodeError, simplejson.errors.JSONDecodeError):
        raise fdp_exc.RegistryAPICallError(
            f"Failed to retrieve JSON data from request to '{_url}'",
            error_code=_request.status_code,
        )

    if _request.status_code not in response_codes:
        _info = ""
        if isinstance(_result, dict) and "detail" in _result:
            _info = _result["detail"]
        if not _info:
            _info = _result
        raise fdp_exc.RegistryAPICallError(
            f"Request failed with status code {_request.status_code}:" f" {_info}",
            error_code=_request.status_code,
        )
    return _result


def post(
    uri: str,
    obj_path: str,
    data: typing.Dict[str, typing.Any],
    headers: typing.Dict[str, typing.Any] = None,
    token: str = None,
) -> typing.Dict:
    """Post an object to the registry

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        type of object to post
    data : typing.Dict[str, typing.Any]
        data for the object
    headers : typing.Dict[str, typing.Any], optional
        any additional headers for the request, by default None
    token : str, optional
        token for accessing the registry, by default None

    Returns
    -------
    typing.Dict
        resulting data returned from the request
    """
    if headers is None:
        headers = {}

    if not token:
        token = local_token()

    headers.update({"Content-Type": "application/json"})
    return _access(
        uri,
        "post",
        obj_path,
        headers=headers,
        data=json.dumps(data, cls=fdp_util.JSONDateTimeEncoder),
        token=token,
    )


def url_get(url: str, *args, **kwargs) -> typing.Dict:
    """Send a URL only request and retrieve results

    Unlike 'get' this method is 'raw' in that there is no validation of
    components

    Parameters
    ----------
    url : str
        URL to send request to

    Returns
    -------
    typing.Dict
        results dictionary
    """
    return _access(url, "get", *args, **kwargs)


def get(
    uri: str,
    obj_path: str,
    headers: typing.Dict[str, typing.Any] = None,
    params: typing.Dict[str, typing.Any] = None,
    token: str = None,
) -> typing.Dict:
    """Retrieve an object from the given registry

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        type of the object to fetch
    headers : typing.Dict[str, typing.Any], optional
        any additional headers for the request, by default None
    params : typing.Dict[str, typing.Any], optional
        search parameters for the object, by default None
    token : str, optional
        token for accessing the registry, by default None

    Returns
    -------
    typing.Dict
        returned data for the given request
    """
    logger.debug(
        "Retrieving object of type '%s' from registry at '%s' with parameters: %s",
        obj_path,
        uri,
        params,
    )

    if not headers:
        headers = {}

    params = {} if not params else copy.deepcopy(params)

    if not token:
        token = local_token()

    if "namespace" in params and isinstance(params["namespace"], str):
        _namespaces = get(
            uri, "namespace", params={SEARCH_KEYS["namespace"]: params["namespace"]}
        )

        if len(_namespaces) > 1:
            raise fdp_exc.UserConfigError(
                f"Multiple ({len(_namespaces)}) hits for namespace '{params['namespace']}'"
            )
        elif len(_namespaces) == 0:
            raise fdp_exc.UserConfigError(
                f"No hits for namespace '{params['namespace']}'"
            )

        _results = re.search(r"^" + uri + r"/?namespace/(\d+)/$", _namespaces[0]["url"])

        if not _results:
            raise fdp_exc.InternalError("Failed to parse namespace identifiers")

        params["namespace"] = int(_results.group(1))

    if "data_product" in params:
        _data_products = get(
            uri,
            "data_product",
            params={
                SEARCH_KEYS["data_product"]: params["data_product"],
                "namespace": params["namespace"],
            },
        )

        _results = [
            re.search(r"^" + uri + r"/?data_product/(\d+)/$", _data_product["url"])
            for _data_product in _data_products
        ]

        _output = []
        del params["namespace"]
        for data_product in _results:
            params["data_product"] = int(data_product.group(1))
            _output.extend(
                _access(
                    uri,
                    "get",
                    obj_path,
                    [200],
                    headers=headers,
                    params=params,
                    token=token,
                )
            )
        return _output

    return _access(
        uri,
        "get",
        obj_path,
        headers=headers,
        params=params,
        token=token
    )


def post_else_get(
    uri: str,
    obj_path: str,
    data: typing.Dict[str, typing.Any],
    params: typing.Dict[str, typing.Any] = None,
    token: str = None,
) -> str:
    """Post to the registry if an object does not exist else retrieve URL

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        object type to post
    data : typing.Dict[str, typing.Any]
        data for the object to be posted
    params : typing.Dict[str, typing.Any], optional
        parameters for searching if object exists, by default None
    token : str, optional
        token to access registry, by default None

    Returns
    -------
    str
        URL for object either retrieved or posted
    """
    if not params:
        params = {}

    if not token:
        token = local_token()

    try:
        logger.debug("Attempting to post an instance of '%s' to '%s'", obj_path, uri)
        _loc = post(uri, obj_path, data=data, token=token)
    except fdp_exc.RegistryAPICallError as e:
        # If the item is already in the registry then ignore the
        # conflict error and continue, else raise exception
        if e.error_code == 409:
            logger.debug("Object already exists, retrieving entry")
            _loc = get(uri, obj_path, params=params, token=token)
        else:
            raise e

    if isinstance(_loc, list):
        if not _loc:
            raise fdp_exc.RegistryError(
                "Expected to receive a URL location from registry post"
            )
        _loc = _loc[0]
    if isinstance(_loc, dict):
        _loc = _loc["url"]
    return _loc


def filter_object_dependencies(
    uri: str, obj_path: str, filter: typing.Dict[str, typing.Any]
) -> typing.List[str]:
    """Filter dependencies of an API object based on a set of conditions

    Parameters
    ----------
    uri : str
        endpoint of the registry
    object : str
        path of object type, e.g. 'code_run'
    filter : typing.Dict[str, typing.Any]
        list of filters to apply to listing

    Returns
    -------
    typing.List[str]
        list of object type paths
    """
    try:
        _actions = _access(uri, 'options', obj_path)['actions']['POST']
    except KeyError:
        # No 'actions' key means no dependencies
        return []
    _fields: typing.List[str] = []

    for name, info in _actions.items():
        _filter_result: typing.List[bool] = []
        for filt, value in filter.items():
            # Some objects may not have the key
            if filt not in info:
                continue
            _filter_result.append(info[filt] == value)
        if all(_filter_result):
            _fields.append(name)

    return _fields


def get_filter_variables(uri: str, obj_path: str) -> typing.List[str]:
    """Retrieves a list of variables you can filter by for a given object

    Parameters
    ----------
    uri : str
        endpoint of registry
    obj_path : str
        type of object

    Returns
    -------
    typing.List[str]
        list of filterable fields
    """
    try:
        _filters = _access(uri, 'options', obj_path)['filter_fields']
    except KeyError:
        # No 'filter_fields' key means no filters
        return []
    return [*_filters]


def get_writable_fields(uri: str, obj_path: str) -> typing.List[str]:
    """Retrieve a list of writable fields for the given RestAPI object

    Parameters
    ----------
    uri : str
        endpoint of the registry
    object : str
        path of object type, e.g. 'code_run'

    Returns
    -------
    typing.List[str]
        list of object type paths
    """
    return filter_object_dependencies(uri, obj_path, {"read_only": False})


def download_file(url: str, chunk_size: int = 8192) -> str:
    """Download a file from a given URL

    Parameters
    ----------
    url : str
        address of remote file
    chunk_size : int, optional
        chunk size for download, by default 8192

    Returns
    -------
    str
        path of downloaded temporary file
    """
    # Save the data to a temporary file so we can calculate the hash
    _file, _fname = tempfile.mkstemp()

    with requests.get(url, stream=True) as r_in:
        try:
            r_in.raise_for_status()
        except requests.HTTPError:
            raise fdp_exc.FileNotFoundError(
                f"Failed to download file from '{url}'"
                f" with status code {r_in.status_code}"
            )
        with os.fdopen(_file, "wb") as in_f:
            for chunk in r_in.iter_content(chunk_size=chunk_size):
                in_f.write(chunk)

    return _fname


def get_dependency_listing(uri: str) -> typing.Dict:
    """Get complete listing of all objects and their registry based dependencies

    Parameters
    ----------
    uri : str
        endpoint of the registry

    Returns
    -------
    typing.Dict
        dictionary of object types and their registry based dependencies
    """

    _registry_objs = url_get(uri)

    return {
        obj: filter_object_dependencies(
            uri, obj, {"read_only": False, "type": "field", "local": True}
        )
        for obj in _registry_objs
    }


def get_obj_type_from_url(request_url: str) -> str:
    """Retrieves the type of object from the given URL

    Parameters
    ----------
    request_url : str
        url to type check

    Returns
    -------
    str
        object type if recognised else empty string
    """
    _uri, _ = split_api_url(request_url)
    for obj_type in sorted([*url_get(_uri)], key=len, reverse=True):
        if obj_type in request_url:
            return obj_type
    return ""
