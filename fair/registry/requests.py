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
import traceback
import os
import re
import shutil
import tempfile
import typing
import platform
import urllib.parse
import urllib.request

import requests
import simplejson.errors

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.utilities as fdp_util

import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

logger = logging.getLogger("FAIRDataPipeline.Requests")


def split_api_url(
    request_url: str, splitter: str = "api"
) -> typing.Tuple[str]:
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
    method: str = None,
    token: str = None,
    obj_path: str = None,
    response_codes: typing.List[int] = None,
    headers: typing.Dict[str, typing.Any] = None,
    params: typing.Dict = None,
    data: typing.Dict = None,
    files: typing.Dict = None,
    trailing_slash = True,
):
    if response_codes is None:
        response_codes = [201, 200]
    if not headers:
        headers: typing.Dict[str, str] = {}

    if not params:
        params: typing.Dict[str, str] = {}

    if not data:
        data: typing.Dict[str, str] = {}

    # Make sure we have the right number of '/' in the components
    _uri = uri
    _uri = fdp_util.check_trailing_slash(_uri)

    _url = urllib.parse.urljoin(_uri, obj_path) if obj_path else uri

    if trailing_slash:
        _url = fdp_util.check_trailing_slash(_url)
    else:
        _url = fdp_util.remove_trailing_slash(_url)

    _headers = copy.deepcopy(headers)
    if token:
        _headers["Authorization"] = f"token {token}"

    logger.debug("Sending request of type '%s': %s", method, _url)

    try:
        if method == "get":
            logger.debug("Query parameters: %s", params)
            _request = requests.get(_url, headers=_headers, params=params)
        elif method == "post":
            logger.debug("Post data: %s", data)
            _request = requests.post(_url, headers=_headers, data=data)
        elif method == "patch":
            logger.debug("Patch data: %s", data)
            _headers.update({"Content-Type": "application/json"})
            _request = requests.patch(_url, headers=_headers, data=json.dumps(data))
        else:
            _request = getattr(requests, method)(_url, headers=_headers)
    except requests.exceptions.ConnectionError as e:
        raise fdp_exc.UnexpectedRegistryServerState(
            f"Failed to make registry API request '{_url}'",
            hint="Is this remote correct and the server running?",
        ) from e

    _info = f"url = {_url}, "
    _info += f" parameters = {params}," if params else ""
    _info += f" data = {data}" if data else ""

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
            f"Failed to run method '{method}' for url {_url}, request forbidden",
            error_code=403,
        )
    elif _request.status_code == 409:
        _searchable = obj_path or uri
        raise fdp_exc.RegistryAPICallError(
            f"Cannot post object of type '{_searchable}' "
            f"using method '{method}' as it already exists."
            f"Arguments:\n" + _info,
            error_code=409,
        )

    try:
        _json_req = _request.json()
        _result = _json_req["results"] if "results" in _json_req else _json_req
    except (json.JSONDecodeError, simplejson.errors.JSONDecodeError) as exc:
        raise fdp_exc.RegistryAPICallError(
            f"Failed to retrieve JSON data from request to '{_url}'",
            error_code=_request.status_code,
        ) from exc

    if _request.status_code not in response_codes:
        _info = ""
        if isinstance(_result, dict) and "detail" in _result:
            _info = _result["detail"]
        if not _info:
            _info = _result
        raise fdp_exc.RegistryAPICallError(
            f"Request failed with status code {_request.status_code}: {_info}",
            error_code=_request.status_code,
        )
    return _result


def post(
    uri: str,
    obj_path: str,
    token: str,
    data: typing.Dict[str, typing.Any],
    headers: typing.Dict[str, typing.Any] = None,
) -> typing.Dict:
    """Post an object to the registry

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        type of object to post
    token : str
        token for accessing the registry
    data : typing.Dict[str, typing.Any]
        data for the object
    headers : typing.Dict[str, typing.Any], optional
        any additional headers for the request, by default None

    Returns
    -------
    typing.Dict
        resulting data returned from the request
    """
    if headers is None:
        headers = {}

    headers.update({"Content-Type": "application/json"})

    for param, value in data.copy().items():
        if not value:
            logger.debug(
                f"Key in post data '{param}' has no value so will be ignored"
            )
            del data[param]

    return _access(
        uri,
        "post",
        token,
        obj_path,
        headers=headers,
        data=json.dumps(data, cls=fdp_util.JSONDateTimeEncoder),
    )


def url_get(url: str, token: str) -> typing.Dict:
    """Send a URL only request and retrieve results

    Unlike 'get' this method is 'raw' in that there is no validation of
    components

    Parameters
    ----------
    url : str
        URL to send request to
    token: str
        url access token

    Returns
    -------
    typing.Dict
        results dictionary
    """
    return _access(url, "get", token)

def get(
    uri: str,
    obj_path: str,
    token: str,
    headers: typing.Dict[str, typing.Any] = None,
    params: typing.Dict[str, typing.Any] = None,
) -> typing.Dict:
    """Retrieve an object from the given registry

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        type of the object to fetch
    token : str
        token for accessing the registry
    headers : typing.Dict[str, typing.Any], optional
        any additional headers for the request, by default None
    params : typing.Dict[str, typing.Any], optional
        search parameters for the object, by default None

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

    for param, value in params.copy().items():
        if not value:
            logger.warning(
                f"Key in get parameters '{param}' has no value so will be ignored"
            )
            del params[param]

    return _access(
        uri, "get", token, obj_path=obj_path, headers=headers, params=params
    )


def post_else_get(
    uri: str,
    obj_path: str,
    token: str,
    data: typing.Dict[str, typing.Any],
    params: typing.Dict[str, typing.Any] = None,
) -> str:
    """Post to the registry if an object does not exist else retrieve URL

    Parameters
    ----------
    uri : str
        endpoint of the registry
    obj_path : str
        object type to post
    token : str
        token to access registry
    data : typing.Dict[str, typing.Any]
        data for the object to be posted
    params : typing.Dict[str, typing.Any], optional
        parameters for searching if object exists, by default None

    Returns
    -------
    str
        URL for object either retrieved or posted
    """
    if not params:
        params = {}

    try:
        logger.debug(
            "Attempting to post an instance of '%s' to '%s'", obj_path, uri
        )
        _loc = post(uri, obj_path, data=data, token=token)
    except fdp_exc.RegistryAPICallError as e:
        if e.error_code != 409:
            raise e
        logger.debug(e.msg)
        logger.debug("Object already exists, retrieving entry")
        _loc = get(uri, obj_path, token, params=params)
    if isinstance(_loc, list):
        if not _loc:
            logger.error(f"Results of URL query empty: {_loc}")
            try:
                _full_listing = get(uri, obj_path, token)
                logger.debug(f"Available {obj_path}s: {_full_listing}")
            except fdp_exc.RegistryError:
                logger.debug("No entries of type '{obj_path}' exist")
            raise fdp_exc.RegistryError(
                "Expected to receive a URL location from registry post"
            )
        _loc = _loc[0]
    if isinstance(_loc, dict):
        _loc = _loc["url"]
    return _loc

def post_upload_url(
    remote_uri: str,
    remote_token: str,
    file_hash: str
) -> str:
    """Function to get a tempory url to upload and object to

    Args:
        remote_uri (str): Remote registry URL
        remote_token (str): Remote token
        file_hash (str): Hash of the file to be uploaded

    Returns:
        str: A tempory url to upload the object to
    """
    _url = urllib.parse.urljoin(remote_uri, "data/")
    _url = urllib.parse.urljoin(_url, file_hash)
    return _access(_url, "post", remote_token, trailing_slash= False)

def filter_object_dependencies(
    uri: str, obj_path: str, token: str, filter: typing.Dict[str, typing.Any]
) -> typing.List[str]:
    """Filter dependencies of an API object based on a set of conditions

    Parameters
    ----------
    uri : str
        endpoint of the registry
    object : str
        path of object type, e.g. 'code_run'
    token : str
        registry access token
    filter : typing.Dict[str, typing.Any]
        list of filters to apply to listing

    Returns
    -------
    typing.List[str]
        list of object type paths
    """
    logger.debug(
        "Filtering dependencies for object '%s' and filter '%s'",
        obj_path,
        filter,
    )
    try:
        _actions = _access(uri, "options", token, obj_path)["actions"]["POST"]
    except KeyError:
        # No 'actions' key means no dependencies
        return []
    _fields: typing.List[str] = []

    for name, info in _actions.items():
        _filter_result: typing.List[bool] = [
            info[filt] == value
            for filt, value in filter.items()
            if filt in info
        ]

        if all(_filter_result):
            _fields.append(name)

    return _fields


def get_filter_variables(
    uri: str, obj_path: str, token: str
) -> typing.List[str]:
    """Retrieves a list of variables you can filter by for a given object

    Parameters
    ----------
    uri : str
        endpoint of registry
    obj_path : str
        type of object
    token : str
        registry access token

    Returns
    -------
    typing.List[str]
        list of filterable fields
    """
    try:
        _filters = _access(uri, "options", token, obj_path)["filter_fields"]
    except KeyError:
        # No 'filter_fields' key means no filters
        return []
    return [*_filters]


def get_writable_fields(
    uri: str, obj_path: str, token: str
) -> typing.List[str]:
    """Retrieve a list of writable fields for the given RestAPI object

    Parameters
    ----------
    uri : str
        endpoint of the registry
    object : str
        path of object type, e.g. 'code_run'
    token: str
        registry access token

    Returns
    -------
    typing.List[str]
        list of object type paths
    """
    return filter_object_dependencies(
        uri, obj_path, token, {"read_only": False}
    )

def put_file(upload_url: str, file_loc: str) -> bool:
    """Upload a file to a given url using put
    Currently forces the use of TLS 1.2

    Args:
        upload_url (str): URL of where to send the put request to
        file_loc (str): Location of the file to be uploaded

    Raises:
        fdp_exc.RegistryError: If the upload fails a RegistryError will be raised

    Returns:
        bool: Will return True if the upload succeeded.
    """
    s = requests.Session()
    _req = s.put(upload_url, data= open(file_loc,'rb').read())
    if _req.status_code not in [200, 201]:
        raise fdp_exc.RegistryError(f"File: {file_loc} could not be uploaded, Registry Returned: {_req.status_code}")
    return True

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
    _file = tempfile.NamedTemporaryFile(delete=False)
    _fname = _file.name

    # Copy File if local (Windows fix)
    if "file://" in url and platform.system() == "Windows":
        _local_fname = url.replace("file://", "")
        try:
            shutil.copy2(_local_fname, _fname)
        except Exception as e:
            raise fdp_exc.FAIRCLIException(
                f"Failed to download file '{url}'"
                f" due to connection error: {traceback.format_exc()}"
            ) from e

    else:
        try:
            with urllib.request.urlopen(url) as response, open(
                _fname, "wb"
            ) as out_file:
                shutil.copyfileobj(response, out_file)
        except urllib.error.URLError as e:
            raise fdp_exc.FAIRCLIException(
                f"Failed to download file '{url}'"
                f" due to connection error: {e.reason}"
            ) from e

    return _fname


def get_dependency_listing(uri: str, token: str, read_only: bool = False) -> typing.Dict:
    """Get complete listing of all objects and their registry based dependencies

    Parameters
    ----------
    uri : str
        endpoint of the registry
    token : str
        registry access token

    Returns
    -------
    typing.Dict
        dictionary of object types and their registry based dependencies
    """
    try:
        _registry_objs = url_get(uri, token)
    except:
        return {[]}

    _rtn =  {
        obj: filter_object_dependencies(
            uri,
            obj,
            token,
            {"read_only": read_only, "type": "field", "local": True},
        )
        for obj in _registry_objs
        }
    return _rtn


def get_obj_id_from_url(object_url: str) -> int:
    """Retrieves the ID from an object url

    Parameters
    ----------
    object_url : str
        URL for an object on the registry

    Returns
    -------
    int
        integer ID for that object
    """
    _url = urllib.parse.urlparse(object_url)
    return [i for i in _url.path.split("/") if i.strip()][-1]


def get_obj_type_from_url(request_url: str, token: str) -> str:
    """Retrieves the type of object from the given URL

    Parameters
    ----------
    request_url : str
        url to type check
    token: str
        token for accessing specified registry

    Returns
    -------
    str
        object type if recognised else empty string
    """
    _uri, _ = split_api_url(request_url)
    for obj_type in sorted(
        [*url_get(_uri, token=token)], key=len, reverse=True
    ):
        if obj_type in request_url:
            return obj_type
    return ""