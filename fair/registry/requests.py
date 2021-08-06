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

import os
import json
import posixpath
import urllib.parse
from _pytest.mark import param
import requests
from typing import Tuple, Any, Dict


import fair.common as fdp_com
import fair.exceptions as fdp_exc


def local_token() -> str:
    """Read the local registry token from the relevant file"""
    _local_token_file = os.path.join(fdp_com.REGISTRY_HOME, "token")
    if not os.path.exists(_local_token_file):
        raise fdp_exc.FileNotFoundError(
            f"Failed to find local registry token, file '{_local_token_file}'"
            "does not exist."
        )
    return open(_local_token_file).readlines()[0].strip()


def _access(
    uri: str,
    method: str,
    obj_path: Tuple[str],
    response_code: int,
    headers: Dict[str, Any] = {},
    params: Dict = {},
    *args,
    **kwargs,
):
    _obj_path = posixpath.join(*obj_path)
    _url = urllib.parse.urljoin(uri, _obj_path)
    if _url[-1] != "/":
        _url = _url + "/"
    if params:
        _url += "?"
        _param_strs = [f"{k}={v}" for k, v in params.items()]
        _url += "&".join(_param_strs)
    _headers = headers.copy()
    _headers["Authorization"] = f"token {local_token()}"
    try:
        _request = getattr(requests, method)(
            _url, headers=_headers, *args, **kwargs
        )
    except requests.exceptions.ConnectionError:
        raise fdp_exc.UnexpectedRegistryServerState(
            f"Failed to make registry API request '{_url}'",
            hint="Is this remote correct and the server running?"
        )
    _json_req = _request.json()
    _result = _json_req["results"] if "results" in _json_req else _json_req

    # Case of unrecognised object
    if _request.status_code == 403:
        raise fdp_exc.RegistryAPICallError(
            f"Failed to retrieve object of type '{' '.join(obj_path)}' with parameters '{params}'", error_code=403
        )
    if _request.status_code != response_code:
        _info = ""
        if isinstance(_result, dict):
            if "detail" in _result:
                _info = _result["detail"]
        if not _info:
            _info = _result
        raise fdp_exc.RegistryAPICallError(
            f"Request failed with status code {_request.status_code}:"
            f" {_info}",
            error_code=_request.status_code,
        )
    return _result


def post(
    uri: str,
    obj_path: Tuple[str],
    data: Dict[str, Any],
    headers: Dict[str, Any] = {},
):
    headers.update({"Content-Type": "application/json"})
    return _access(uri, "post", obj_path, 201, headers, data=json.dumps(data))


def get(
    uri: str,
    obj_path: Tuple[str],
    headers: Dict[str, Any] = {},
    params: Dict[str, Any] = {},
):
    return _access(uri, "get", obj_path, 200, headers, params=params)


def post_else_get(
    uri: str,
    obj_path: Tuple[str],
    data: Dict[str, Any],
    params: Dict[str, Any] = {},
):
    try:
        _loc = post(uri, obj_path, data=data)
    except fdp_exc.RegistryAPICallError as e:
        # If the working config is already in the registry then ignore the
        # conflict error and continue, else raise exception
        if e.error_code == 409:
            _loc = get(uri, obj_path, params=params)
        else:
            raise e
    if isinstance(_loc, list):
        _loc = _loc[0]
    if isinstance(_loc, dict):
        _loc = _loc["url"]
    return _loc
