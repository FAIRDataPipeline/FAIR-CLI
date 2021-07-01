#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry
========

Methods relating to connection with local and remote registries for the purpose
of synchronisation and push/pull.

Contents
========
"""
import os
import json
import posixpath
import urllib.parse
import yaml
import requests
from typing import Tuple, Any, Dict

__date__ = "2021-07-01"


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


class Requester:
    def __init__(self, uri: str, working_config: str) -> None:
        """Registry connection class for accessing data from either a remote or
        local registry instance.

        Parameters
        ----------
        uri : str
            end point for the registry
        """
        self._endpoint = uri

        if not os.path.exists(working_config):
            raise fdp_exc.FileNotFoundError(
                f"Cannot process working config '{working_config}', "
                "file not found."
            )
        _work_cfg = yaml.safe_load(open(working_config))

        if "run_metadata" not in _work_cfg:
            raise fdp_exc.UserConfigError(
                f"Cannot process working config '{working_config}', "
                "no run metadata found."
            )

        self._work_cfg_meta = _work_cfg["run_metadata"]

    def _initial_setup(self, user_config: str) -> None:
        """Check if write storage roots are available else create them"""
        _run_meta = user_config["run_metadata"]
        _write_store_root = _run_meta["write_data_store"]
        _head = "file://"
        _data = {
            "root": f"{_head}{_write_store_root}{os.path.sep}",
            "local": True,
        }
        if not self._check_if_exists(
            ("storage_root",), {"root": _data["root"]}
        ):
            print(json.dumps(_data))
            self._post(("storage_root",), data=_data)

    def _access(
        self,
        method: str,
        obj_path: Tuple[str],
        response_code: int,
        headers: Dict[str, Any] = {},
        params: Dict = {},
        *args,
        **kwargs,
    ):
        _obj_path = posixpath.join(*obj_path)
        _url = urllib.parse.urljoin(self._endpoint, _obj_path)
        if _url[-1] != "/":
            _url = _url + "/"
        if params:
            _url += "?"
            _param_strs = [f"{k}={v}" for k, v in params.items()]
            _url += "&".join(_param_strs)
        _headers = headers.copy()
        _headers["Authorization"] = f"token {local_token()}"
        _request = getattr(requests, method)(
            _url, headers=_headers, *args, **kwargs
        )
        _json_req = _request.json()
        _result = _json_req["results"] if "results" in _json_req else _json_req
        if _request.status_code != response_code:
            raise AssertionError(
                f"Request failed with status code {_request.status_code}:"
                f" {_result['detail'] if not isinstance(_result, list) else _result}"
            )
        return _result

    def _post(
        self,
        obj_path: Tuple[str],
        data: Dict[str, Any],
        headers: Dict[str, Any] = {},
    ):
        headers.update({"Content-Type": "application/json"})
        return self._access(
            "post", obj_path, 201, headers, data=json.dumps(data)
        )

    def _get(
        self,
        obj_path: Tuple[str],
        headers: Dict[str, Any] = {},
        params: Dict[str, Any] = {},
    ):
        return self._access("get", obj_path, 200, headers, params=params)

    def get_write_storage(self) -> str:
        """Construct storage root if it does not exist"""

        if "write_data_store" not in self._work_cfg_meta:
            raise fdp_exc.UserConfigError(
                "Cannot create a storage location on the registry for writing,"
                " no local file location specified."
            )
        _write_store_root = f"file://{self._work_cfg_meta['write_data_store']}"
        _search_root = self._get(
            ("storage_root",), params={"root": _write_store_root}
        )

        if not _search_root:
            _post_data = {"root": _write_store_root, "local": True}
            _storage_root = self._post(("storage_root",), data=_post_data)
            return _storage_root["url"]
        else:
            return _search_root[0]["url"]
