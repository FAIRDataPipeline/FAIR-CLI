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

import typing
import urllib.parse
import os

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.versioning as fdp_ver


def parse_registrations(
    local_uri: str,
    registrations: typing.List[str],
    local_data_store: str) -> typing.Dict:
    _expected_keys = [
        "root",
        "path",
        "file_type",
        "primary",
        "version"
    ]
    for entry in registrations:
        for key in _expected_keys:
            if key not in entry:
                raise fdp_exc.UserConfigError(
                    f"Expected key '{key}' in 'register' item"
                )
        
        _doi: str = entry['doi'] if 'doi' in entry else ''
        _unique_name: str = entry['unique_name'] if 'unique_name' in entry else ''

        _data_product: str = entry['data_product'] if 'data_product' in entry else ''
        _external_object: str = entry['external_object'] if 'external_object' in entry else ''

        if not _external_object and not _data_product:
            raise fdp_exc.UserConfigError(
                f"Expected either 'data_product' or 'external_object' in 'register' item"
            )
        elif _external_object and _data_product:
            raise fdp_exc.UserConfigError(
                "Only one type may be provided (data_product/external_object)"
            )
        elif _external_object:
            _name = entry['external_object']
            _obj_type = ('external_object',)
        else:
            _name = entry['data_product']
            _obj_type = ('data_product',)

        if not _doi and not _unique_name:
            raise fdp_exc.UserConfigError(
                f"Expected either 'unique_name' or 'doi' in 'register' item"
            )
        elif _doi and _unique_name:
            raise fdp_exc.UserConfigError(
                "Only one unique identifier may be provided (doi/unique_name)"
            )

        _root, _path = entry["root"], entry['path']

        _params = entry["query"] if "query" in entry else {}
    
        _data = fdp_req.url_get(
            urllib.parse.urljoin(_root, _path),
            params=_params
        )

        _file_type_url = fdp_store.create_file_type(
            local_uri, entry['file_type']
        )

        _local_dir = os.path.join(local_data_store, _name)

        # Check if the object is already present on the local registry
        # if so a new semantic version is needed
        _results = fdp_req.get(local_uri, _obj_type, params={"name": _name})

        if 'version' in entry:
            _new_version = ""
    
        _new_version = fdp_ver.get_latest_version(_results)

        os.makedirs(_local_dir, exist_ok=True)

        with open()

        
