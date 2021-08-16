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

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store


def parse_registrations(registrations: typing.List[str]) -> typing.Dict:
    _expected_keys = [
        "external_object",
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

        if not _doi and not _unique_name:
            raise fdp_exc.UserConfigError(
                f"Expected either 'unique_name' or 'doi' in 'register' item"
            )

        _root, _path = entry["root"], entry['path']

        _params = entry["query"] if "query" in entry else {}
    
        _data = fdp_req.url_get(
            urllib.parse.urljoin(_root, _path),
            params=_params
        )

        
