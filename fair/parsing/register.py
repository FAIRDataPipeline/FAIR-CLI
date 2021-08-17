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

import os
import typing
import urllib.parse
import shutil

import click
import semver
import yaml

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import requests


def parse_registrations(
    local_uri: str,
    repo_dir: str,
    config_yaml: str,
    local_data_store: str,
    ) -> typing.Dict:

    _config = yaml.safe_load(config_yaml)
    _registrations = _config['register']

    _expected_keys = [
        "root",
        "path",
        "file_type",
        "primary",
        "version"
    ]

    if 'public' in _config['run_metadata']:
        _public = _config['run_metadata']['public']

    _stored_objects: typing.List[str] = []

    for entry in _registrations:
        for key in _expected_keys:
            if key not in entry:
                raise fdp_exc.UserConfigError(
                    f"Expected key '{key}' in 'register' item"
                )
        
        _doi: str = entry['doi'] if 'doi' in entry else ''
        _unique_name: str = entry['unique_name'] if 'unique_name' in entry else ''

        _data_product = None
        _external_object = None

        if 'data_product' in entry:
            _data_product: str = entry['data_product']
        elif 'external_object' in entry:
            _external_object: str = entry['external_object']

        if not _external_object and not _data_product:
            raise fdp_exc.UserConfigError(
                f"Expected either 'data_product' or "
                "'external_object' in 'register' item"
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

        # Encode the path first
        _path = urllib.parse.quote_plus(_path)

        _url = f"{_root}{_path}"

        try:
            _temp_data_file = fdp_req.download_file(_url)
        except requests.HTTPError as r_in:
            raise fdp_exc.UserConfigError(
                f"Failed to fetch item '{_url}' with exit code "
                f"{r_in.response}"
            )

        _local_dir = os.path.join(local_data_store, _name)

        # Check if the object is already present on the local registry
        _is_present = fdp_store.check_if_object_exists(
            local_uri, _temp_data_file, _obj_type, _name
        )

        # Hash matched version already present
        if _is_present == "hash_match":
            click.echo(
                f"Skipping item '{_name}' as hash matched entry already in"
                "the local registry with this name"
            )
            os.remove(_temp_data_file)
            continue
        
        # Item found but not hash matched retrieve a version number
        elif _is_present != "absent":
            _latest_version = _is_present
            if 'version' in entry:
                _user_version = semver.VersionInfo.parse(entry['version'])
                if _user_version < _latest_version:
                    raise fdp_exc.UserConfigError(
                        f"Cannot add item '{_name}' to local registry "
                        f"with version '{entry['version']}', "
                        "item name already present with latest version "
                        f"'{str(_latest_version)}'"
                    )
            else:
                _user_version = _latest_version
        else:
            if 'version' in entry:
                _user_version = semver.VersionInfo.parse(entry['version'])
            else:
                _user_version = semver.VersionInfo.parse("0.1.0")
        
        # Create object location directory, ignoring if already present
        # as multiple version files can exist
        os.makedirs(_local_dir, exist_ok=True)

        _local_file = os.path.join(
            _local_dir,
            f"{_user_version}{entry['file_type']}"
        )

        # Copy the temporary file into the data store
        # then remove temporary file to save space
        shutil.copy(_temp_data_file, _local_file)
        os.remove(_temp_data_file)

        if 'public' in entry:
            _public = entry['public']

        _file_url = fdp_store.store_data_file(
            uri=local_uri,
            repo_dir=repo_dir,
            data=entry,
            local_file=_local_file,
            config_yaml=config_yaml,
            public=_public
        )

        _stored_objects.append(_file_url)

            


