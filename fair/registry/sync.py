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

import typing
import os
import collections
import urllib.parse
import click
import logging
import yaml

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.versioning as fdp_ver
import semver


_logger = logging.getLogger('FairDataPipeline.Sync')


def get_new_version(
    uri: str,
    obj_path: str,
    version_fmt: str = None,
    **kwargs) -> semver.VersionInfo:
    """Determine the next release version for an object
    
    Parameters
    ----------
    uri : str
        end point of the registry
    obj_path : str
        path of object type, e.g. 'code_run'
    version_fmt : str
        a formatting string which determines how the version is incremented
    
    Returns
    -------
    semver.VersionInfo
        new version allocation
    """
    _existing = fdp_req.get(uri, obj_path, params=kwargs)

    if _existing:
        if 'version' not in _existing[0]:
            raise fdp_exc.RegistryError(
                "Expected 'version' in RestAPI call object."
            )

        _versions = [semver.VersionInfo.parse(i['version']) for i in _existing]
        _versions = sorted(_versions)
        _latest = _versions[-1]
    else:
        _latest = semver.VersionInfo.parse("0.0.0")

    if not version_fmt:
        version_fmt = '${{ MINOR }}'

    _bump_func = fdp_ver.parse_incrementer(version_fmt)
    
    return getattr(_latest, _bump_func)()


def get_dependency_chain(object_url: str) -> collections.deque:
    """Get all objects relating to an object in order of dependency

    For a given URL this function fetches all component URLs ordering them
    in terms of creation order (no dependencies -> most dependencies)
    allowing them to then be re-created in the correct order.

    Parameters
    ----------
    object_url : str
        Full URL of an object within a registry

    Returns
    -------
    collections.deque
        ordered iterable of component object URLs
    """
    _logger.debug(f"Retrieving dependency chain for '{object_url}'")
    _local_uri, _ = fdp_req.split_api_url(object_url)

    _dependency_list = fdp_req.get_dependency_listing(_local_uri)

    def _dependency_of(url_list: collections.deque, item: str):
        if item in url_list:
            return
        url_list.appendleft(item)
        _results = fdp_req.url_get(item)
        for req in _results:
            if req in _dependency_list and _results[req]:
                _dependency_of(url_list, _results[req])

    # Ordering is important so use a deque to preserve
    _urls = collections.deque()
    _dependency_of(_urls, object_url)

    return _urls


def push_dependency_chain(
    object_url: str,
    dest_uri: str,
    dest_token: str,
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
    dest_token : str
        access token for the destination registry

    Returns
    -------
    typing.Dict[str, str]
        dictionary showing conversion from source registry URL to destination
    """
    _logger.debug(
        f"Attempting to push object '{object_url}' to '{dest_uri}'"
    )

    _dependency_chain: collections.deque = get_dependency_chain(object_url)
    _new_urls: typing.Dict[str, str] = {
        k: ""
        for k in _dependency_chain
    }

    for object in _dependency_chain:
        _obj_data = fdp_req.url_get(object)
        _obj_type = fdp_req.get_obj_type_from_url(object)
        _uri, _ = fdp_req.split_api_url(object)
        # Substitute any local dependency URLs for those
        # from the components created on the remote
        for key, value in _obj_data:
            if value in _new_urls:
                _obj_data[key] = _new_urls[value]
        _writable_data = {
            k: v for k, v in _obj_data.items()
            if k in fdp_req.get_writable_fields(_uri, _obj_type)
        }

        # Filters are all variables returned by 'filter_fields' request for a
        # given object minus any variables which have a URL value
        # (as remote URL will never match local)
        _filters = {
            k: v for k, v in _obj_data.items()
            if k in fdp_req.get_filter_variables(_uri, _obj_type) and
            not urllib.parse.urlparse(v).netloc
        }

        _logger.debug(
            f"Pushing member '{object}' to '{dest_uri}'"
        )
        _new_url = fdp_req.post_else_get(
            dest_uri,
            _obj_type,
            data=_writable_data,
            token=dest_token,
            params=_filters
        )

        _new_urls[object] = _new_url

    return _new_urls
        

def push_from_config(
    local_uri: str,
    dest_uri: str,
    dest_token: str,
    config_yaml: str) -> None:
    if not os.path.exists(config_yaml):
        raise fdp_exc.FileNotFoundError(
            f"Cannot load write statements from '{config_yaml}', "
            "file does not exist."
        )
    _logger.debug(f"Reading 'write' statement in '{config_yaml}'")
    _config = yaml.safe_load(open(config_yaml))

    if 'write' not in _config:
        click.echo("Nothing to push.")
        return

    for object in _config['write']:
        _logger.debug(f"Processing object '{object}'")
        if 'external_object' in object:
            _usable_fields = {}
            if 'identifier' in object:
                _usable_fields['identifier'] = object['identifier']
            elif 'alternate_identifier' in object:
                _usable_fields['identifier'] = object['alternate_identifier']
            if 'title' in object:
                _usable_fields['title'] = object['title']
            if 'version' in object:
                _usable_fields['version'] = object['version']
            _entries = fdp_req.get(
                local_uri,
                'external_object',
                params=_usable_fields
            )

            if not _entries or len(_entries) > 1:
                raise fdp_exc.InternalError(
                    "Expected single entry for 'external_object' "
                    f"'{object['external_object']}"
                )
            
            _url = _entries[0]['url']

            push_dependency_chain(_url, dest_uri, dest_token)
        elif 'data_product' in object:
            _usable_fields = {'name': object['data_product']}
            if 'version' in object:
                _usable_fields['version'] = object['version']
            
            _entries = fdp_req.get(
                local_uri,
                'data_product',
                params=_usable_fields
            )

            if not _entries or len(_entries) > 1:
                raise fdp_exc.InternalError(
                    "Expected single entry for 'data_product' "
                    f"'{object['data_product']}"
                )
            
            _url = _entries[0]['url']

            push_dependency_chain(_url, dest_uri, dest_token)
        else:
            fdp_exc.NotImplementedError(
                "Cannot write unsupported data type, object must be "
                "either a data_product or external_object"
            )
            