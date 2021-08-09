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

from os import remove
import typing

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.versioning as fdp_ver
import semver


def get_new_version(
    uri: str,
    obj_path: typing.Tuple[str],
    version_fmt: str = "",
    **kwargs) -> semver.VersionInfo:
    """Determine the next release version for an object
    
    Parameters
    ----------
    uri : str
        end point of the registry
    obj_path : Tuple[str]
        path of object type, e.g. ('code_run',)
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


def push_item(
    source_uri: str,
    dest_uri: str,
    object_path: typing.Tuple[str],
    params: typing.Dict,
    remote_token: str) -> None:
    """Push an object to the remote registry

    Parameters
    ----------
    source_uri : str
        endpoint of source registry
    dest_uri : str
        endpoint of destination registry
    object_path : Tuple[str]
        path of object type, e.g. ('code_run',)
    params : Dict
        dictionary containing search term parameters of the
        object on the source registry to push
    token : str
        token for the remote registry to push to
    """
    _response = fdp_req.get(source_uri, object_path, params=params)

    _writable_fields = fdp_req.get_writable_fields(source_uri, object_path)

    _data = {
        {k: v} for k, v in _response.items()
        if k in _writable_fields
    }
    
    try:
        _response = fdp_req.post(
            dest_uri, object_path, _data, token=remote_token
        )
    except fdp_exc.RegistryAPICallError as e:
        raise fdp_exc.SynchronisationError(
            f"Failed to push object of type '{''.join(object_path)}'"
            f" with data '{_data}' to registry at '{dest_uri}' "
            f" server returned code {e.error_code}",
            error_code=e.error_code
        )

def pull_item():
    pass
