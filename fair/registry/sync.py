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
import fair.utilities as fdp_util
import semver


def get_new_version(
    uri: str,
    obj_path: typing.Tuple[str],
    version_fmt: str = None,
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
    local_uri: str,
    remote_uri: str,
    object_path: typing.Tuple[str],
    params: typing.Dict,
    remote_token: str) -> None:
    """Push an object to the remote registry

    Parameters
    ----------
    local_uri : str
        endpoint of local registry
    remote_uri : str
        endpoint of remote registry
    object_path : Tuple[str]
        path of object type, e.g. ('code_run',)
    params : Dict
        dictionary containing search term parameters of the
        object on the source registry to push
    token : str
        token for the remote registry to push to
    """
    _response = fdp_req.get(local_uri, object_path, params=params)

    if not _response:
        raise fdp_exc.SynchronisationError(
            f"Failed to retrieve '{''.join(object_path)}'"
            f" from registry '{local_uri}', no object found."
        )

    _writable_fields = fdp_req.get_writable_fields(local_uri, object_path)

    _data = {
        k: v for k, v in _response.items()
        if k in _writable_fields
    }
    
    try:
        _response = fdp_req.post(
            remote_uri, object_path, data=_data, token=remote_token
        )
    except fdp_exc.RegistryAPICallError as e:
        raise fdp_exc.SynchronisationError(
            f"Failed to sync object of type '{''.join(object_path)}'"
            f" with data '{_data}' to registry at '{remote_uri}' "
            f" server returned code {e.error_code}",
            error_code=e.error_code
        )


def pull_item(
    remote_uri: str,
    local_uri: str,
    object_path: typing.Tuple[str],
    params: typing.Dict,
    local_token: str
    ) -> None:
    """Pull an object from the remote registry

    Parameters
    ----------
    local_uri : str
        endpoint of local registry
    remote_uri : str
        endpoint of remote registry
    object_path : Tuple[str]
        path of object type, e.g. ('code_run',)
    params : Dict
        dictionary containing search term parameters of the
        object on the source registry to push
    token : str
        token for the remote registry to push to
    """
    push_item(local_uri, remote_uri, object_path, params, local_token)


def compare_entries(
    local_reg_entry: typing.Dict, remote_reg_entry: typing.Dict
    ) -> bool:
    # TODO: This assumes the UUIDs have been setup to always match between
    # registries. Ensure this occurs.

    _flat_loc = fdp_util.flatten_dict(local_reg_entry)
    _flat_rem = fdp_util.flatten_dict(remote_reg_entry)

    return _flat_loc == _flat_rem


def sync(
    local_uri: str,
    remote_uri: str,
    remote_key: str,
    local_key: str,
    sync_objects: typing.Dict,
    semver_spec: typing.Dict
    ) -> None:
    """Synchronise remote and local registries

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry
    remote_uri : str
        endpoint of the remote registry
    remote_key : str
        remote repository token
    local_key : str
        local repository token
    sync_objects : Dict[str, Dict[str, Dict]]
        dictionary containing objects to be synchronised
    semver_spec: Dict
        semantic version specifier dictionary

    The semantic versioning specification dictionary must be in the same form
    as the sync_objects dictionary. Objects are arranged by type (as a path)
    and a local identifier used to connect the sync_objects and semver_spec
    dictionaries. By default the semver_spec entry can be None as many objects
    do not have versioning, where this is the case a conflict should arise.
    """
    for obj_path, obj_listing in sync_objects.items():
        for obj_id, data in obj_listing.items():
            if obj_id not in semver_spec[obj_path]:
                raise fdp_exc.InternalError(
                    "Semantic version specification "
                    "did not match expectation"
                )

            _entry_local = None
            _entry_remote = None

            try:
                _entry_local = fdp_req.get(
                    local_uri,
                    obj_path,
                    params=data,
                    token=local_key
                )
                if not _entry_local:
                    raise AssertionError
            except (AssertionError, fdp_exc.fdp_exc.RegistryAPICallError):
                pass
            
            try:
                _entry_remote = fdp_req.get(
                    remote_uri,
                    obj_path,
                    params=data,
                    token=remote_key
                )
                if not _entry_remote:
                    raise AssertionError
            except (AssertionError, fdp_exc.fdp_exc.RegistryAPICallError):
                pass

            # If the two are exactly the same then skip
            if compare_entries(_entry_local, _entry_remote):
                continue

            # Case of sync conflict
            if _entry_local and _entry_remote:
                raise fdp_exc.SynchronisationError(
                    "CONFLICT cannot synchronise object "
                    f" '{'/'.join(obj_path)}' with parameters "
                    f"{data}, this object is already present in both "
                    "registries and semantic versioning is not permitted."
                )

            
                