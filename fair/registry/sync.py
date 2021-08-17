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


def push(
    local_uri: str,
    remote_uri: str,
    remote_token: str,
    items: typing.Dict[typing.Tuple[str], typing.List[typing.Dict]]
    ) -> None:

    # To make sure the items are pushed in the right order make a list
    # of URLs and delete as items are successfully pushed
    _items_unpushed = []

    while _items_unpushed:
        for obj_path in items:
            # Check if the object has any API based dependencies
            _field_type_dependencies = fdp_req.filter_object_dependencies(
                remote_uri,
                obj_path,
                {
                    "read_only": False,
                    "type": "field",
                    "local": True
                }
            )

            for data in items[obj_path]:
                # Firstly check that this object does indeed exist on the
                # local registry with the data provided
                _local_obj = fdp_req.get(
                    local_uri, obj_path, params=data
                )
                if not _local_obj:
                    raise fdp_exc.SynchronisationError(
                        "Failed to retrieve local registry object matching "
                        f"'{data}'"
                    )
                
                # Now check that the objects requirements (if any)
                # are already in the remote registry
                for field in _field_type_dependencies:
                    if not field in data or not data[field]:
                        raise fdp_exc.InternalError(
                            f"Expected non-empty field '{field}' in object "
                            f" '{data}'"
                        )
                    fdp_req.url_get(data[field])
