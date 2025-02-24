#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
User Config Remote Globbing
===========================

Handles the inclusion of wildcards in configuration statements by speaking to
the local registry and extracting items.


Constants
---------

    - DISPOSABLES: tuple of keys to be removed before adding to config.yaml

"""
import typing

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
from fair.registry import SEARCH_KEYS

__date__ = "2022-01-11"


DISPOSABLES = (
    "name",
    "object",
    "last_updated",
    "namespace",
    "release_date",
    "updated_by",
    "original_store",
    "prov_report",
    "external_object",
    "internal_format",
    "url",
)


def get_single_layer_objects(
    results_list: typing.List[typing.Dict], object_type: str
) -> typing.List[typing.Dict]:
    """
    Retrieve results for a wildcard search for the given object

    This object should not have any requirements (other registry URLs)

    Parameters
    ----------
    results_list : typing.List[typing.Dict]
        results of registry search for the wildcard

    Returns
    -------
    typing.List[typing.Dict]
        entries for the config.yaml file
    """
    _new_entries: typing.List[typing.Dict] = []

    for result in results_list:
        _data = result.copy()
        for key, value in result.items():
            if not value:
                _data.pop(key, None)
        _data[object_type] = _data["name"]

        for key in DISPOSABLES:
            _data.pop(key, None)

        _new_entries.append(_data)
    return _new_entries


def get_data_product_objects(
    registry_token: str,
    results_list: typing.List[typing.Dict],
    block_type: str,
    version: str = None,
) -> typing.List[typing.Dict]:
    """
    Retrieve results for a wildcard search of a data_product

    Parameters
    ----------
    results_list : typing.List[typing.Dict]
        results of registry search for the wildcard

    Returns
    -------
    typing.List[typing.Dict]
        entries for the config.yaml file
    """
    _new_entries: typing.List[typing.Dict] = []

    # If a data product need to retrieve the namespace name
    for entry in results_list:
        _data = entry.copy()

        for key, value in entry.items():
            if not value:
                _data.pop(key, None)

        _namespace = fdp_req.url_get(entry["namespace"], registry_token)

        if not _namespace:
            raise fdp_exc.InternalError(
                "Failed to retrieve namespace for external_object "
                f"{entry[SEARCH_KEYS['data_product']]}"
            )

        _version = entry["version"]

        if block_type == "write" and version:
            _version = version

        _data["use"] = {}
        _data["use"]["namespace"] = _namespace["name"]
        _data["data_product"] = _data["name"]
        _data["use"]["data_product"] = _data["name"]
        _data["use"]["version"] = _version

        for key in DISPOSABLES:
            _data.pop(key, None)

        _new_entries.append(_data)

    return _new_entries


def get_external_objects(
    registry_token: str,
    results_list: typing.List[typing.Dict],
    block_type: str,
    version: str = None,
) -> typing.List[typing.Dict]:
    """
    Retrieve results for a wildcard search of a external_object

    Parameters
    ----------
    results_list : typing.List[typing.Dict]
        results of registry search for the wildcard

    Returns
    -------
    typing.List[typing.Dict]
        entries for the config.yaml file
    """
    _new_entries: typing.List[typing.Dict] = []

    for result in results_list:
        _data = result.copy()

        for key, value in result.items():
            if not value:
                _data.pop(key, None)

        _data_product = fdp_req.url_get(result["data_product"], registry_token)

        if not _data_product:
            raise fdp_exc.InternalError(
                "Failed to retrieve data_product for external_object "
                f"{result[SEARCH_KEYS['data_product']]}"
            )

        _namespace = fdp_req.url_get(_data_product["namespace"], fdp_req.local_token())

        if not _namespace:
            raise fdp_exc.InternalError(
                "Failed to retrieve namespace for external_object "
                f"{result[SEARCH_KEYS['data_product']]}"
            )

        _version = result["version"]

        if block_type == "write" and version:
            _version = version

        _data["use"] = {}
        _data["use"]["namespace"] = (_namespace["name"],)
        _data["use"]["version"] = _version
        _data.pop("name", None)
        _data.pop("last_updated", None)

        for key in DISPOSABLES:
            _data.pop(key, None)

        _new_entries.append(_data)

    return _new_entries
