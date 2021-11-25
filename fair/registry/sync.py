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
import collections
import logging
import re

import validators

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req


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
    _logger = logging.getLogger("FAIRDataPipeline.Sync")
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
    _logger = logging.getLogger("FAIRDataPipeline.Sync")
    _logger.debug(f"Attempting to push object '{object_url}' to '{dest_uri}'")

    _dependency_chain: collections.deque = get_dependency_chain(object_url)
    _new_urls: typing.Dict[str, str] = {k: "" for k in _dependency_chain}
    _dependency_chain.popleft()

    # For every object (and the order) in the dependency chain
    # post the object then store the URL so it can be used to assemble those
    # further down the chain
    for object_url in _dependency_chain:
        _logger.debug("Preparing object '%s'", object_url)
        # Retrieve the data for the object from the registry
        _obj_data = fdp_req.url_get(object_url)

        # Get the URI from the URL
        _uri, _ = fdp_req.split_api_url(object_url)

        # Deduce the object type from its URL
        _obj_type = fdp_req.get_obj_type_from_url(object_url)

        # Filter object data to only the writable fields
        _writable_data = {
            k: v
            for k, v in _obj_data.items()
            if k in fdp_req.get_writable_fields(_uri, _obj_type)
        }

        _logger.debug("Writable local object data: %s", _writable_data)
        _new_obj_data: typing.Dict[str, typing.Any] = {}
        _url_fields: typing.List[str] = []

        # Iterate through the object data, for any values which are URLs
        # substitute the local URL for the created remote ones.
        # For the first object there should be no URL values at all.
        for key, value in _writable_data.items():
            # Check if value is URL
            if not isinstance(value, str):
                _new_obj_data[key] = value
                continue
            elif isinstance(value, str) and not validators.url(value):
                _new_obj_data[key] = value
                continue
            # Store which fields have URLs to use later
            _url_fields.append(key)
            # Make sure that a URL for the component does exist
            if value not in _new_urls:
                raise fdp_exc.RegistryError(
                    f"Expected URL from remote '{dest_uri}' for component "
                    f"'{key}' of local object '{value}' during push."
                )

            # Retrieve from the new URLs the correct value and substitute
            _new_obj_data[key] = _new_urls[value]

        # Filters are all variables returned by 'filter_fields' request for a
        # given object minus any variables which have a URL value
        # (as remote URL will never match local)

        _filters = {
            k: v
            for k, v in _new_obj_data.items()
            if k in fdp_req.get_filter_variables(_uri, _obj_type)
            and isinstance(v, str)
            and k not in _url_fields
        }


        _logger.debug(f"Pushing member '{object_url}' to '{dest_uri}'")

        _new_url = fdp_req.post_else_get(
            dest_uri, _obj_type, data=_new_obj_data, token=dest_token, params=_filters
        )

        # Fill the new URLs dictionary with the result
        _new_urls[object_url] = _new_url

    return _new_urls


def push_data_products(
    local_uri: str, dest_uri: str, dest_token: str, data_products: typing.List[str]
) -> None:
    _logger = logging.getLogger("FAIRDataPipeline.Sync")
    for data_product in data_products:
        namespace, name, version = re.split("[:@]", data_product)
        query_params = {"namespace": namespace, "name": name, "version": version}
        result = fdp_req.get(local_uri, "data_product", query_params)

        if not result:
            raise fdp_exc.RegistryAPICallError(
                f"Data product not found in local registry using params {query_params}"
            )

        push_dependency_chain(result[0]["url"], dest_uri, dest_token)
