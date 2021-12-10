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

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.utilities as fdp_util
from fair.register import SEARCH_KEYS

logger = logging.getLogger("FAIRDataPipeline.Sync")

def get_dependency_chain(object_url: str, *args, **kwargs) -> collections.deque:
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

    _dependency_list = fdp_req.get_dependency_listing(_local_uri, *args, **kwargs)

    def _dependency_of(url_list: collections.deque, item: str):
        if item in url_list:
            return
        url_list.appendleft(item)
        _results = fdp_req.url_get(item)
        _type = fdp_req.get_obj_type_from_url(item)
        for req, val in _results.items():
            if req in _dependency_list[_type] and val:
                if isinstance(val, list):
                    for url in val:
                        _dependency_of(url_list, url)
                else:
                    _dependency_of(url_list, val)

    # Ordering is important so use a deque to preserve
    _urls = collections.deque()
    _dependency_of(_urls, object_url)

    return _urls


def push_dependency_chain(
    object_url: str,
    dest_uri: str,
    origin_uri: str,
    dest_token: str,
    origin_token: str
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
    origin_uri : str
        endpoint of the local registry
    dest_token : str
        access token for the destination registry
    origin_token : str
        access token for the origin registry

    Returns
    -------
    typing.Dict[str, str]
        dictionary showing conversion from source registry URL to destination
    """
    _logger = logging.getLogger("FAIRDataPipeline.Sync")
    _logger.debug(f"Attempting to push object '{object_url}' to '{dest_uri}'")

    _dependency_chain: collections.deque = get_dependency_chain(
        object_url,
        token=origin_token
    )
    _new_urls: typing.Dict[str, str] = {k: "" for k in _dependency_chain}
    # For every object (and the order) in the dependency chain
    # post the object then store the URL so it can be used to assemble those
    # further down the chain
    for object_url in _dependency_chain:
        _logger.debug("Preparing object '%s'", object_url)
        # Retrieve the data for the object from the registry
        _obj_data = fdp_req.url_get(object_url, token=origin_token)
        # Get the URI from the URL
        _uri, _ = fdp_req.split_api_url(object_url)

        # Deduce the object type from its URL
        _obj_type = fdp_req.get_obj_type_from_url(object_url)

        # Filter object data to only the writable fields
        _writable_data = {
            k: v
            for k, v in _obj_data.items()
            if k in fdp_req.get_writable_fields(_uri, _obj_type, token=origin_token)
        }

        _logger.debug("Writable local object data: %s", _writable_data)
        _new_obj_data: typing.Dict[str, typing.Any] = {}
        _url_fields: typing.List[str] = []

        # Iterate through the object data, for any values which are URLs
        # substitute the local URL for the created remote ones.
        # For the first object there should be no URL values at all.
        for key, value in _writable_data.items():
            # Check if value is URL
            _not_str = not isinstance(value, str)
            _not_url = isinstance(value, str) and not fdp_util.is_api_url(
                origin_uri, value
            )
            if _not_str or _not_url:
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
            if k in fdp_req.get_filter_variables(_uri, _obj_type, token=origin_token)
            and isinstance(v, str)
            and k not in _url_fields
        }

        _logger.debug(f"Pushing member '{object_url}' to '{dest_uri}'")

        if dest_uri == origin_uri:
            raise fdp_exc.InternalError("Cannot push object to its source address")

        _new_url = fdp_req.post_else_get(
            dest_uri, _obj_type, data=_new_obj_data, token=dest_token, params=_filters
        )

        if not fdp_util.is_api_url(dest_uri, _new_url):
            raise fdp_exc.InternalError(
                f"Expected new URL '{_new_url}' to be compatible with destination URI '{dest_uri}'"
            )

        # Fill the new URLs dictionary with the result
        _new_urls[object_url] = _new_url

    return _new_urls


def push_data_products(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    data_products: typing.List[str]
) -> None:
    """Push data products from one registry to another
    
    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    dest_token : str
        path to token for destination data registry
    origin_token : str
        path to token for origin data registry
    data_products : typing.List[str]
        list of data products to push
    """
    for data_product in data_products:
        namespace, name, version = re.split("[:@]", data_product)

        # Convert namespace name to an ID for retrieval
        _namespaces = fdp_req.get(
            origin_uri,
            "namespace",
            params={SEARCH_KEYS["namespace"]: namespace},
            token=origin_token
        )

        _namespace_id = fdp_req.get_obj_id_from_url(_namespaces[0]["url"])

        query_params = {
            "namespace": _namespace_id,
            "name": name,
            "version": version.replace("v", "")
        }

        result = fdp_req.get(
            origin_uri,
            "data_product",
            params=query_params,
            token=origin_token
        )

        if not result:
            raise fdp_exc.RegistryError(
                f"Failed to find data product matching descriptor '{data_product}'"
            )

        push_dependency_chain(
            object_url=result[0]["url"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token
        )
