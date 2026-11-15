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

import collections
import contextlib
import logging
import os
import re
import shutil
import tempfile
import typing
import urllib.parse
import traceback

import requests

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.utilities as fdp_util
import fair.registry.storage as fdp_store
from fair.registry import SEARCH_KEYS

logger = logging.getLogger("FAIRDataPipeline.Sync")


def get_dependency_chain(
    object_url: str, token: str, substitutes: typing.Dict = None
) -> collections.deque:
    """Get all objects relating to an object in order of dependency

    For a given URL this function fetches all component URLs ordering them
    in terms of creation order (no dependencies -> most dependencies)
    allowing them to then be re-created in the correct order.

    Parameters
    ----------
    object_url : str
        Full URL of an object within a registry
    token: str
        registry access token
    substitutes : optional, typing.Dict
        values to read in place of an object's own, keyed by the object's URL
        and then by field

    Returns
    -------
    collections.deque
        ordered iterable of component object URLs
    """
    logger.debug(f"Retrieving dependency chain for '{object_url}'")
    _local_uri, _ = fdp_req.split_api_url(object_url)

    def _dependency_of(url_list: collections.deque, item: str, _dependency_list: dict):
        if item in url_list:
            return
        url_list.appendleft(item)
        try:
            _results = fdp_req.url_get(item, token)
        except Exception:
            _results = {}
        _results.update((substitutes or {}).get(item, {}))
        _type = fdp_req.get_obj_type_from_url(item, token)
        for req, val in _results.items():
            if req in _dependency_list[_type] and val:
                if isinstance(val, list):
                    for url in val:
                        _dependency_of(url_list, url, _dependency_list)
                else:
                    _dependency_of(url_list, val, _dependency_list)

    # Ordering is important so use a deque to preserve
    _object_dependency_list = fdp_req.get_dependency_listing(_local_uri, token)
    _urls = collections.deque()
    _dependency_of(_urls, object_url, _object_dependency_list)

    return _urls


def pull_all_namespaces(
    local_uri: str, remote_uri: str, local_token: str, remote_token: str
) -> typing.List[str]:
    """Pull all namespaces from a remote registry

    This ensures a user does not try to register locally a namespace that
    already exists on the remote and so lowers the risk of conflicting
    metadata when running a pull

    Parameters
    ----------
    local_uri : str
        endpoint of the local registry
    remote_uri : str
        endpoint of the remote registry
    local_token : str
        access token for the local registry
    remote_token : str
        access token for the remote registry

    Returns
    -------
    typing.List[str]
        list of identified namespaces
    """
    logger.debug("Pulling all namespaces to local registry")

    _remote_namespaces = fdp_req.get(remote_uri, "namespace", remote_token)

    logger.debug(
        "Found %s namespace%s on remote",
        len(_remote_namespaces),
        "s" if len(_remote_namespaces) != 1 else "",
    )

    if not _remote_namespaces:
        return []

    _writable_fields = fdp_req.get_writable_fields(local_uri, "namespace", local_token)

    for namespace in _remote_namespaces:
        _writable_data = {k: v for k, v in namespace.items() if k in _writable_fields}
        logger.debug("Writable local object data: %s", _writable_data)
        fdp_req.post_else_get(local_uri, "namespace", local_token, _writable_data)


def sync_dependency_chain(
    object_url: str,
    dest_uri: str,
    origin_uri: str,
    dest_token: str,
    origin_token: str,
    local_data_store: str = None,
    public: bool = False,
    substitutes: typing.Dict = None,
    placed: typing.Dict[str, str] = None,
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
        endpoint of the origin registry
    dest_token : str
        access token for the destination registry
    origin_token : str
        access token for the origin registry
    local_data_store : optional, str
        local data store path
    public : optional, bool
        is the associated storage_location public, defaults to True
    substitutes : optional, typing.Dict
        values to read in place of an object's own, keyed by the object's URL
        and then by field
    placed : optional, typing.Dict[str, str]
        URLs on the destination of objects that are recorded there already in
        their own way, keyed by the object's URL: these are not synchronised

    Returns
    -------
    typing.Dict[str, str]
        dictionary showing conversion from source registry URL to destination
    """
    logger.debug(f"Attempting to push object '{object_url}' to '{dest_uri}'")

    if not origin_token:
        raise fdp_exc.InternalError("Expected an origin token to be provided")

    _dependency_chain: collections.deque = get_dependency_chain(
        object_url, origin_token, substitutes
    )

    _new_urls: typing.Dict[str, str] = {k: "" for k in _dependency_chain}
    _writable_fields: typing.Dict[str, str] = {}

    # For every object (and the order) in the dependency chain
    # post the object then store the URL so it can be used to assemble those
    # further down the chain. The list grows as it is walked (see below)
    _to_sync = list(_dependency_chain)
    for object_url in _to_sync:
        if object_url in (placed or {}):
            _new_urls[object_url] = placed[object_url]
            continue
        logger.debug("Preparing object '%s'", object_url)
        # Retrieve the data for the object from the registry
        _obj_data = fdp_req.url_get(object_url, token=origin_token)
        _obj_data.update((substitutes or {}).get(object_url, {}))

        # Deduce the object type from its URL
        _obj_type = fdp_req.get_obj_type_from_url(object_url, token=origin_token)

        # A code repository's release points at its object, so nothing in the
        # chain leads to it: it is synchronised after the object
        _release = None
        if _obj_type == "object":
            _release = _obj_data.get("code_repo_release")
        if _release and _release not in _new_urls:
            _to_sync.append(_release)
            _new_urls[_release] = ""

        if _obj_type not in _writable_fields:
            _writable_fields[_obj_type] = fdp_req.get_writable_fields(
                origin_uri, _obj_type, origin_token
            )

        # Filter object data to only the writable fields
        _writable_data = {
            k: v for k, v in _obj_data.items() if k in _writable_fields[_obj_type]
        }

        logger.debug("Writable local object data: %s", _writable_data)

        _new_url = _get_new_url(
            origin_uri=origin_uri,
            origin_token=origin_token,
            dest_uri=dest_uri,
            dest_token=dest_token,
            object_url=object_url,
            new_urls=_new_urls,
            writable_data=_writable_data,
            object_data=_obj_data,
            local_data_store=local_data_store,
            public=public,
        )

        if not fdp_util.is_api_url(dest_uri, _new_url):
            raise fdp_exc.InternalError(
                f"Expected new URL '{_new_url}' to be compatible with \
                    destination URI '{dest_uri}'"
            )

        # Fill the new URLs dictionary with the result
        _new_urls[object_url] = _new_url

    return _new_urls


# Whether a storage_root record is on the pushing machine's file system
def _is_local_root(storage_root: typing.Dict) -> bool:
    return storage_root.get("root", "").startswith("file://")


# Whether an object record has a file to move between registries. One with no
# storage location has none; nor has a code repository, whose location is the
# repository and whose hash is a commit. The registry says an object is a code
# repository only by its role: a release of it, or a code run naming it
def _has_file(object_data: typing.Dict, token: str) -> bool:
    if not object_data.get("storage_location"):
        return False
    if object_data.get("code_repo_release"):
        return False
    _registry_uri, _ = fdp_req.split_api_url(object_data["url"])
    return not fdp_req.get(
        _registry_uri,
        "code_run",
        token,
        params={"code_repo": fdp_req.get_obj_id_from_url(object_data["url"])},
    )


# The destination's object for a code repository - an object with a location
# and no file to move - as a local registry keeps one for a commit: under a
# storage root with the same root, a public location with the commit's hash,
# whatever its path, and of the objects there the one a release is registered
# on, else the oldest. None where the destination holds none, or the object is
# not a code repository
def _dest_repository(
    origin_object: typing.Dict, dest_uri: str, dest_token: str, origin_token: str
) -> typing.Optional[typing.Dict]:
    if not origin_object.get("storage_location") or _has_file(
        origin_object, origin_token
    ):
        return None
    _location = fdp_req.url_get(origin_object["storage_location"], origin_token)
    _root = fdp_req.url_get(_location["storage_root"], origin_token)
    _objects = []
    for _dest_root in fdp_req.get(
        dest_uri, "storage_root", dest_token, params={"root": _root["root"]}
    ):
        for _dest_location in fdp_req.get(
            dest_uri,
            "storage_location",
            dest_token,
            params={
                "hash": _location["hash"],
                "public": True,
                "storage_root": fdp_req.get_obj_id_from_url(_dest_root["url"]),
            },
        ):
            _objects += fdp_req.get(
                dest_uri,
                "object",
                dest_token,
                params={
                    "storage_location": fdp_req.get_obj_id_from_url(
                        _dest_location["url"]
                    )
                },
            )
    _released = [_object for _object in _objects if _object.get("code_repo_release")]
    if not _objects:
        return None
    return min(
        _released or _objects,
        key=lambda _object: int(fdp_req.get_obj_id_from_url(_object["url"])),
    )


# The destination's object and location for a code repository, keyed by the
# origin's, as sync_dependency_chain takes them to be placed; empty where
# _dest_repository finds none
def _repository_placed(
    origin_object_url: str, dest_uri: str, dest_token: str, origin_token: str
) -> typing.Dict[str, str]:
    _origin_object = fdp_req.url_get(origin_object_url, origin_token)
    _held = _dest_repository(_origin_object, dest_uri, dest_token, origin_token)
    if not _held:
        return {}
    return {
        origin_object_url: _held["url"],
        _origin_object["storage_location"]: _held["storage_location"],
    }


# URL of a registry's own data store: the storage_root whose root is the
# registry's data/ path, beside its api/, from which it serves stored files.
# A registry whose site URL differs from its API's address records that root
# under another name; its first storage_root is then taken, as set_site_info
# creates the data store first.
def _remote_data_store_url(registry_uri: str, token: str) -> str:
    _registry_uri = fdp_util.check_trailing_slash(registry_uri)
    _root = urllib.parse.urljoin(_registry_uri, "../data/")
    _data_store = fdp_req.get(
        registry_uri, "storage_root", token, params={"root": _root}
    )
    if _data_store:
        return _data_store[0]["url"]
    _fallback = urllib.parse.urljoin(_registry_uri, "storage_root/1/")
    logger.warning(
        "Registry '%s' has no storage root '%s'; assuming its data store is "
        "'%s'. Its site URL may not match its address.",
        registry_uri,
        _root,
        _fallback,
    )
    return _fallback


def _get_new_url(
    origin_uri: str,
    origin_token: str,
    dest_uri: str,
    dest_token: str,
    object_url: str,
    new_urls: typing.Dict,
    writable_data: typing.Dict,
    object_data={},
    local_data_store=None,
    public=False,
) -> typing.Tuple[typing.Dict, typing.List]:
    """Internal Function to return a registry entry from the remote registry
    given an origin entry URL
    If the entry does not exist it will be created

    Args:
        origin_uri (str): Url of the origin registry
        origin_token (str): Token of the origin registry
        dest_uri (str): Url of the destination registry
        dest_token (str): Token of the destination registry
        object_url (str): Url of the destination registry entry
        new_urls (typing.Dict): If the entry contains registry URLS,
            a list of remote registry URLs for these
        writable_data (typing.Dict): Dictionary of writable fields for
            the given entity type

    Raises:
        fdp_exc.RegistryError: If the given new_urls are not valid for
            the entry a RegistryError will be raised
        fdp_exc.InternalError: If the origin url and the destination URLs
            are the same an InternalError with be raised

    Returns:
        typing.Tuple[typing.Dict, typing.List]: A tuple containing the
            destination registry entity
    """
    _new_obj_data: typing.Dict[str, typing.Any] = {}
    _url_fields: typing.List[str] = []

    # Iterate through the object data, for any values which are URLs
    # substitute the local URL for the created remote ones.
    # For the first object there should be no URL values at all.
    for key, value in writable_data.items():
        # Check if value is URL
        _not_str = not isinstance(value, str)
        _not_url = isinstance(value, str) and not fdp_util.is_api_url(origin_uri, value)
        if _not_str or _not_url:
            _new_obj_data[key] = value
            continue
        # Store which fields have URLs to use later
        _url_fields.append(key)
        # Make sure that a URL for the component does exist
        if value not in new_urls:
            raise fdp_exc.RegistryError(
                f"Expected a URL on '{dest_uri}' for component '{key}', "
                f"whose value '{value}' should have been synchronised "
                "before it."
            )

        # Retrieve from the new URLs the correct value and substitute
        _new_obj_data[key] = new_urls[value]

    # Filters are all variables returned by 'filter_fields' request for a
    # given object minus any variables which have a URL value
    # (as remote URL will never match local)

    _obj_type = fdp_req.get_obj_type_from_url(object_url, token=origin_token)

    _filters = {
        k: v
        for k, v in _new_obj_data.items()
        if k in fdp_req.get_filter_variables(dest_uri, _obj_type, dest_token)
        and isinstance(v, str)
        and k not in _url_fields
    }

    logger.debug(f"Pushing member '{object_url}' to '{dest_uri}'")

    if dest_uri == origin_uri:
        raise fdp_exc.InternalError("Cannot push object to its source address")

    # If Public is true then any files will be uploaded to the remote registry
    # If local_data_store is set we're pulling to a local registry
    if public and not local_data_store:
        # A local data store is any root on the pusher's own file system,
        # however many projects share the local registry; it is replaced by
        # the remote data store. Other roots (e.g. https://github.com/) are
        # pushed as they are.
        if _obj_type == "storage_root" and _is_local_root(object_data):
            return _remote_data_store_url(dest_uri, dest_token)
        # If the current object is a storage_location
        elif _obj_type == "storage_location":
            # Again if the storage_root is a local data store
            if _is_local_root(
                fdp_req.url_get(object_data["storage_root"], origin_token)
            ):
                _remote_storage_root_url = _remote_data_store_url(
                    dest_uri, dest_token
                )
                # Update the new object path and filter path
                _new_obj_data["path"] = _filters["path"] = _new_obj_data["hash"]
                # Update the new object storage_root
                _new_obj_data["storage_root"] = _remote_storage_root_url
                # Update the filters for storage_root to the ID
                _filters["storage_root"] = fdp_req.get_obj_id_from_url(
                    _remote_storage_root_url
                )

    if _obj_type == "object":
        _new_obj_data["authors"] = []
        _new_author_urls = []
        _filters["authors"] = []
        _tmp_obj = fdp_req.url_get(object_url, origin_token)
        if "authors" in _tmp_obj:
            for _author_url in _tmp_obj["authors"]:
                _new_author_urls.append(
                    get_new_author_url(
                        origin_uri, origin_token, dest_uri, dest_token, _author_url
                    )
                )
        for _new_author_url in _new_author_urls:
            _new_obj_data["authors"].append(_new_author_url)
            _filters["authors"].append(fdp_req.get_obj_id_from_url(_new_author_url))

    if _obj_type == "author" and "identifier" in _new_obj_data:
        _author = fdp_req.get_author_exists(
            dest_uri, token=dest_token, identifier=_new_obj_data["identifier"]
        )
        if _author:
            return _author

    return fdp_req.post_else_get(
        dest_uri,
        _obj_type,
        data=_new_obj_data,
        token=dest_token,
        params=_filters,
    )


def sync_author(
    origin_uri: str, dest_uri: str, dest_token: str, origin_token: str, identifier: str
) -> None:
    current_author = fdp_req.get(
        origin_uri, "author", origin_token, params={"identifier": identifier}
    )
    if not current_author:
        raise fdp_exc.RegistryError(
            f"No author matching {identifier}, on local registry",
            "Have you run fair init?",
        )
    current_author_url = current_author[0]["url"]
    new_urls = sync_dependency_chain(
        current_author_url, dest_uri, origin_uri, dest_token, origin_token
    )
    if not new_urls:
        raise fdp_exc.RegistryError(
            f"Author {identifier}, could not be pushed to {dest_uri}"
        )
    new_author_url = new_urls[current_author_url]
    if not new_author_url:
        raise fdp_exc.RegistryError(
            f"Author {identifier}, was not be pushed to {dest_uri}"
        )
    return new_author_url


def sync_user_author(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    author_url: str,
    remote_user: str,
) -> None:
    _ = origin_uri  # prevent unused variable warning
    _ = origin_token  # prevent unused variable warning
    current_user = fdp_req.get(
        dest_uri, "users", dest_token, params={"username": remote_user}
    )
    if not current_user:
        raise fdp_exc.RegistryError(
            f"No user matching {remote_user}, on remote registry",
            "Does your Remote username match the remote registry user?",
        )
    current_user_url = current_user[0]["url"]
    fdp_store.store_user_author(dest_uri, dest_token, current_user_url, author_url)


def get_new_author_url(
    origin_uri: str, origin_token: str, dest_uri: str, dest_token: str, author_url: str
) -> None:
    _author_data = fdp_req.url_get(author_url, token=origin_token)
    _writable_fields = fdp_req.get_writable_fields(origin_uri, "author", origin_token)
    _writable_data = {k: v for k, v in _author_data.items() if k in _writable_fields}
    return _get_new_url(
        origin_uri=origin_uri,
        origin_token=origin_token,
        dest_uri=dest_uri,
        dest_token=dest_token,
        object_url=author_url,
        new_urls={},
        writable_data=_writable_data,
    )


class PushContext:
    """What one push or pull has done so far

    Within one, each data product is sent, or found on the destination and
    checked, once; and each code run that a push reaches is linked with its
    inputs once, after the data products are sent.

    Attributes
    ----------
    handled : typing.Set[str]
        the data products sent or checked, each as 'namespace:name@vversion'
    dest_components : typing.Dict[str, str]
        the destination's component for each origin component looked up
    to_link : typing.Dict[str, typing.Set[str]]
        the code runs to link, by UUID, each with the origin components it
        wrote that were sent and are not yet linked as its outputs
    linked : typing.Set[str]
        the code runs, by UUID, already linked with their inputs
    waiting : typing.Set[typing.Tuple[str, str]]
        the objects, by their URLs on the origin and the destination, whose
        quality-control record waits for its document to be sent
    """

    def __init__(self) -> None:
        self.handled: typing.Set[str] = set()
        self.dest_components: typing.Dict[str, str] = {}
        self.to_link: typing.Dict[str, typing.Set[str]] = {}
        self.linked: typing.Set[str] = set()
        self.waiting: typing.Set[typing.Tuple[str, str]] = set()


@contextlib.contextmanager
def push_context(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    force: bool = False,
) -> typing.Generator[PushContext, None, None]:
    """Hold the context of one push or pull, linking the runs it reached at the end

    A push that fails still links what it sent before the failure, as far as
    it can, and the failure is then raised. A quality-control record whose
    document was sent after its object is sent at the end.

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    dest_token : str
        token for the destination data registry
    origin_token : str
        token for the origin data registry
    remote_label : str
        name of the remote registry, as messages name it
    force : bool, optional
        whether to send again a data product the destination holds with the
        same contents, by default False
    """
    _context = PushContext()
    _linking = (origin_uri, dest_uri, dest_token, origin_token, remote_label)
    try:
        yield _context
    except Exception:
        link_runs(*_linking, _context, force, best_effort=True)
        raise
    link_runs(*_linking, _context, force)
    for _origin_object, _dest_object in sorted(_context.waiting):
        sync_attached(
            origin_uri,
            dest_uri,
            origin_token,
            dest_token,
            _origin_object,
            _dest_object,
        )


# The destination's component for an origin component, looked up once in a
# push
def _dest_component(
    context: PushContext,
    origin_component_url: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
) -> str:
    if origin_component_url not in context.dest_components:
        context.dest_components[origin_component_url] = get_dest_component_url(
            origin_component_url, dest_uri, dest_token, origin_token
        )
    return context.dest_components[origin_component_url]


def link_runs(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    context: PushContext,
    force: bool = False,
    best_effort: bool = False,
) -> None:
    """Link each code run a push reached with what it read and wrote

    A run is linked with its inputs once, any the destination lacks being sent
    first, which may reach more runs, linked in turn; and with each output the
    push sent.

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    dest_token : str
        token for the destination data registry
    origin_token : str
        token for the origin data registry
    remote_label : str
        name of the remote registry, as messages name it
    context : PushContext
        the push's context, naming the runs to link
    force : bool, optional
        whether to send again an input the destination holds with the same
        contents, by default False
    best_effort : bool, optional
        pass over a run that cannot be linked, saying so, as after a push
        that failed, by default False
    """
    while _uuid := next(
        (
            _pending
            for _pending, _outputs in context.to_link.items()
            if _outputs or _pending not in context.linked
        ),
        None,
    ):
        _outputs = context.to_link[_uuid]
        context.to_link[_uuid] = set()
        try:
            _inputs = []
            if _uuid not in context.linked:
                context.linked.add(_uuid)
                _code_run = fdp_req.get(
                    origin_uri, "code_run", origin_token, params={"uuid": _uuid}
                )[0]
                _inputs = get_dest_inputs(
                    _code_run["inputs"],
                    origin_uri,
                    dest_uri,
                    dest_token,
                    origin_token,
                    remote_label,
                    force,
                    context=context,
                )
            sync_code_run(
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                _uuid,
                inputs=_inputs,
                outputs=[
                    _dest_component(
                        context, _component, dest_uri, dest_token, origin_token
                    )
                    for _component in sorted(_outputs)
                ],
            )
        except Exception as e:
            if not best_effort:
                raise
            logger.warning(
                f"Code run {_uuid} was not linked on '{remote_label}': {e}"
            )


def sync_data_products(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    data_products: typing.List[str],
    local_data_store: str = None,
    force: bool = False,
    caches: typing.Dict[str, str] = None,
    context: PushContext = None,
) -> None:
    """Transfer data products from one registry to another

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
    remote_label : str
        name of remote in listing
    data_products : typing.List[str]
        list of data products to push
    local_data_store : optional, str
        specified when pulling from remote registry to local
    force : optional, bool
        whether to send again a data product the destination holds with the
        same contents, as after a push that failed part way, by default
        False. One it holds with other contents is refused whatever this says
    caches : optional, typing.Dict[str, str]
        when pulling, a copy of a data product's file that the user holds,
        taken in place of the download, by data product as in data_products
    context : optional, PushContext
        the context of the push or pull this is part of, whose owner links the
        runs reached; by default one of its own, the runs linked before this
        returns
    """
    if context is None:
        with push_context(
            origin_uri, dest_uri, dest_token, origin_token, remote_label, force
        ) as _context:
            return sync_data_products(
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                remote_label,
                data_products,
                local_data_store,
                force,
                caches,
                context=_context,
            )
    for data_product in data_products:
        if data_product in context.handled:
            continue
        namespace, name, version = re.split("[:@]", data_product)

        if _existing_namespace := fdp_req.get(
            dest_uri,
            "namespace",
            params={SEARCH_KEYS["namespace"]: namespace},
            token=dest_token,
        ):
            _namespace_id = fdp_req.get_obj_id_from_url(_existing_namespace[0]["url"])
            if _existing := fdp_req.get(
                dest_uri,
                "data_product",
                dest_token,
                params={
                    "namespace": _namespace_id,
                    "name": name,
                    "version": version.replace("v", ""),
                },
            ):
                # Refused here too, for a caller that did not look first
                refuse_conflicts(
                    origin_uri,
                    dest_uri,
                    origin_token,
                    dest_token,
                    [data_product],
                    remote_label,
                    pulling=bool(local_data_store),
                )
                if not force:
                    logger.info(
                        f"Data product '{data_product}' is on '{remote_label}' "
                        "already, with the same contents"
                    )
                    # Records attached since it was sent go now
                    sync_attached(
                        origin_uri,
                        dest_uri,
                        origin_token,
                        dest_token,
                        _find_data_product(origin_uri, origin_token, data_product)[
                            "object"
                        ],
                        _existing[0]["object"],
                        waiting=context.waiting,
                    )
                    context.handled.add(data_product)
                    continue

        # Convert namespace name to an ID for retrieval
        _namespaces = fdp_req.get(
            origin_uri,
            "namespace",
            params={SEARCH_KEYS["namespace"]: namespace},
            token=origin_token,
        )

        if not _namespaces:
            raise fdp_exc.RegistryError(
                f"Failed to find namespace '{namespace}' on registry {origin_uri}"
            )

        _namespace_id = fdp_req.get_obj_id_from_url(_namespaces[0]["url"])

        query_params = {
            "namespace": _namespace_id,
            "name": name,
            "version": version.replace("v", ""),
        }

        result = fdp_req.get(
            origin_uri, "data_product", params=query_params, token=origin_token
        )

        if not result:
            raise fdp_exc.RegistryError(
                f"Failed to find data product matching descriptor '{data_product}'"
            )
        result = result[0]

        # Held separately because `result` becomes the external object below,
        # and the file is fetched by the data product's own namespace, name
        # and version
        _data_product = result

        result_object = fdp_req.url_get(result["object"], token=origin_token)
        # A data product may have no file to move - no storage location, or a
        # code repository's: there are then only its records to synchronise,
        # its location as it is
        _moves_file = _has_file(result_object, origin_token)
        _is_public = False
        if _moves_file:
            _is_public = fdp_req.url_get(
                result_object["storage_location"], token=origin_token
            )["public"]

        # if the data_product is an external object sync that first
        _substitutes = {}
        if result["external_object"]:
            result = fdp_req.url_get(result["external_object"], token=origin_token)
            # An external object may be shared by several data products, and
            # its record names only one of them: the one it is synchronised
            # with here is this data product
            _substitutes[result["url"]] = {
                "data_product": _data_product["url"]
            }

        # The file is moved before its records are written, so that a failure
        # leaves no record, and the next attempt does not take this data
        # product for one already there. If local_data_store assume we're
        # syncing from remote to local, else a public file is uploaded to
        # object storage
        _placed = {}
        if not _moves_file:
            # A code repository takes the destination's object for its commit
            _placed = _repository_placed(
                result_object["url"], dest_uri, dest_token, origin_token
            )
        elif local_data_store:
            logger.debug("Retrieving files from remote registry data storage")
            _placed = _place_pulled_file(
                dest_uri,
                dest_token,
                origin_token,
                local_data_store,
                _data_product,
                (caches or {}).get(data_product),
            )
        elif _is_public:
            upload_object(
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                result_object["url"],
            )

        _dest_urls = sync_dependency_chain(
            object_url=result["url"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token,
            local_data_store=local_data_store,
            public=_is_public,
            substitutes=_substitutes,
            placed=_placed,
        )
        # Before the runs below, which are mapped onto its named components
        sync_attached(
            origin_uri,
            dest_uri,
            origin_token,
            dest_token,
            result_object["url"],
            _dest_urls[result_object["url"]],
            waiting=context.waiting,
        )
        context.handled.add(data_product)
        # Going from local to remote, each run that read or wrote it is linked
        # once every data product is sent, a writer with what it wrote that was
        # sent. Linking here would walk a reading run's inputs again for each
        # data product sent, sending the next inside the walk
        if not local_data_store:
            for origin_component_url in result_object["components"]:
                _component_id = fdp_req.get_obj_id_from_url(
                    origin_component_url
                )
                for origin_input_code_run in fdp_req.get(
                    origin_uri,
                    "code_run",
                    origin_token,
                    params={"inputs": _component_id},
                ):
                    context.to_link.setdefault(
                        origin_input_code_run["uuid"], set()
                    )
                for origin_output_code_run in fdp_req.get(
                    origin_uri,
                    "code_run",
                    origin_token,
                    params={"outputs": _component_id},
                ):
                    context.to_link.setdefault(
                        origin_output_code_run["uuid"], set()
                    ).add(origin_component_url)
        logger.info(f"Synced Data Product: {data_product}")


def get_dest_component_url(
    origin_component_url: str, dest_uri: str, dest_token: str, origin_token: str
) -> str:
    """Get the destination component url for the given origin component url

    Args:
        origin_component_url (str): Url of the origin component
        dest_uri (str): Destination api uri
        dest_token (str): Destination Token
        origin_token (str): Origin Token

    Raises:
        fdp_exc.RegistryError: If the destination component does not exist a RegistryError will be raised

    Returns:
        str: Url of the destination component
    """
    origin_component = fdp_req.url_get(origin_component_url, token=origin_token)
    # A data product may be of a code repository, so its object may be one
    dest_object_url = get_dest_object_url(
        origin_component["object"],
        dest_uri,
        dest_token,
        origin_token,
        repository=True,
    )
    dest_component = fdp_req.get(
        dest_uri,
        "object_component",
        dest_token,
        params={
            "name": origin_component["name"],
            "object": fdp_req.get_obj_id_from_url(dest_object_url),
        },
    )
    if not dest_component:
        raise fdp_exc.RegistryError(
            f'Failed to access component with object: {fdp_req.get_obj_id_from_url(dest_object_url)} and name {origin_component["name"]} on remote registry'
        )
    return dest_component[0]["url"]


# The records that point at an object and are sent with it, by kind: the
# fields that name one among those of its object, and its other fields. An
# object has one quality-control record and one code release
_ATTACHED = {
    "object_component": (("name",), ("description",)),
    "licence": (("identifier",), ("licence_info",)),
    "keyword": (("keyphrase",), ("identifier",)),
    "key_value": (("key",), ("value",)),
    "quality_controlled": ((), ("document",)),
    "code_repo_release": ((), ("name", "version", "website")),
}


# What names a record among those of its kind on one object, as the
# registry's constraints do: a licence with no identifier is named by its text
def _name_on_object(kind: str, record: typing.Dict) -> typing.Tuple:
    if kind == "licence" and not record.get("identifier"):
        return ("licence_info", record.get("licence_info"))
    return tuple(record.get(_field) for _field in _ATTACHED[kind][0])


# A component's counterpart on the destination, None where it is not there
def _dest_component_or_none(
    origin_component_url: str, dest_uri: str, dest_token: str, origin_token: str
) -> typing.Optional[str]:
    try:
        return get_dest_component_url(
            origin_component_url, dest_uri, dest_token, origin_token
        )
    except fdp_exc.RegistryError:
        return None


def sync_attached(
    origin_uri: str,
    dest_uri: str,
    origin_token: str,
    dest_token: str,
    origin_object_url: str,
    dest_object_url: str,
    waiting: typing.Set[typing.Tuple[str, str]] = None,
) -> None:
    """Send the records that point at an object to its copy on another registry

    These are its named components, licences, keywords, key-values,
    quality-control record and code release, and the issues on its
    components. Each is posted where the destination's object has none of its
    name. An issue is found by its UUID, else as an issue with its severity
    and description on one of its components there, and is given the
    components it is on in both registries; its components on objects the
    destination does not hold are left out. A record the destination holds
    with other values is kept as it is, with a warning.

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    origin_token : str
        token for the origin data registry
    dest_token : str
        token for the destination data registry
    origin_object_url : str
        URL of the object on the origin registry
    dest_object_url : str
        URL of the same object on the destination registry
    waiting : typing.Set[typing.Tuple[str, str]], optional
        where to add the two objects' URLs, saying nothing, if the
        quality-control record's document is not on the destination, for a
        caller that will send it later; by default it is left out with a
        warning
    """
    _origin_id = fdp_req.get_obj_id_from_url(origin_object_url)
    _dest_id = fdp_req.get_obj_id_from_url(dest_object_url)
    for _kind, (_names, _others) in _ATTACHED.items():
        _held = {
            _name_on_object(_kind, _record): _record
            for _record in fdp_req.get(
                dest_uri, _kind, dest_token, params={"object": _dest_id}
            )
        }
        for _record in fdp_req.get(
            origin_uri, _kind, origin_token, params={"object": _origin_id}
        ):
            # The registry makes an object's whole_object component itself
            if _record.get("whole_object"):
                continue
            _data = {_field: _record.get(_field) for _field in _names + _others}
            if _kind == "quality_controlled":
                try:
                    _data["document"] = get_dest_object_url(
                        _record["document"], dest_uri, dest_token, origin_token
                    )
                except fdp_exc.RegistryError:
                    if waiting is not None:
                        waiting.add((origin_object_url, dest_object_url))
                        continue
                    logger.warning(
                        "The quality-control record of '%s' is not sent to "
                        "'%s': its document is not there",
                        origin_object_url,
                        dest_uri,
                    )
                    continue
            _there = _held.get(_name_on_object(_kind, _data))
            if _there is None:
                try:
                    fdp_req.post(
                        dest_uri,
                        _kind,
                        dest_token,
                        {"object": dest_object_url, **_data},
                    )
                except fdp_exc.RegistryAPICallError as e:
                    if e.error_code != 409:
                        raise
                    logger.warning(
                        "The %s %s of '%s' is not sent to '%s': it conflicts "
                        "with one there",
                        _kind,
                        _data,
                        origin_object_url,
                        dest_uri,
                    )
            elif any((_there.get(_f) or None) != (_data[_f] or None) for _f in _others):
                logger.warning(
                    "The %s of '%s' on '%s' is kept as it is there, %s, not "
                    "as sent, %s",
                    _kind,
                    dest_object_url,
                    dest_uri,
                    {_f: _there.get(_f) for _f in _names + _others},
                    _data,
                )
    _sync_issues(
        origin_uri,
        dest_uri,
        origin_token,
        dest_token,
        origin_object_url,
        dest_object_url,
    )


# The issues on an object's components, sent to its copy on the destination
# as sync_attached describes
def _sync_issues(
    origin_uri: str,
    dest_uri: str,
    origin_token: str,
    dest_token: str,
    origin_object_url: str,
    dest_object_url: str,
) -> None:
    _dest_components = {
        _component["name"]: _component["url"]
        for _component in fdp_req.get(
            dest_uri,
            "object_component",
            dest_token,
            params={"object": fdp_req.get_obj_id_from_url(dest_object_url)},
        )
    }
    _issue_urls: typing.List[str] = []
    for _component in fdp_req.get(
        origin_uri,
        "object_component",
        origin_token,
        params={"object": fdp_req.get_obj_id_from_url(origin_object_url)},
    ):
        _issue_urls += [
            _url for _url in _component["issues"] if _url not in _issue_urls
        ]
    for _issue_url in _issue_urls:
        _issue = fdp_req.url_get(_issue_url, origin_token)
        _components = []
        for _component_url in _issue["component_issues"]:
            _component = fdp_req.url_get(_component_url, origin_token)
            if _component["object"] == origin_object_url:
                _component_there = _dest_components.get(_component["name"])
            else:
                _component_there = _dest_component_or_none(
                    _component_url, dest_uri, dest_token, origin_token
                )
            if _component_there:
                _components.append(_component_there)
        _data = {
            "severity": _issue["severity"],
            "description": _issue["description"],
        }
        _there = fdp_req.get(
            dest_uri, "issue", dest_token, params={"uuid": _issue["uuid"]}
        ) or _same_issue(dest_token, _data, _components)
        if not _there:
            fdp_req.post(
                dest_uri,
                "issue",
                dest_token,
                {"uuid": _issue["uuid"], "component_issues": _components, **_data},
            )
            continue
        _there = _there[0]
        if any(_there[_field] != _value for _field, _value in _data.items()):
            logger.warning(
                "The issue %s on '%s' is kept as it is there, %s, not as sent, %s",
                _there["uuid"],
                dest_uri,
                {_field: _there[_field] for _field in _data},
                _data,
            )
        _all = sorted(set(_there["component_issues"]) | set(_components))
        if _all != sorted(_there["component_issues"]):
            fdp_req._access(
                _there["url"],
                "patch",
                dest_token,
                data={"component_issues": _all},
            )


# An issue a registry holds with the given severity and description on one of
# the given components, which a client takes for the same issue raised again,
# as a list of none or one
def _same_issue(
    token: str, data: typing.Dict, components: typing.List[str]
) -> typing.List[typing.Dict]:
    for _component_url in components:
        for _issue_url in fdp_req.url_get(_component_url, token)["issues"]:
            _issue = fdp_req.url_get(_issue_url, token)
            if all(_issue[_field] == _value for _field, _value in data.items()):
                return [_issue]
    return []


def sync_code_runs(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    code_runs: typing.List[str],
    local_data_store: str = None,
    force: bool = False,
    context: PushContext = None,
) -> None:
    """Transfer data code_run(s) from one registry to another

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
    remote_label : str
        name of remote in listing
    code_runs : typing.List[str]
        list of code_run(s) to push
    local_data_store : optional, str
        specified when pulling from remote registry to local
    force : optional, bool
        whether to send again a data product the destination holds with the
        same contents, as after a push that failed part way, by default
        False. One it holds with other contents is refused whatever this says
    context : optional, PushContext
        the context of the push this is part of, whose owner links the other
        runs reached; by default one of its own, those runs linked before this
        returns
    """
    if context is None:
        with push_context(
            origin_uri, dest_uri, dest_token, origin_token, remote_label, force
        ) as _context:
            return sync_code_runs(
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                remote_label,
                code_runs,
                local_data_store,
                force,
                context=_context,
            )
    for code_run_uuid in code_runs:
        _code_run = fdp_req.get(
            origin_uri, "code_run", origin_token, params={"uuid": code_run_uuid}
        )
        # Ensure the code run exists on the local registry
        if not _code_run:
            raise fdp_exc.RegistryError(
                f"Failed to find code_run '{code_run_uuid} on local registry'"
            )
        _coderun = _code_run[0]

        # Each component's data products, fetched once for all that follows
        _by_component = _component_data_products(_coderun, origin_token)
        _inputs_data_products, _outputs_data_products = _code_run_data_products(
            _coderun, origin_token, _by_component
        )
        _origin_data_products_formatted = _inputs_data_products + _outputs_data_products
        # Sync all the data products associated with the code run
        sync_data_products(
            origin_uri,
            dest_uri,
            dest_token,
            origin_token,
            remote_label,
            _origin_data_products_formatted,
            local_data_store,
            force,
            context=context,
        )

        # Each component the run read or wrote, as the destination names it.
        # One of an object with no data product was not sent, and is left out
        _dest_inputs = [
            _dest_component(
                context, _component, dest_uri, dest_token, origin_token
            )
            for _component in _coderun["inputs"]
            if _by_component[_component]
        ]
        _dest_outputs = [
            _dest_component(
                context, _component, dest_uri, dest_token, origin_token
            )
            for _component in _coderun["outputs"]
            if _by_component[_component]
        ]
        logger.debug(
            f"attempting to sync coderun {code_run_uuid} with inputs {_dest_inputs} and outputs {_dest_outputs}"
        )
        sync_code_run(
            origin_uri,
            dest_uri,
            dest_token,
            origin_token,
            code_run_uuid,
            _dest_inputs,
            _dest_outputs,
        )
        # Linked with everything it read and wrote
        context.linked.add(code_run_uuid)
        context.to_link[code_run_uuid] = set()
        logger.info(f"Synced Code Run: {code_run_uuid}")


# The data products of the object of each component a code run read or wrote,
# by component, each component fetched once
def _component_data_products(
    code_run: typing.Dict, token: str
) -> typing.Dict[str, typing.List[str]]:
    return {
        _component: get_data_products_from_component(_component, token)
        for _component in dict.fromkeys(code_run["inputs"] + code_run["outputs"])
    }


# The data products of the objects a code run read and wrote, each as
# 'namespace:name@vversion': those it read, and those it wrote. Each is
# formatted once, and the components' data products are taken from
# by_component where given
def _code_run_data_products(
    code_run: typing.Dict,
    token: str,
    by_component: typing.Optional[typing.Dict[str, typing.List[str]]] = None,
) -> typing.Tuple[typing.List[str], typing.List[str]]:
    if by_component is None:
        by_component = _component_data_products(code_run, token)
    _formatted: typing.Dict[str, str] = {}

    def _format(components: typing.List[str]) -> typing.List[str]:
        _urls = [_url for _component in components for _url in by_component[_component]]
        for _url in _urls:
            if _url not in _formatted:
                _formatted[_url] = format_data_product(_url, token)
        return [_formatted[_url] for _url in _urls]

    return _format(code_run["inputs"]), _format(code_run["outputs"])


def code_run_data_products(
    uri: str, token: str, code_runs: typing.List[str]
) -> typing.List[str]:
    """Return the data products code runs read and wrote

    Parameters
    ----------
    uri : str
        registry URL
    token : str
        token for the registry
    code_runs : typing.List[str]
        UUIDs of the code runs

    Returns
    -------
    typing.List[str]
        each data product once, as 'namespace:name@vversion'
    """
    _data_products: typing.List[str] = []
    for _uuid in code_runs:
        _code_run = fdp_req.get(uri, "code_run", token, params={"uuid": _uuid})
        if not _code_run:
            raise fdp_exc.RegistryError(f"Failed to find code_run '{_uuid}' on '{uri}'")
        _inputs, _outputs = _code_run_data_products(_code_run[0], token)
        _data_products += [d for d in _inputs + _outputs if d not in _data_products]
    return _data_products


def unsent_producers(
    origin_uri: str,
    dest_uri: str,
    origin_token: str,
    dest_token: str,
    code_runs: typing.List[str],
    data_products: typing.List[str],
) -> typing.List[typing.Tuple[str, str]]:
    """Return the runs that made what was pushed that the destination lacks

    The walk starts from the given code runs and the runs that wrote the given
    data products, and goes back through the runs that wrote each one's
    inputs, as far as the runs the destination holds.

    Parameters
    ----------
    origin_uri : str
        origin data registry URL
    dest_uri : str
        destination data registry URL
    origin_token : str
        token for the origin data registry
    dest_token : str
        token for the destination data registry
    code_runs : typing.List[str]
        UUIDs of the code runs pushed
    data_products : typing.List[str]
        data products pushed, as 'namespace:name@vversion'

    Returns
    -------
    typing.List[typing.Tuple[str, str]]
        each run the destination lacks, by UUID, with the data products of
        what it wrote that the walk came from
    """
    _components: typing.List[str] = []
    for _uuid in code_runs:
        for _code_run in fdp_req.get(
            origin_uri, "code_run", origin_token, params={"uuid": _uuid}
        ):
            _components += _code_run["inputs"]
    for _data_product in data_products:
        if _found := _find_data_product(origin_uri, origin_token, _data_product):
            _components += fdp_req.url_get(_found["object"], origin_token)["components"]
    _seen_components = set()
    _seen_runs = set(code_runs)
    _missing = []
    while _components:
        _component = _components.pop()
        if _component in _seen_components:
            continue
        _seen_components.add(_component)
        for _producer in fdp_req.get(
            origin_uri,
            "code_run",
            origin_token,
            params={"outputs": fdp_req.get_obj_id_from_url(_component)},
        ):
            if _producer["uuid"] in _seen_runs:
                continue
            _seen_runs.add(_producer["uuid"])
            if fdp_req.get(
                dest_uri, "code_run", dest_token, params={"uuid": _producer["uuid"]}
            ):
                _components += _producer["inputs"]
            else:
                _made = format_data_product_list(
                    get_data_products_from_component(_component, origin_token),
                    origin_token,
                )
                _missing.append((_producer["uuid"], ", ".join(_made)))
    return _missing


# A data product in a registry, by 'namespace:name@vversion', None where the
# registry has none
def _find_data_product(
    uri: str, token: str, data_product: str
) -> typing.Optional[typing.Dict]:
    namespace, name, version = re.split("[:@]", data_product)
    _namespace = fdp_req.get(
        uri, "namespace", token, params={SEARCH_KEYS["namespace"]: namespace}
    )
    if not _namespace:
        return None
    _found = fdp_req.get(
        uri,
        "data_product",
        token,
        params={
            "namespace": fdp_req.get_obj_id_from_url(_namespace[0]["url"]),
            "name": name,
            "version": version.replace("v", ""),
        },
    )
    return _found[0] if _found else None


# What tells a data product's contents apart from another's: its file's hash
# (a code repository's being its commit); else its source - a source being one
# file - by identifier, or alternate identifier and its type, title and
# release version; else nothing
def _contents(data_product: typing.Dict, token: str) -> typing.Tuple:
    _object = fdp_req.url_get(data_product["object"], token)
    if _object.get("storage_location"):
        return ("file", fdp_req.url_get(_object["storage_location"], token)["hash"])
    if data_product.get("external_object"):
        _source = fdp_req.url_get(data_product["external_object"], token)
        return (
            "source",
            tuple(
                _source.get(_field)
                for _field in (
                    "identifier",
                    "alternate_identifier",
                    "alternate_identifier_type",
                    "title",
                    "version",
                )
            ),
        )
    return ("nothing",)


# Contents as a refusal names them
def _describe_contents(contents: typing.Tuple) -> str:
    if contents[0] == "file":
        return f"a file with hash {contents[1]}"
    if contents[0] == "source":
        _identifier, _alternate, _type, _title, _version = contents[1]
        _named = _identifier or f"{_alternate} ({_type})"
        return f"no file, and the source {_named} titled '{_title}', release {_version}"
    return "no file and no source"


def refuse_conflicts(
    origin_uri: str,
    dest_uri: str,
    origin_token: str,
    dest_token: str,
    data_products: typing.List[str],
    remote_label: str,
    pulling: bool = False,
) -> None:
    """Refuse data products the destination holds under their names with other contents

    A data product is a namespace, a name and a version, and two registries
    holding the same three with other contents hold different things, which
    a push or a pull would otherwise take for one. Every data product is
    looked at before the refusal, which names them all.

    Parameters
    ----------
    origin_uri : str
        URL of the registry they are sent from
    dest_uri : str
        URL of the registry they are sent to
    origin_token : str
        token for the registry they are sent from
    dest_token : str
        token for the registry they are sent to
    data_products : typing.List[str]
        the data products, each as 'namespace:name@vversion'
    remote_label : str
        name of the remote registry, as the refusal names it
    pulling : bool, optional
        whether they are pulled from the remote, rather than pushed to it, by
        default False
    """
    _conflicts = []
    for _data_product in data_products:
        _there = _find_data_product(dest_uri, dest_token, _data_product)
        if not _there:
            continue
        _here = _find_data_product(origin_uri, origin_token, _data_product)
        if not _here:
            continue
        _contents_there = _contents(_there, dest_token)
        _contents_here = _contents(_here, origin_token)
        if _contents_there != _contents_here:
            _remote, _local = (
                (_contents_here, _contents_there)
                if pulling
                else (_contents_there, _contents_here)
            )
            _conflicts.append(
                f"'{_data_product}': on '{remote_label}' "
                f"{_describe_contents(_remote)}, and in the local registry "
                f"{_describe_contents(_local)}"
            )
    if not _conflicts:
        return
    _held = "\n".join(f"  - {_conflict}" for _conflict in _conflicts)
    if pulling:
        raise fdp_exc.RegistryError(
            "Nothing was pulled: the local registry holds another data product "
            f"under the name and version of each of these on '{remote_label}':\n"
            f"{_held}",
            hint="Pull into a local registry that holds none of them - a new one "
            "with a data store of its own, by 'fair registry install "
            "--directory <directory>', or this one emptied by 'fair registry "
            "reset'",
        )
    raise fdp_exc.RegistryError(
        f"Nothing was pushed: '{remote_label}' holds another data product under "
        f"the name and version of each of these:\n{_held}",
        hint="Register each under a version the remote does not have - write it "
        "again, or give its 'write:' or 'register:' entry a version - and push "
        "that instead",
    )


# The destination's object for an origin object, found by a data product of
# the object's: the namespace, name and version it has in both registries.
# None if the object has no data product, or none of them is there
def _get_dest_object_url_by_data_product(
    origin_object: typing.Dict,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
) -> typing.Optional[str]:
    for _data_product_url in origin_object["data_products"]:
        _data_product = fdp_req.url_get(_data_product_url, origin_token)
        _namespace = fdp_req.url_get(_data_product["namespace"], origin_token)
        _dest_namespace = fdp_req.get(
            dest_uri,
            "namespace",
            dest_token,
            params={"name": _namespace["name"]},
        )
        if not _dest_namespace:
            continue
        _dest_data_product = fdp_req.get(
            dest_uri,
            "data_product",
            dest_token,
            params={
                "name": _data_product["name"],
                "version": _data_product["version"],
                "namespace": fdp_req.get_obj_id_from_url(
                    _dest_namespace[0]["url"]
                ),
            },
        )
        if _dest_data_product:
            return _dest_data_product[0]["object"]
    return None


# Internal function to return the (remote) object associated with a code_run field containing and object url
def get_dest_object_url(
    origin_object_url: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    repository: bool = False,
) -> str:
    """Get the destination object url for a given origin object url
    Assumes the Object has already been synced and is on the remote registry

    Args:
        origin_object_url (str): Url of the origin object
        dest_uri (str): Destination registry URL
        dest_token (str): Token for the Destination Registry
        origin_token (str): Token for the Origin Registry
        repository (bool, optional): whether the object may be a code repository, found on the destination by its
            commit rather than by a file's hash, as a code run's 'code_repo' or a data product's object may be; by
            default False, for an object that is always a file - a code run's model config or submission script,
            or a quality-control document - which spares the origin a query for each

    Raises:
        fdp_exc.RegistryError: If the object has no storage location and no data product of it is on the destination
        fdp_exc.RegistryError: If the destination object does exist an RegistryError will be raised

    Returns:
        str: URL of the destination object
    """
    _origin_object = fdp_req.url_get(origin_object_url, origin_token)
    if repository and (
        _repository := _dest_repository(
            _origin_object, dest_uri, dest_token, origin_token
        )
    ):
        return _repository["url"]
    _origin_object_storage_location = _origin_object["storage_location"]
    # An object with no file has no hash to be found by
    if not _origin_object_storage_location:
        _dest_object_url = _get_dest_object_url_by_data_product(
            _origin_object, dest_uri, dest_token, origin_token
        )
        if not _dest_object_url:
            raise fdp_exc.RegistryError(
                f"Failed to find object '{origin_object_url}' on registry "
                f"'{dest_uri}': it has no file, and no data product of it "
                "is there"
            )
        return _dest_object_url
    _model_object_hash = fdp_req.url_get(_origin_object_storage_location, origin_token)[
        "hash"
    ]
    _dest_object_storage_location = fdp_req.get(
        dest_uri, "storage_location", dest_token, params={"hash": _model_object_hash}
    )
    if not _dest_object_storage_location:
        raise fdp_exc.RegistryError(
            f"No file with the hash {_model_object_hash}, the file of "
            f"'{origin_object_url}', is on '{dest_uri}'",
            hint="Push the data product it belongs to first, by 'fair add "
            "<namespace>:<name>@v<version>' and 'fair push'",
        )
    # The same file may be recorded at more than one location: a registered
    # file is also recorded at the place it was fetched from, where no object
    # is stored. The registry may list them in either order
    _dest_objects = []
    for _location in _dest_object_storage_location:
        _dest_objects += fdp_req.get(
            dest_uri,
            "object",
            dest_token,
            params={
                "storage_location": fdp_req.get_obj_id_from_url(
                    _location["url"]
                )
            },
        )
    if not _dest_objects:
        raise fdp_exc.RegistryError(
            f"No object on '{dest_uri}' holds the file with the hash "
            f"{_model_object_hash}, the file of '{origin_object_url}'",
            hint="Push the data product it belongs to first, by 'fair add "
            "<namespace>:<name>@v<version>' and 'fair push'",
        )
    # Several objects may hold the same file - one registered under two
    # names, two outputs with the same contents - and the one wanted is then
    # the one with a data product of the origin object's
    if len(_dest_objects) > 1:
        _dest_object_url = _get_dest_object_url_by_data_product(
            _origin_object, dest_uri, dest_token, origin_token
        )
        if _dest_object_url in [_object["url"] for _object in _dest_objects]:
            return _dest_object_url
    return _dest_objects[0]["url"]


def sync_code_run(
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    code_run_uuid: str,
    inputs=[],
    outputs=[],
) -> typing.Dict:
    """_summary_

    Args:
        origin_uri (str): Origin registry URL
        dest_uri (str): Destination registry URL
        dest_token (str): Destingation token
        origin_token (str): Origin token
        code_run_uuid (str): Code Run UUID
        inputs (list, optional): list of destination component urls to use as inputs Defaults to [].
        outputs (list, optional): list of destination component urls to use as outputs Defaults to [].

    Raises:
        fdp_exc.RegistryError: If the code run does not exist on the origin registry a RegistryError is raised

    Returns:
        typing.Dict: A dictionary containing the resulting destination code run
    """
    code_run = fdp_req.get(
        origin_uri, "code_run", origin_token, params={"uuid": code_run_uuid}
    )
    if not code_run:
        raise fdp_exc.RegistryError(
            f"Failed to access code_run {code_run_uuid} on local registry"
        )
    code_run = code_run[0]
    _remote_coderun = fdp_req.get(
        dest_uri, "code_run", dest_token, params={"uuid": code_run["uuid"]}
    )
    if _remote_coderun:
        _remote_coderun = _remote_coderun[0]
        # New lists: extending the arguments would extend their defaults
        inputs = inputs + _remote_coderun["inputs"]
        outputs = outputs + _remote_coderun["outputs"]
        logger.debug(
            f'patching code run: {code_run["uuid"]} with inputs: {inputs} and outputs {outputs}'
        )
        fdp_req._access(
            _remote_coderun["url"],
            "patch",
            dest_token,
            data={"inputs": list(set(inputs)), "outputs": list(set(outputs))},
        )
        _sync_attached_to_run(
            origin_uri, dest_uri, origin_token, dest_token, code_run, _remote_coderun
        )
    else:
        # Get and sync model config, the file before its records
        upload_object(
            origin_uri, dest_uri, dest_token, origin_token, code_run["model_config"]
        )
        sync_dependency_chain(
            object_url=code_run["model_config"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token,
            public=True,
        )
        _dest_code_run_model_config = get_dest_object_url(
            code_run["model_config"], dest_uri, dest_token, origin_token
        )
        # If theres a code_repo sync it. Its location is the repository, so it
        # is pushed as it is, never moved into the remote's data store, and the
        # destination's object for its commit is taken where there is one
        _dest_code_run_code_repo = None
        if code_run["code_repo"]:
            _placed = _repository_placed(
                code_run["code_repo"], dest_uri, dest_token, origin_token
            )
            sync_dependency_chain(
                object_url=code_run["code_repo"],
                dest_uri=dest_uri,
                origin_uri=origin_uri,
                dest_token=dest_token,
                origin_token=origin_token,
                public=False,
                placed=_placed,
            )
            # The destination's object found already, else the one just sent
            _dest_code_run_code_repo = _placed.get(
                code_run["code_repo"]
            ) or get_dest_object_url(
                code_run["code_repo"],
                dest_uri,
                dest_token,
                origin_token,
                repository=True,
            )
        # Sync Submision Script, the file before its records
        upload_object(
            origin_uri,
            dest_uri,
            dest_token,
            origin_token,
            code_run["submission_script"],
        )
        sync_dependency_chain(
            object_url=code_run["submission_script"],
            dest_uri=dest_uri,
            origin_uri=origin_uri,
            dest_token=dest_token,
            origin_token=origin_token,
            public=True,
        )
        _dest_code_run_submission_script = get_dest_object_url(
            code_run["submission_script"], dest_uri, dest_token, origin_token
        )

        # If the code run is not in the remote registry post the coderun
        dest_code_run = fdp_req.post(
            dest_uri,
            "code_run",
            dest_token,
            {
                "run_date": code_run["run_date"],
                "description": code_run["description"],
                "model_config": _dest_code_run_model_config,
                "submission_script": _dest_code_run_submission_script,
                "code_repo": _dest_code_run_code_repo,
                "inputs": inputs,
                "outputs": outputs,
                "uuid": code_run["uuid"],
            },
        )
        _sync_attached_to_run(
            origin_uri, dest_uri, origin_token, dest_token, code_run, dest_code_run
        )
        return dest_code_run


# The records attached to a run's configuration, script and code repository,
# such as an issue on the repository, sent to the run's copy on the destination
def _sync_attached_to_run(
    origin_uri: str,
    dest_uri: str,
    origin_token: str,
    dest_token: str,
    code_run: typing.Dict,
    dest_code_run: typing.Dict,
) -> None:
    for _field in ("model_config", "submission_script", "code_repo"):
        if code_run[_field] and dest_code_run.get(_field):
            sync_attached(
                origin_uri,
                dest_uri,
                origin_token,
                dest_token,
                code_run[_field],
                dest_code_run[_field],
            )


def is_component_public(component_url: str, token: str) -> bool:
    """Checks whether component is linked with a storage location and whether or not that is public

    Args:
        component_url (str): The full component url
        token (str): Token for the relevent registry

    Returns:
        bool: True if the component has an object and that object's storage location is public else false
    """
    component = fdp_req.url_get(component_url, token)
    if not component["object"]:
        return False
    object = fdp_req.url_get(component["object"], token)
    if not object["storage_location"]:
        return False
    storage_location = fdp_req.url_get(object["storage_location"], token)
    if not storage_location:
        return False
    return storage_location["public"]


def get_data_products_from_component(component_url: str, token: str) -> typing.List:
    """Returns data_products association with a component (url)

    Args:
        component_url (str): Full URL of the component
        token (str): Token for the relevent registry

    Returns:
        typing.List: A list of data_product URLs related to the component
    """
    component = fdp_req.url_get(component_url, token)
    object_ = fdp_req.url_get(component["object"], token)
    return object_["data_products"]


def format_data_product_list(data_product_urls: typing.List, token: str) -> typing.List:
    """Returns a string formated list for a given list of data_product URLs
    String is formated like so: {namespace_name}:{data_product_name}@v{data_product_version}

    Args:
        data_product_urls (typing.List): List of Full data_product URLS
        token (str): Token for the relevent registry

    Returns:
        typing.List: A list of formatted data_products as strings in the format: {namespace_name}:{data_product_name}@v{data_product_version}
    """
    rtn = []
    for data_product_url in data_product_urls:
        data_product = fdp_req.url_get(data_product_url, token)
        namespace = fdp_req.url_get(data_product["namespace"], token)
        rtn.append(
            f'{namespace["name"]}:{data_product["name"]}@v{data_product["version"]}'
        )
    return rtn


def format_data_product(data_product_url: str, token: str) -> str:
    """Retuns a formatted string for a given data_product Url
    String is formated like so: {namespace_name}:{data_product_name}@v{data_product_version}

    Args:
        data_product_url (str): data_product url
        token (str): token to the relevent regisrty

    Returns:
        str: formatted data_product as string in the format: {namespace_name}:{data_product_name}@v{data_product_version}
    """
    if not format_data_product_list([data_product_url], token):
        return ""
    return format_data_product_list([data_product_url], token)[0]


def get_dest_inputs(
    origin_inputs: typing.List,
    origin_uri: str,
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    remote_label: str,
    force: bool = False,
    context: PushContext = None,
) -> list:
    """Returns a list of input component urls on the destination registry
     from a given list of input component urls from the origin registry
     assumes the destination componets already exists in the remote registry

    Args:
        origin_inputs (typing.List): list of object_component urls from the origin registry
        origin_uri (str): Origin registry URL
        dest_uri (str): Destination registry URL
        dest_token (str): Destination Token
        origin_token (str): Origin Token
        remote_label (str): Remote label of the registry as define in the config yaml
        force (bool): whether to send again a data product the destination holds with the same contents, as after
            a push that failed part way, by default False. One it holds with other contents is refused whatever
            this says
        context (PushContext, optional): the context of the push this is part of, whose owner links the runs
            reached; by default one of its own, those runs linked before this returns

    Returns:
        list: A list of destination input component URLS
    """
    if context is None:
        with push_context(
            origin_uri, dest_uri, dest_token, origin_token, remote_label, force
        ) as _context:
            return get_dest_inputs(
                origin_inputs,
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                remote_label,
                force,
                context=_context,
            )
    dest_inputs = []
    for origin_input in origin_inputs:
        component_data_products = get_data_products_from_component(
            origin_input, origin_token
        )
        if component_data_products:
            sync_data_products(
                origin_uri,
                dest_uri,
                dest_token,
                origin_token,
                remote_label,
                format_data_product_list(component_data_products, origin_token),
                force=force,
                context=context,
            )
            dest_inputs.append(
                _dest_component(
                    context, origin_input, dest_uri, dest_token, origin_token
                )
            )
        else:
            sync_dependency_chain(
                object_url=origin_input,
                dest_uri=dest_uri,
                origin_uri=origin_uri,
                dest_token=dest_token,
                origin_token=origin_token,
                public=is_component_public(origin_input, origin_token),
            )
            dest_inputs.append(
                get_dest_component_url(origin_input, dest_uri, dest_token, origin_token)
            )
    return dest_inputs


def fetch_data_product(
    remote_token: str,
    local_data_store: str,
    data_product: typing.Dict,
    out_file: str = None,
    cache: str = None,
) -> typing.Optional[str]:
    """
    Retrieve a file using the given user configuration metadata

    Parameters
    ----------

    remote_token : str
        remote registry access token
    local_data_store : str
        local data store path
    data_product : typing.Dict
        the data product, as the remote registry holds it
    out_file : optional, str
        where to put the file, by default under the data product's namespace
        and name in the data store, named by its version
    cache : optional, str
        a copy of the file that the user holds, copied into place in place of
        the download, and left as it is

    Returns
    -------
    typing.Optional[str]
        path of the file, None for a data product that has no file
    """
    _object = fdp_req.url_get(data_product["object"], remote_token)

    _endpoint = data_product["object"].split("data_product")[0]

    if not _object.get("storage_location", None):
        logger.debug(
            "Skipping item '%s' for download "
            "as there is no physical storage location",
            data_product,
        )
        return None

    _storage_loc = fdp_req.url_get(_object["storage_location"], remote_token)

    _namespace = fdp_req.url_get(data_product["namespace"], remote_token)

    if _file_type_url := _object.get("file_type", None):
        _file_type = f'.{fdp_req.url_get(_file_type_url, remote_token)["extension"]}'
    else:
        _file_type = ""

    _local_dir = os.path.join(
        local_data_store, _namespace["name"], data_product["name"]
    )

    _out_file = out_file or os.path.join(
        _local_dir, f'{data_product["version"]}{_file_type}'
    )

    # A file that is there already is the data product's only if it is the
    # one the registry describes
    if os.path.exists(_out_file):
        if fdp_store.calculate_file_hash(_out_file) != _storage_loc["hash"]:
            raise fdp_exc.SynchronisationError(
                f"Data product '{_namespace['name']}:{data_product['name']}"
                f"@v{data_product['version']}' was not fetched: the data "
                f"store holds another file at '{_out_file}', where it is "
                "to go",
                error_code=1,
            )
        logger.debug("File '%s' already exists skipping download", _out_file)
        return _out_file

    if cache:
        _place_cached_file(
            cache,
            _out_file,
            _storage_loc["hash"],
            f"{_namespace['name']}:{data_product['name']}"
            f"@v{data_product['version']}",
        )
        return _out_file

    _path = _storage_loc["path"]
    _path = urllib.parse.quote(_path)
    _root = fdp_req.url_get(_storage_loc["storage_root"], remote_token)

    _reg_parse = urllib.parse.urlparse(_endpoint)
    _reg_url = f"{_reg_parse.scheme}://{_reg_parse.netloc}"

    _downloaded_file = download_from_registry(_reg_url, _root["root"], _path)

    # A file is kept only if it is the one the registry describes
    _hash = fdp_store.calculate_file_hash(_downloaded_file)
    if _hash != _storage_loc["hash"]:
        os.remove(_downloaded_file)
        raise fdp_exc.SynchronisationError(
            f"Data product '{_namespace['name']}:{data_product['name']}"
            f"@v{data_product['version']}' was not fetched: the file at "
            f"'{_root['root']}{_path}' has hash '{_hash}', and the registry "
            f"records '{_storage_loc['hash']}'",
            error_code=1,
        )

    os.makedirs(os.path.dirname(_out_file), exist_ok=True)

    shutil.move(_downloaded_file, _out_file)

    return _out_file


# Copy the user's own copy of a data product's file to its place in the data
# store, if it is the file the registry describes. It is copied beside that
# place and renamed into it, so that a copy cut short never stands there as
# the data product's file
def _place_cached_file(
    cache: str, out_file: str, registered_hash: str, data_product: str
) -> None:
    if not os.path.isfile(cache):
        raise fdp_exc.SynchronisationError(
            f"Data product '{data_product}' was not fetched: its 'cache', "
            f"'{cache}', is not a file",
            error_code=1,
        )
    _hash = fdp_store.calculate_file_hash(cache)
    if _hash != registered_hash:
        raise fdp_exc.SynchronisationError(
            f"Data product '{data_product}' was not fetched: its 'cache', "
            f"'{cache}', has hash '{_hash}', and the registry records "
            f"'{registered_hash}'",
            error_code=1,
        )
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    _handle, _copy = tempfile.mkstemp(dir=os.path.dirname(out_file))
    os.close(_handle)
    try:
        shutil.copyfile(cache, _copy)
        os.replace(_copy, out_file)
    except BaseException:
        os.remove(_copy)
        raise


# Fetch a pulled data product's file into the local data store and record it
# there, in the destination registry: the storage location of the file on
# this machine, keyed by the origin's storage location that it stands for,
# whose own record says where the file is on the remote. A registry records a
# file once under a root, so a file the data store holds already - registered
# there, or pulled under another name - is the one recorded, and it is
# fetched to that place only if it has gone: copied from 'cache', the
# user's own copy, where that is given. Nothing for a data product that has
# no file
def _place_pulled_file(
    dest_uri: str,
    dest_token: str,
    origin_token: str,
    local_data_store: str,
    data_product: typing.Dict,
    cache: str = None,
) -> typing.Dict[str, str]:
    _location_url = fdp_req.url_get(data_product["object"], origin_token)[
        "storage_location"
    ]
    if not _location_url:
        return {}
    _location = fdp_req.url_get(_location_url, origin_token)

    _root_url = fdp_store.get_write_storage(
        dest_uri, local_data_store, dest_token
    )
    _held = [
        location
        for location in fdp_req.get(
            dest_uri,
            "storage_location",
            dest_token,
            params={
                "hash": _location["hash"],
                "storage_root": fdp_req.get_obj_id_from_url(_root_url),
            },
        )
        if location["public"] == _location["public"]
    ]

    _file = fetch_data_product(
        origin_token,
        local_data_store,
        data_product,
        os.path.join(local_data_store, _held[0]["path"]) if _held else None,
        cache=cache,
    )
    if _held:
        return {_location_url: _held[0]["url"]}

    return {
        _location_url: fdp_req.post(
            dest_uri,
            "storage_location",
            dest_token,
            data={
                "path": os.path.relpath(_file, local_data_store),
                "storage_root": _root_url,
                "public": _location["public"],
                "hash": _location["hash"],
            },
        )["url"]
    }


def download_from_registry(registry_url: str, root: str, path: str) -> str:
    """
    Download a file from the registry given the storage root and path.

    If the root starts with '/' assume the file exists on the same location as
    the registry itself and try to download from that.

    Parameters
    ----------
    registry_url : str
        net location of the registry (not the endpoint of the API)
    root : str
        storage root
    path : str
        path of file on storage location

    Returns
    -------
    str
        path of downloaded temporary file

    Raises
    ------
    fdp_exc.UserConfigError
        if download failed
    """

    if root.startswith("/"):
        logger.warning(
            "Root of data storage location is '/' assuming data exists"
            " on registry server"
        )

        if not registry_url.endswith("/"):
            registry_url = registry_url[:-1]

        root = f"{registry_url}{root}"

    _download_url = f"{root}{path}"

    try:
        _temp_data_file = fdp_req.download_file(_download_url)
        logger.debug("Downloaded file from '%s' to temporary file", _download_url)
    except requests.HTTPError as r_in:
        raise fdp_exc.UserConfigError(
            f"Failed to fetch item '{_download_url}' with status code "
            f"{r_in.response.status_code}"
        ) from r_in

    return _temp_data_file


def upload_object(
    origin_uri: str, dest_uri: str, dest_token: str, origin_token: str, object_url: str
) -> bool:
    """
    Upload a file from the remote registry given the object url.

    Parameters
    ----------
    origin_url : str
        url of the local registry
    dest_utl : str
        url of the remote registry
    dest_token : str
        token of the remote registry
    origin_token : str
        token of the local registry
    object_url: str
        url of the the object to be uploded

    Returns
    -------
    bool
        whether there was a file to upload

    Raises
    ------
    fdp_exc.SynchronisationError
        if the file was not uploaded
    """
    _object = fdp_req.url_get(object_url, origin_token)
    if not _object["storage_location"]:
        logger.warning(
            f'File upload error: {object_url} ({_object["description"]}) has no storage_location'
        )
        return False
    _object_storage_location = fdp_req.url_get(
        _object["storage_location"], origin_token
    )
    _object_storage_location_root = fdp_req.url_get(
        _object_storage_location["storage_root"], origin_token
    )
    # A file on this machine is uploaded from where it is. Any other is
    # fetched first, to a temporary file which is removed afterwards
    _is_local = _is_local_root(_object_storage_location_root)
    if _is_local:
        _file_loc = (
            _object_storage_location_root["root"]
            + _object_storage_location["path"]
        ).replace("file://", "")
    else:
        _file_loc = download_from_registry(
            origin_uri,
            _object_storage_location_root["root"],
            _object_storage_location["path"],
        )
    try:
        fdp_store.upload_remote_file(_file_loc, dest_uri, dest_token)
    except Exception as e:
        logger.debug(f"{traceback.format_exc()}")
        raise fdp_exc.SynchronisationError(
            f"File '{_file_loc}' ({_object['description']}) was not uploaded "
            f"to registry '{dest_uri}': {e}",
            error_code=1,
        ) from e
    finally:
        if not _is_local:
            os.remove(_file_loc)
    logger.debug(f"File {_file_loc} Uploaded Successfully")
    return True
