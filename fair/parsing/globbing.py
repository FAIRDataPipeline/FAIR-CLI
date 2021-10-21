#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Glob User Config
=================

Substitute globular expressions in `config.yaml` for entries
within the local registry


Contents
========

Functions
-------

    glob_read_write - glob expressions in 'read' and 'write' blocks

"""

__date__ = "2021-08-16"

import typing
import copy

import fair.registry.requests as fdp_req
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.registry.versioning as fdp_ver
import fair.utilities as fdp_util


def expand_wildcards(cfg: typing.Dict, blocktype: str) -> None:
    """Expand the wildcards in the config yaml from the local registry

    Parameters
    ----------
    cfg : typing.Dict
        user config yaml

    blocktype : str
        key name of block to fill in entries for ('read' or 'write')

    """
    glob_read_write(
        cfg, blocktype, None, True, blocktype=='read'
    )

def glob_read_write(
    cfg: typing.Dict,
    blocktype: str,
    search_key: str = None,
    local_glob: bool = False,
    remove_wildcard: bool = False) -> typing.List:
    """Substitute glob expressions in the 'read' or 'write' part of a user config

    Parameters
    ----------
    cfg : typing.Dict
        config yaml
    blocktype : str
        block type to process
    search_key : str, optional
        key to search under, default is taken from SEARCH_KEYS
    local_glob : bool, optional
        whether to search the local or remote registry,
        default is False.
    remove_wildcard: bool, optional
        whether to delete wildcard from yaml file, default is False
    """
    
    _block_cfg = cfg[blocktype]
    _parsed: typing.List[typing.Dict] = []

    # Check whether to glob the local or remote registry
    # retrieve the URI from the repository CLI config
    if local_glob:
        _uri = fdp_conf.registry_url("local", cfg)
    else:
        _uri = fdp_conf.registry_url("global", cfg)

    # Iterate through all entries in the section looking for any
    # key-value pairs that contain glob statements.
    for entry in _block_cfg:
        # We still want to keep the wildcard version in case the
        # user wants to write to this namespace.
        # Wipe version info for this object to start from beginning
        _orig_entry = copy.deepcopy(entry)
        _orig_entry['use']['version'] = str(fdp_ver.get_correct_version(cfg))

        _glob_vals = [(k, v) for k, v in entry.items() if isinstance(v, str) and '*' in v]
        if len(_glob_vals) > 1:
            # For now only allow one value within the dictionary to have them
            raise fdp_exc.NotImplementedError(
                "Only one key-value pair in a 'read' list entry may contain a"
                " globbable value"
            )
        elif len(_glob_vals) == 0:
            # If no globbables keep existing statement
            _parsed.append(entry)
            continue
        elif not remove_wildcard:
            # If we're going ahead, add in the wildcard too if wanted
            _parsed.append(_orig_entry)

        _key_glob, _globbable = _glob_vals[0]

        if not search_key:
            search_key = fdp_req.SEARCH_KEYS[_key_glob]

        _search_dict = {search_key: _globbable}

        # Update search from 'use' block
        _search_dict.update(entry['use'])

        try:
            fdp_ver.parse_incrementer(_search_dict["version"])
            # Is an incrementer, so get rid of it
            _search_dict.pop('version', None)
        except fdp_exc.UserConfigError: # Should be an exact version, so keep
            None

        # Send a request to the relevant registry using the search string
        # and the selected search key        
        _results = fdp_req.get(
            _uri,
            _key_glob,
            params = _search_dict
        )

        # Iterate through all results, make a copy of the entry and swap
        # the globbable statement for the result statement appending this
        # to the output list
        for result in _results:
            _entry_dict = copy.deepcopy(entry)
            if _key_glob in _entry_dict['use']:
                _entry_dict['use'][_key_glob] = result[search_key]
            if _key_glob in _entry_dict:
                _entry_dict[_key_glob] = result[search_key]
            _parsed.append(_entry_dict)

    # Before returning the list of dictionaries remove any duplicates
    cfg[blocktype] = fdp_util.remove_dictlist_dupes(_parsed)
