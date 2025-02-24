#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

FDP-CLI Utilities
=================

Collection of methods used which are not specific to the FDP-CLI and so are
defined as 'utilities'.


Contains
========

Functions
---------

    flatten_dict - convert a nested dictionary into a single level version.
    expand_dict  - expands a single level dictionary to a nested version.
    remove_dictlist_dupes - removes duplicates from list of depth 1 dictionaries

Class
-----
    JSONDateTimeEncoder - for allowing datetime strings to be json parsed

"""

__date__ = "2021-08-04"

import datetime
import json
import logging
import typing
import urllib.parse

import validators

logger = logging.getLogger("FAIRDataPipeline.Utilities")


def flatten_dict(
    in_dict: typing.Dict,
    separator: str = ".",
    _out_dict: typing.Dict = None,
    _parent_key: str = None,
) -> typing.Dict:
    """Flatten a nested dictionary into a single level variant

    Constructs a new dictionary from a nested dictionary of just one level
    using a separator for the keys.

    Parameters
    ----------
    in_dict : Dict
        input dictionary
    separator : str, optional
        separator to use for keys, by default "."

    Returns
    -------
    Dict
        a flattened version of the input dictionary
    """
    if _out_dict is None:
        _out_dict = {}

    for label, value in in_dict.items():
        new_label = f"{_parent_key}{separator}{label}" if _parent_key else label
        if isinstance(value, dict):
            flatten_dict(in_dict=value, _out_dict=_out_dict, _parent_key=new_label)
            continue

        _out_dict[new_label] = value

    return _out_dict


def expand_dict(
    in_dict: typing.Dict, separator: str = ".", _out_dict: typing.Dict = None
) -> typing.Dict:
    """Expand a flattened dictionary into a nested dictionary

    Expands a dictionary with a separator in the keys to be a nested
    dictionary.

    Parameters
    ----------
    in_dict : Dict
        input single level dictionary
    separator : str, optional
        key separator, by default "."

    Returns
    -------
    Dict
        nested dictionary representation of the input
    """
    if _out_dict is None:
        _out_dict = {}

    for label, value in in_dict.items():
        if separator not in label:
            _out_dict.update({label: value})
            continue
        key, _components = label.split(separator, 1)
        if key not in _out_dict:
            _out_dict[key] = {}
        expand_dict({_components: value}, separator, _out_dict[key])

    return _out_dict


def remove_dictlist_dupes(
    dicts: typing.List[typing.Dict],
) -> typing.List[typing.Dict]:
    """Remove duplicate dictionaries from a list of dictionaries

    Note: this will only work with single layer dictionaries!

    Parameters
    ----------
    dicts : List[Dict]
        a list of dictionaries

    Returns
    -------
    List[Dict]
        new list without duplicates
    """
    # Convert single layer dictionary to a list of key-value tuples
    _tupleify = [list(d.items()) for d in dicts]

    # Only append unique tuple lists
    _set_tupleify = []
    for t in _tupleify:
        if t not in _set_tupleify:
            _set_tupleify.append(t)

    # Convert the tuple list back to a list of dictionaries
    return [{i[0]: i[1] for i in kv} for kv in _set_tupleify]


def get_nested_key(
    search_dict: typing.Dict, key_addr: str, separator: str = "."
) -> typing.Any:
    """Retrieve a value from a multi-level dictionary

    Parameters
    ----------
    search_dict : typing.Dict
        dictionary to retrieve from
    key_addr : str
        key address of value to recover

    Returns
    -------
    typing.Any
        value at the specified address
    """
    _flattened_dict = flatten_dict(search_dict, separator=separator)
    return _flattened_dict[key_addr]


class JSONDateTimeEncoder(json.JSONEncoder):
    def default(self, date_time_candidate):
        if isinstance(date_time_candidate, datetime.datetime):
            return str(date_time_candidate)
        else:
            return super().default(date_time_candidate)


def check_trailing_slash(string: str):
    if string[-1] != "/":
        string += "/"
    return string


def remove_trailing_slash(string: str):
    if string[-1] == "/":
        string = string.rstrip(string[-1])
    return string


def is_api_url(uri: str, string: str) -> bool:
    """Checks if given string is a valid API URL

    Parameters
    ----------
    uri : str
        the URI of the API to check against
    string : str
        URL candidate to check

    Returns
    -------
    bool
        if a valid URL for the given API endpoint
    """
    if not validators.url(string):
        return False
    _url = urllib.parse.urlparse(string)
    _uri = urllib.parse.urlparse(uri)

    logger.debug(
        "Checking if '%s' is a valid API URL against net location '%s'",
        string,
        _uri.netloc,
    )

    return _url.netloc == _uri.netloc
