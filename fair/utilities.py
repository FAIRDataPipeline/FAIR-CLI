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

"""

from typing import Dict


def flatten_dict(
    in_dict: Dict,
    separator: str = ".",
    _out_dict: Dict = None,
    _parent_key: str = None,
) -> Dict:
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
        new_label = (
            f"{_parent_key}{separator}{label}" if _parent_key else label
        )
        if isinstance(value, dict):
            flatten_dict(
                in_dict=value, _out_dict=_out_dict, _parent_key=new_label
            )
            continue

        _out_dict[new_label] = value

    return _out_dict


def expand_dict(
    in_dict: Dict, separator: str = ".", _out_dict: Dict = None
) -> Dict:
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
