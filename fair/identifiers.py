#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Identifiers
===========

Methods relating to retrieval and parsing of unique identifiers

Contents
========

Functions
---------
    check_orcid - verify an ORCID and return name information
"""

__date__ = "2021-07-01"

import urllib.parse
import requests
from typing import Dict, Any


ORCID_URL = "https://pub.orcid.org/v2.0/"


def check_orcid(orcid: str) -> Dict:
    """Checks if valid ORCID using ORCID public api

    Parameters
    ----------
    orcid : str
        ORCID to be checked

    Returns
    -------
    bool
        whether ID is valid
    """

    _header = {'Accept': 'application/json'}
    _url = urllib.parse.urljoin(ORCID_URL, orcid)
    _response = requests.get(_url, headers = _header)

    _result_dict: Dict[str, Any] = {}

    if _response.status_code != 200:
        return _result_dict
    
    _names = _response.json()['person']['name']
    _given = _names['given-names']['value']
    _family = _names['family-name']['value']

    _result_dict['family_name'] = _family
    _result_dict['given_names'] = _given
    _result_dict['orcid'] = orcid

    return _result_dict


def check_id_permitted(identifier: str) -> bool:
    """Check a user provided identifier is permitted

    This ID is expected to be a valid URL

    Parameters
    ----------
    identifier : str
        identifier URL candidate

    Returns
    -------
    bool
        if valid identifier
    """
    try:
        requests.get(identifier).raise_for_status()
        return True
    except requests.HTTPError:
        return False
