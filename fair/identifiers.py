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
from typing import Dict

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
    _names = _response.json()['person']['name']
    _given = _names['given-names']['value']
    _family = _names['family-name']['value']

    return {"orcid": orcid, "given_name": _given, "family_name": _family}
