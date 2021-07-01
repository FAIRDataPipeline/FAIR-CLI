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

import bs4

ORCID_URL = "https://orcid.org/"


def check_orcid(orcid: str) -> Dict:
    """Checks if valid ORCID

    Parameters
    ----------
    orcid : str
        ORCID to be checked

    Returns
    -------
    bool
        whether ID is valid
    """
    # FIXME: Rather hackish method of getting name from ORCID site
    # Relies on the ORCID appearing in the resultant webpage to confirm it
    # exists (usually returns sign in for invalid results)
    _url = urllib.parse.urljoin(ORCID_URL, orcid)
    _response = requests.get(_url)
    _soup = bs4.BeautifulSoup(_response.text, "html.parser")

    try:
        _data = _soup.head.find_all(property="og:title")[0]["content"]
    except IndexError:
        return {}

    _name = _data.split("(")[0].strip()
    _given, _family = _name.split(" ", 1)
    return {"orcid": orcid, "given_name": _given, "family_name": _family}
