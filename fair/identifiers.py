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
    check_ror - verify a ROR and return name information
    check_grid - verify a GRID ID and return name information
"""

__date__ = "2021-07-01"

import time
import typing
import urllib.parse

import requests
import requests.exceptions

ID_URIS = {
    "orcid": "https://orcid.org/",
    "ror": "https://ror.org/",
    "grid": "https://www.grid.ac/institutes/",
}

QUERY_URLS = {
    "orcid": "https://pub.orcid.org/v2.0/",
    "ror": "https://api.ror.org/organizations?query=",
    "grid": "https://www.grid.ac/institutes/",
}


def check_orcid(orcid: str) -> typing.Dict:
    """Checks if valid ORCID using ORCID public api

    Parameters
    ----------
    orcid : str
        ORCID to be checked

    Returns
    -------
    typing.Dict
        metadata from the given ID
    """

    _header = {"Accept": "application/json"}
    _url = urllib.parse.urljoin(QUERY_URLS["orcid"], orcid)
    _response = requests.get(_url, headers=_header)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code != 200:
        return _result_dict

    _names = _response.json()["person"]["name"]
    _given = _names["given-names"]["value"]
    _family = _names["family-name"]["value"]
    _name = f"{_given} {_family}"

    _result_dict["name"] = _name
    _result_dict["family_name"] = _family
    _result_dict["given_names"] = _given
    _result_dict["orcid"] = orcid
    _result_dict["uri"] = f'{ID_URIS["orcid"]}{orcid}'

    return _result_dict


def check_ror(ror: str) -> typing.Dict:
    """Checks if valid ROR using ROR public api

    Parameters
    ----------
    ror : str
        ROR to be checked

    Returns
    -------
    typing.Dict
        metadata from the given ID
    """

    _url = f"{QUERY_URLS['ror']}{ror}"
    _response = requests.get(_url)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code != 200:
        return _result_dict

    if _response.json()["number_of_results"] == 0:
        return _result_dict

    _name = _response.json()["items"][0]["name"]
    _result_dict["name"] = _name
    _result_dict["family_name"] = _name
    _result_dict["given_names"] = None
    _result_dict["ror"] = ror
    _result_dict["uri"] = f'{ID_URIS["ror"]}{ror}'

    return _result_dict


def check_grid(grid_id: str) -> typing.Dict:
    """Checks if valid GRID ID using GRID public api
    Parameters
    ----------
    grid_id : str
        GRID ID to be checked
    Returns
    -------
    typing.Dict
        metadata from the given ID
    """
    _header = {"Accept": "application/json"}
    _response = requests.get(f'{QUERY_URLS["grid"]}{grid_id}', headers=_header)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code != 200:
        return _result_dict

    _name = _response.json()["institute"]["name"]

    _result_dict["name"] = _name
    _result_dict["family_name"] = _name
    _result_dict["given_names"] = None
    _result_dict["grid"] = grid_id
    _result_dict["uri"] = f'{ID_URIS["grid"]}{grid_id}'

    return _result_dict


def check_id_permitted(identifier: str, retries: int = 5) -> bool:
    """Check a user provided identifier is permitted

    This ID is expected to be a valid URL

    Parameters
    ----------
    identifier : str
        identifier URL candidate
    retries: int
        number of attempts

    Returns
    -------
    bool
        if valid identifier
    """
    _n_attempts = 0

    while _n_attempts < retries:
        try:
            requests.get(identifier).raise_for_status()
            return True
        except (
            requests.exceptions.MissingSchema,
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ):
            _n_attempts += 1
            time.sleep(1)
            continue

    return False
