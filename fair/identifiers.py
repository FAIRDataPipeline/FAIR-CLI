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
import logging
from urllib3.exceptions import InsecureRequestWarning
from fake_useragent import UserAgent

logger = logging.getLogger("FAIRDataPipeline.Identifiers")

ID_URIS = {
    "orcid": "https://orcid.org/",
    "ror": "https://ror.org/",
    "github" : "https://github.com/"
}

QUERY_URLS = {
    "orcid": "https://pub.orcid.org/v3.0/",
    "ror": "https://api.ror.org/organizations?query=",
    "github": "https://api.github.com/users/"
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
    orcid = orcid.replace(ID_URIS["orcid"], "")
    _header = {"Accept": "application/json"}
    _url = urllib.parse.urljoin(QUERY_URLS["orcid"], orcid)
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    _response = requests.get(_url, headers=_header, verify = False, allow_redirects = True)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code != 200:
        logger.debug(f"{_url} Responded with {_response.status_code}")
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

def check_github(github: str) -> typing.Dict:
    """Checks if valid ORCID using ORCID public api

    Parameters
    ----------
    github : str
        github username to be checked

    Returns
    -------
    typing.Dict
        metadata from the given ID
    """
    _header = {"Accept": "application/json"}
    _url = urllib.parse.urljoin(QUERY_URLS["github"], github)
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
    _response = requests.get(_url, headers=_header, verify = False, allow_redirects = True)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code == 403:
        time.sleep(3)
        _header = {"Accept": "application/json", 'User-Agent':str(UserAgent().chrome)}
        _response = requests.get(_url, headers=_header, verify = False, allow_redirects = True)

    if _response.status_code != 200:
        logger.debug(f"{_url} Responded with {_response.status_code}")
        return _result_dict

    _login = _response.json()["login"]
    _name = _response.json()["name"]
    if _name:
        _result_dict["family_name"] = _name.split()[-1]
        _result_dict["given_names"] = " ".join(_name.split()[:-1])
        
    _result_dict["name"] = _name    
    _result_dict["github"] = _login
    _result_dict["uri"] = f'{ID_URIS["github"]}{_login}'

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
    _result_dict = _check_generic_ror(ror)
    if _result_dict:
        _result_dict["ror"] = ror

    return _result_dict


def check_grid(grid_id: str) -> typing.Dict:
    """Checks if valid GRID ID using ROR (https://ror.org/) public api
    Parameters
    ----------
    grid_id : str
        GRID ID to be checked
    Returns
    -------
    typing.Dict
        metadata from the given ID
    """
    _result_dict = _check_generic_ror(f'"{grid_id}"')
    if _result_dict:
        _result_dict["grid"] = grid_id

    return _result_dict

def _check_generic_ror(id: str) -> typing.Dict:
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
    _url = f"{QUERY_URLS['ror']}{id}"
    _response = requests.get(_url)

    _result_dict: typing.Dict[str, typing.Any] = {}

    if _response.status_code != 200:
        logger.debug(f"{_url} Responded with {_response.status_code}")
        return _result_dict

    if _response.json()["number_of_results"] == 0:
        return _result_dict

    _id = _response.json()["items"][0]["id"]
    _name = _response.json()["items"][0]["name"]
    _result_dict["name"] = _name
    _result_dict["family_name"] = _name
    _result_dict["given_names"] = None
    _result_dict["uri"] = _id

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
    fake_agent = False

    while _n_attempts < retries:
        try:
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
            headers = {}
            if fake_agent:
                headers = {'User-Agent':str(UserAgent().chrome)} 
            requests.get(identifier, verify = False, allow_redirects = True, headers = headers).raise_for_status()
            return True
        except (
            requests.exceptions.MissingSchema,
            requests.exceptions.HTTPError,
            requests.exceptions.ConnectionError,
        ) as e:
            _n_attempts += 1
            time.sleep(3)
            fake_agent = True
            logger.warning(f"Error identifier: '{identifier}' caused '{e}'")
            continue

    return False

def strip_identifier(identifier):
    _url_parse = urllib.parse.urlparse(identifier.strip())
    if _url_parse:
        _url_split = _url_parse[2].rpartition('/')
        if _url_split:
            return _url_split[2]
    return ""