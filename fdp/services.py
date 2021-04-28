import os

import requests

from data_pipeline_api.registry.download import download_from_config_file


def registry_running():
    try:
        r = requests.get("http://localhost:8000/api?")
    except requests.exceptions.ConnectionError:
        return False
    else:
        if r.status_code == 200:
            return True
        else:
            return False


def token():
    with open("token.txt", "r") as file:
        api_token = file.read()
        return api_token


def download_data(config):
    """
    Download any data required by read: from the remote data store.
    """
    download_from_config_file(config, token())
    pass
