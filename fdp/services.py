import requests


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


def download_data(config):
    """
    Download any data required by read: from the remote data store.
    """
    pass
