import yaml
import os
import hashlib
import urllib.parse

import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.identifiers as fdp_id


def get_write_storage(uri: str, work_cfg_yml: str) -> str:
    """Construct storage root if it does not exist

    Parameters
    ----------
    uri : str
        end point of the RestAPI
    work_cfg_yml : str
        path of the working config file

    Returns
    -------
    str
        URI of the created/retrieved storage root

    Raises
    ------
    fdp_exc.UserConfigError
        If 'write_data_store' not present in the working config
    """

    _work_cfg = yaml.safe_load(open(work_cfg_yml))
    _work_cfg_meta = _work_cfg["run_metadata"]

    if "write_data_store" not in _work_cfg_meta:
        raise fdp_exc.UserConfigError(
            "Cannot create a storage location on the registry for writing,"
            " no local file location specified."
        )

    # Convert local file path to a valid data store path
    _write_store_root = f"file://{_work_cfg_meta['write_data_store']}/"

    # Check if the data store already exists by querying for it
    _search_root = fdp_req.get(
        uri, ("storage_root",), params={"root": _write_store_root}
    )

    # If the data store already exists just return the URI else create it
    # and then do the same
    if not _search_root:
        _post_data = {"root": _write_store_root, "local": True}
        _storage_root = fdp_req.post(uri, ("storage_root",), data=_post_data)
        return _storage_root["url"]
    else:
        return _search_root[0]["url"]


def store_user(run_dir: str, uri: str) -> str:
    """Creates an Author entry for the user if one does not exist

    Parameters
    ----------
    uri : str
        registry RestAPI endpoint

    Returns
    -------
    str
        URI for created author
    """
    _user = fdp_conf.get_current_user_name(run_dir)
    _data = {}
    if len(_user) > 1:
        _data["family_name"] = _user[0]
        _data["given_name"] = _user[1]
    else:
        _data["given_name"] = _user[0]

    try:
        _orcid = fdp_conf.get_current_user_orcid(run_dir)
        _orcid = urllib.parse.urljoin(fdp_id.ORCID_URL, _orcid)
        _data["identifier"] = _orcid
        return fdp_req.post_else_get(
            uri, ("author",), data=_data, params={"identifier": _orcid}
        )
    except fdp_exc.CLIConfigurationError:
        _uuid = fdp_conf.get_current_user_uuid(run_dir)
        _data["uuid"] = _uuid
        return fdp_req.post_else_get(
            uri, ("author",), data=_data, params={"uuid": _uuid}
        )


def create_file_type(uri: str, name: str, extension: str) -> str:
    """Creates a new file type on the registry

    Parameters
    ----------
    uri : str
        registry RestAPI end point
    ftype : str
        file extension

    Returns
    -------
    str
        URI for created file type
    """
    return fdp_req.post_else_get(
        uri, ("file_type",), data={"name": name, "extension": extension}
    )


def store_working_config(run_dir: str, uri: str, work_cfg_yml: str) -> str:
    """Construct a storage location and object for the working config

    Parameters
    ----------
    uri : str
        RestAPI end point
    work_cfg_yml : str
        location of working config yaml

    Returns
    -------
    str
        new URI for the created object

    Raises
    ------
    fair.exceptions.RegistryAPICallError
        if bad status code returned from the registry
    """
    _root_store = get_write_storage(uri, work_cfg_yml)

    _work_cfg = yaml.safe_load(open(work_cfg_yml))
    _work_cfg_data_store = _work_cfg["run_metadata"]["write_data_store"]
    _rel_path = os.path.relpath(work_cfg_yml, _work_cfg_data_store)

    _hash = hashlib.sha1(open(work_cfg_yml).read().encode("utf-8")).hexdigest()

    _storage_loc_data = {
        "path": _rel_path,
        "storage_root": _root_store,
        "public": False,
        "hash": _hash,
    }

    _post_store_loc = fdp_req.post_else_get(
        uri,
        ("storage_location",),
        data=_storage_loc_data,
        params={"hash": _hash},
    )

    _user = store_user(run_dir, uri)

    _yaml_type = create_file_type(
        uri, "YAML human readable data storage file", "yaml"
    )

    _time_stamp_dir = os.path.basename(os.path.dirname(work_cfg_yml))
    _desc = f"Working configuration file for timestamp {_time_stamp_dir}"
    _object_data = {
        "description": _desc,
        "storage_location": _post_store_loc,
        "file_type": _yaml_type,
        "authors": [_user],
    }

    return fdp_req.post_else_get(
        uri, ("object",), data=_object_data, params={"description": _desc}
    )
