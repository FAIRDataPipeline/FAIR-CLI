import os
import pathlib
import tempfile
import typing

import git

import fair.common as fdp_com
import fair.identifiers as fdp_id


def create_configurations(
    registry_dir: str,
    local_git_dir: typing.Optional[str] = None,
    remote_reg_dir: typing.Optional[str] = None,
    testing_dir: str = tempfile.mkdtemp(),
    tokenless: bool = False,
) -> typing.Dict:
    """
    Setup CLI for testing

    Running without a token limits the functionality, this should only be used
    when testing without need to access a local registry.

    Parameters
    ----------
    registry_dir : str
        directory of the local registry
    testing_dir : str
        working directory for the test
    tokenless : optional, bool
        create a fake token, default False

    Returns
    -------
    typing.Dict
        A CLI configuration dictionary that can be loaded for a the CLI session
    """
    if not registry_dir:
        if "FAIR_REGISTRY_DIR" in os.environ:
            registry_dir = os.environ["FAIR_REGISTRY_DIR"]
        else:
            registry_dir = fdp_com.DEFAULT_REGISTRY_LOCATION
    if not remote_reg_dir:
        remote_reg_dir = os.path.join(
            os.path.dirname(fdp_com.DEFAULT_REGISTRY_LOCATION), "registry-rem"
        )

    _loc_data_store = os.path.join(testing_dir, "data_store") + os.path.sep
    _proj_dir = os.path.join(testing_dir, "project")

    if tokenless:
        _token = "t35tt0k3n"
        _token_file = os.path.join(registry_dir, "token.txt")
        with open(_token_file, "w") as out_f:
            out_f.write(_token)

    if not local_git_dir:
        local_git_dir = _no_git_setup(_proj_dir)
    os.makedirs(_loc_data_store, exist_ok=True)
    _local_uri = "http://127.0.0.1:8000/api/"
    _origin_uri = "http://127.0.0.1:8001/api/"
    return {
        "namespaces": {"input": "testing", "output": "testing"},
        "registries": {
            "local": {
                "data_store": _loc_data_store,
                "directory": registry_dir,
                "token": os.path.join(registry_dir, "token"),
                "uri": _local_uri,
            },
            "origin": {
                "data_store": os.path.join(remote_reg_dir, "data"),
                "token": os.path.join(remote_reg_dir, "token"),
                "uri": _origin_uri,
            },
        },
        "user": {
            "email": "test@noreply.com",
            "family_name": "Test",
            "given_names": "Interface",
            "orcid": "000-0000-0000-0000",
            "uri": f'{fdp_id.ID_URIS["orcid"]}000-0000-0000-0000',
            "uuid": "2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1",
        },
        "git": {
            "local_repo": local_git_dir,
            "remote": "origin",
            "remote_repo": "git@notagit.com/user/project.git",
        },
    }


def _no_git_setup(_proj_dir):
    _repo = git.Repo.init(_proj_dir)
    _repo.create_remote("origin", url="git@notagit.com/nope")
    result = _proj_dir
    _demo_file = os.path.join(_proj_dir, "first_file")
    pathlib.Path(_demo_file).touch()
    _repo.index.add(_demo_file)
    _repo.index.commit("First commit of test repository")

    return result
