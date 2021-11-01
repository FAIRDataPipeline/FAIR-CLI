import typing
import tempfile
import os
import platform
import git

import fair.identifiers as fdp_id


def create_configurations(
    registry_dir: str,
    local_git_dir: str = os.getcwd(),
    testing_dir: str = tempfile.mkdtemp(),
    tokenless: bool = False) -> typing.Dict:
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
    _loc_data_store = os.path.join(testing_dir, 'data_store') + os.path.sep
    _proj_dir = os.path.join(testing_dir, 'project')

    if tokenless:
        _token = 't35tt0k3n'
        _token_file = os.path.join(registry_dir, 'token.txt')
        with open(_token_file, 'w') as out_f:
            out_f.write(_token)

    if local_git_dir:
        _repo = git.Repo(local_git_dir)
    else:
        _repo = git.Repo.init(_proj_dir)
        _repo.create_remote('origin', url='git@notagit.com/nope')
        local_git_dir = _proj_dir

    os.makedirs(_loc_data_store)
    _local_uri = 'http://localhost:8000/api/'
    _origin_uri = 'http://localhost:8001/api/'
    if platform.system() == "Windows":
        _local_uri = 'http://127.0.0.1:8000/api/'
        _origin_uri = 'http://127.0.0.1:8001/api/'
    return {
        'namespaces': {'input': 'testing', 'output': 'testing'},
        'registries': {
            'local': {
                'data_store': _loc_data_store,
                'directory': registry_dir,
                'uri': _local_uri
            },
            'origin': {
                'data_store': None,
                'token': os.path.join(registry_dir, 'token.txt'),
                'uri': _origin_uri
            }
        },
        'user': {
            'email': 'test@noreply',
            'family_name': 'Test',
            'given_names': 'Interface',
            'orcid': '000-0000-0000-0000',
            'uri': f'{fdp_id.ID_URIS["orcid"]}000-0000-0000-0000',
            'uuid': '2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1'
        },
        'git': {
            'local_repo': local_git_dir,
            'remote': 'origin'
        },
    }
    
