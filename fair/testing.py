import typing
import tempfile
import os
import platform
import git

import fair.common as fdp_com


def create_configurations(registry_dir: str) -> typing.Dict:
    _test_directory = tempfile.mkdtemp()
    _loc_data_store = os.path.join(_test_directory, 'data_store') + os.path.sep

    _git = fdp_com.find_git_root(os.getcwd())

    if not _git:
        git.Repo.init(os.getcwd())
        _git = os.getcwd()

    os.makedirs(_loc_data_store)
    _config_dict = {
        'namespaces': {'input': 'testing', 'output': 'testing'},
        'registries': {
            'local': {
                'data_store': _loc_data_store,
                'directory': registry_dir,
                'uri': 'http://localhost:8000/api/'
            },
            'origin': {
                'data_store': None,
                'token': None,
                'uri': 'http://localhost:8001/api/'
            }
        },
        'user': {
            'email': 'test@noreply',
            'family_name': 'Test',
            'given_names': 'Interface',
            'orcid': None,
            'uuid': '2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1'
        },
        'git': {
            'local_repo': _git,
            'remote': 'origin'
        },
    }
    if platform.system() == "Windows":
        _config_dict['registries']['local']['uri'] = 'http://127.0.0.1:8000/api/'
        _config_dict['registries']['origin']['uri'] = 'http://127.0.0.1:8001/api/'
    return _config_dict
