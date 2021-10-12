import typing
import tempfile
import os
import git


def create_configurations(registry_dir: str, testing_dir: str = tempfile.mkdtemp()) -> typing.Dict:
    _loc_data_store = os.path.join(testing_dir, 'data_store') + os.path.sep
    _proj_dir = os.path.join(testing_dir, 'project')
    _token = 't35tt0k3n'
    _token_file = os.path.join(registry_dir, 'token.txt')
    with open(_token_file, 'w') as out_f:
        out_f.write(_token)

    _repo = git.Repo.init(_proj_dir)
    _repo.create_remote('origin', url='git@notagit.com/nope')

    os.makedirs(_loc_data_store)
    return {
        'namespaces': {'input': 'testing', 'output': 'testing'},
        'registries': {
            'local': {
                'data_store': _loc_data_store,
                'directory': registry_dir,
                'uri': 'http://localhost:8000/api/'
            },
            'origin': {
                'data_store': None,
                'token': os.path.join(registry_dir, 'token.txt'),
                'uri': 'http://localhost:8001/api/'
            },
            'alternate': {
                'data_store': None,
                'uri': 'http://localhost:8007/api/'
            }
        },
        'user': {
            'email': 'test@noreply',
            'family_name': 'Test',
            'given_names': 'Interface',
            'orcid': '000-0000-0000-0000',
            'uuid': '2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1'
        },
        'git': {
            'local_repo': _proj_dir,
            'remote': 'origin'
        },
    }
