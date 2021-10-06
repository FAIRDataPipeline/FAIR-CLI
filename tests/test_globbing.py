import pytest

import fair.registry.requests as fdp_req
import fair.parsing.globbing as fdp_glob
import fair.configuration as fdp_conf

_test_dict_list = [
    {
        'url': 'http://localhost:8000/api/author/10/',
        'uuid': '8cd7464f-3aaa-46cf-9ed9-a701cc9ba2dd',
        'last_updated': '2021-08-03T10:26:04.480934Z', 
        'name': 'Joe Bloggs',
        'identifier': None,
        'updated_by': 'http://localhost:8000/api/users/1/'
    },
    {
        'url': 'http://localhost:8000/api/author/9/',
        'uuid': 'dae30250-0b3d-4667-9222-5bcc4684821a',
        'last_updated': '2021-08-03T10:26:03.795851Z',
        'name': 'Joe Bloggs',
        'identifier': None,
        'updated_by': 'http://localhost:8000/api/users/1/'
    },
    {
        'url': 'http://localhost:8000/api/author/8/',
        'uuid': '6befd315-08e4-480a-ad9a-bf43f943840c',
        'last_updated': '2021-08-03T10:25:53.644123Z',
        'name': 'John Smith',
        'identifier': None,
        'updated_by': 'http://localhost:8000/api/users/1/'
    }
]

@pytest.mark.globbing
def test_globbing_author(mocker):
    def _dummy_get(*args, **kwargs):
        return _test_dict_list

    def _do_nowt(*args, **kwargs):
        return ''

    mocker.patch.object(fdp_req, 'get', _dummy_get)
    mocker.patch.object(fdp_conf, 'get_local_uri', _do_nowt)

    _out_list = fdp_glob.glob_read_write('.', [{'author': '*'}], local_glob=True)

    assert len(_out_list) == 2
