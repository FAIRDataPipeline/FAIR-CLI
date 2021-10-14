import py
import pytest
import tempfile
import os
import pytest_mock

import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc

from . import conftest as conf


LOCAL_URL = 'http://localhost:8000/api'


@pytest.mark.requests
def test_split_url():
    _test_url = 'https://not_a_site.com/api/object?something=other'
    assert fdp_req.split_api_url(_test_url) == ('https://not_a_site.com/api', 'object?something=other')
    assert fdp_req.split_api_url(_test_url, 'com') == ('https://not_a_site.com', 'api/object?something=other')


@pytest.mark.requests
def test_local_token(mocker: pytest_mock.MockerFixture):
    _dummy_key = 'sdfd234ersdf45234'
    with tempfile.TemporaryDirectory() as tempd:
        _token_file = os.path.join(tempd, 'token')
        mocker.patch('fair.common.registry_home', lambda: tempd)
        with pytest.raises(fdp_exc.FileNotFoundError):
            fdp_req.local_token()
        open(_token_file, 'w').write(_dummy_key)
        assert fdp_req.local_token() == _dummy_key


@pytest.mark.requests
@pytest.mark.dependency(name='post')
def test_post(local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    _name = 'Joseph Bloggs'
    _orcid = 'https://orcid.org/0000-0000-0000-0000'
    with local_registry:
        _result = fdp_req.post(
            LOCAL_URL,
            'author',
            data={'name': _name, 'identifier': _orcid}
        )
        assert _result['url']


@pytest.mark.requests
@pytest.mark.dependency(name='get', depends=['post'])
def test_get(local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    with local_registry:
        assert fdp_req.get(LOCAL_URL, 'author')


@pytest.mark.requests
def test_post_else_get(local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    with local_registry:

        _data = {'name': 'Comma Separated Values', 'extension': 'csv'}
        _params = {'extension': 'csv'}
        _obj_path = 'file_type'
        
        mock_post = mocker.patch('fair.registry.requests.post')
        mock_get = mocker.patch('fair.registry.requests.get')
        # Perform method twice, first should post, second retrieve
        assert fdp_req.post_else_get(
            LOCAL_URL,
            _obj_path,
            data=_data,
            params=_params
        )

        mock_post.assert_called_once()
        mock_get.assert_not_called()

        mocker.resetall()

        def raise_it(*kwargs, **args):
            raise fdp_exc.RegistryAPICallError("woops", error_code=409)

        mocker.patch('fair.common.registry_home', lambda: local_registry._install)
        mocker.patch('fair.registry.requests.post', raise_it)
        mock_get = mocker.patch('fair.registry.requests.get')

        assert fdp_req.post_else_get(
            LOCAL_URL,
            'file_type',
            data={'name': 'Comma Separated Values', 'extension': 'csv'},
            params={'extension': 'csv'}
        )

        mock_get.assert_called_once()

