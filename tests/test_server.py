import pytest
import pytest_mock

from . import conftest as conf
import fair.registry.server as fdp_serv

LOCAL_REGISTRY_URL = 'http://localhost:8000/api'


@pytest.mark.server
def test_check_server_running(local_registry: conf.TestRegistry, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    assert not fdp_serv.check_server_running('http://localhost:9999/api')
    with local_registry:
        assert fdp_serv.check_server_running(LOCAL_REGISTRY_URL)

