from datetime import datetime
import typing
import pytest
import os.path
import git
import pathlib
import pytest_mock
from fair.common import CMD_MODE

import fair.user_config as fdp_user
import fair.exceptions as fdp_exc
from . import conftest as conf

TEST_USER_CONFIG = os.path.join(
    os.path.dirname(__file__),
    'data',
    'test_config.yaml'
)


@pytest.fixture
def make_config(local_config: typing.Tuple[str, str]):
    _config = fdp_user.JobConfiguration(
        TEST_USER_CONFIG,
    )
    _config.update_from_fair(os.path.join(local_config[1], 'project'))
    return _config


@pytest.mark.user_config
def test_get_value(local_config: typing.Tuple[str, str], make_config: fdp_user.JobConfiguration):
    assert make_config['run_metadata.description'] == 'SEIRS Model R'
    assert make_config['run_metadata.local_repo'] == os.path.join(local_config[1], 'project')


@pytest.mark.user_config
def test_set_value(make_config: fdp_user.JobConfiguration):
    make_config['run_metadata.description'] = 'a new description'
    assert make_config._config['run_metadata']['description'] == 'a new description'


@pytest.mark.user_config
def test_is_public(make_config: fdp_user.JobConfiguration):
    assert make_config.is_public_global
    make_config['run_metadata.public'] = False
    assert not make_config.is_public_global


@pytest.mark.user_config
def test_default_input_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_input_namespace == 'unit_testing'


@pytest.mark.user_config
def test_default_output_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_output_namespace == 'testing'


@pytest.mark.user_config
def test_preparation(
    mocker: pytest_mock.MockerFixture,
    make_config: fdp_user.JobConfiguration,
    local_config: typing.Tuple[str, str],
    local_registry: conf.TestRegistry):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    with local_registry:
        make_config.prepare(local_config[1], datetime.now(), CMD_MODE.PULL)
        make_config.write('test.yaml')
