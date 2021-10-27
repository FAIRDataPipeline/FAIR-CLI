"""
CLI Tests
---------

As tests are provided for the CLI backend itself, the following only check that
all CLI commands run without any errors

"""

import os
import tempfile
import typing

from urllib.parse import urljoin

import click.testing
import fair.common as fdp_com
import git
import pytest
import pytest_mock

from tests import conftest as conf
import yaml
from fair.cli import cli
from fair.registry.server import DEFAULT_REGISTRY_DOMAIN


@pytest.fixture
def click_test():
    click_test = click.testing.CliRunner()
    with click_test.isolated_filesystem():
        _repo = git.Repo.init(os.getcwd())
        _repo.create_remote('origin', 'git@notagit.com')
        yield click_test


@pytest.mark.cli
def test_status(local_registry: conf.TestRegistry, click_test: click.testing.CliRunner, mocker: pytest_mock.MockerFixture):
    with local_registry:
        mocker.patch('fair.common.registry_home', lambda: local_registry._install)
        _result = click_test.invoke(cli, ['status', '--debug', '--verbose'])
        assert _result.exit_code == 0


@pytest.mark.cli
def test_create(local_registry: conf.TestRegistry, click_test: click.testing.CliRunner, local_config: typing.Tuple[str, str], mocker: pytest_mock.MockerFixture):
    with local_registry:
        mocker.patch('fair.common.registry_home', lambda: local_registry._install)
        _out_config = os.path.join(os.getcwd(), 'config.yaml')
        _result = click_test.invoke(cli, ['create', '--debug', _out_config])
        assert _result.exit_code == 0
        assert os.path.exists(_out_config)


@pytest.mark.cli
def test_init_from_existing(local_registry: conf.TestRegistry, click_test: click.testing.CliRunner, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    
    _out_config = os.path.join(os.getcwd(), 'config.yaml')
    
    with tempfile.TemporaryDirectory() as tempd:
        _out_cli_config = os.path.join(tempd, 'cli-config.yaml')
        with local_registry:
            _result = click_test.invoke(
                cli,
                [
                    'init',
                    '--debug',
                    '--ci',
                    '--config',
                    _out_config,
                    '--export',
                    _out_cli_config
                ]
            )
            assert _result.exit_code == 0
            assert os.path.exists(_out_cli_config)
            assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))

            click_test = click.testing.CliRunner()
            click_test.isolated_filesystem()

            _result = click_test.invoke(
                cli,
                [
                    'init',
                    '--debug',
                    '--using',
                    _out_cli_config
                ]
            )

        assert _result.exit_code == 0
        
        assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))


@pytest.mark.cli
def test_init_full(local_registry: conf.TestRegistry, click_test: click.testing.CliRunner, mocker: pytest_mock.MockerFixture):
    mocker.patch('fair.common.registry_home', lambda: local_registry._install)
    mocker.patch('fair.registry.server.update_registry_post_setup', lambda *args: None)
    with local_registry:
        with tempfile.TemporaryDirectory() as tempd:
            mocker.patch('fair.common.USER_FAIR_DIR', tempd)
            _dummy_name = 'Joseph Bloggs'
            _dummy_email = 'jbloggs@nowhere.com'
            _args = [
                '',
                '',
                '',
                '',
                '',
                '',
                _dummy_email,
                '',
                _dummy_name,
                'testing',
                '',
                os.getcwd(),
                ''
            ]

            _result = click_test.invoke(cli, ['init', '--debug'], input='\n'.join(_args))

            assert _result.exit_code == 0

            assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))

            _cli_cfg = yaml.safe_load(open(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER, 'cli-config.yaml')))

            assert _cli_cfg
            assert _cli_cfg['git']['local_repo'] == os.getcwd()
            assert _cli_cfg['git']['remote'] == 'origin'
            assert _cli_cfg['git']['remote_repo'] == 'git@notagit.com'
            assert _cli_cfg['namespaces']['input'] == 'testing'
            assert _cli_cfg['namespaces']['output'] == 'jbloggs'
            assert _cli_cfg['registries']['origin']['data_store'] == urljoin(DEFAULT_REGISTRY_DOMAIN, 'data/')
            assert _cli_cfg['registries']['origin']['uri'] == urljoin(DEFAULT_REGISTRY_DOMAIN, 'api/')
            assert _cli_cfg['user']['email'] == _dummy_email
            assert _cli_cfg['user']['family_name'] == 'Bloggs'
            assert _cli_cfg['user']['given_names'] == 'Joseph'
            assert _cli_cfg['user']['uuid']
