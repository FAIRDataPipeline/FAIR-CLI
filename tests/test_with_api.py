import os.path
import pathlib
import typing
import yaml

import click.testing
import git
import pytest
import pytest_mock

from fair.cli import cli
from fair.common import FAIR_CLI_CONFIG, FAIR_FOLDER
from fair.registry.requests import get
from tests.conftest import RegistryTest

PYTHON_API_GIT = "https://github.com/FAIRDataPipeline/pyDataPipeline.git"
REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent


@pytest.mark.with_api
@pytest.mark.dependency(name='pull')
def test_pull(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    capsys):
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    _cli_runner = click.testing.CliRunner()
    _proj_dir = os.path.join(local_config[1], "code")
    _repo = git.Repo.clone_from(PYTHON_API_GIT, to_path=_proj_dir)
    _repo.git.checkout("dev")
    with _cli_runner.isolated_filesystem(_proj_dir):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(_proj_dir, FAIR_FOLDER), exist_ok=True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            _cfg_path = os.path.join(
                _proj_dir,
                "src",
                "org",
                "fairdatapipeline",
                "simpleModel",
                "ext",
                "SEIRSconfig.yaml"
            )
            with capsys.disabled():
                print(f"\tRUNNING: fair pull {_cfg_path} --debug")
            _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])

            assert _res.exit_code == 0
            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                params={
                    "name": "SEIRS_model/parameters",
                }
            )
            assert get(
                "http://127.0.0.1:8000/api/",
                "namespace",
                params={
                    "name": "testing"
                }
            )

@pytest.mark.with_api
@pytest.mark.dependency(name='push', depends=['pull'])
def test_push(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    capsys):
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    _cli_runner = click.testing.CliRunner()
    _proj_dir = os.path.join(local_config[1], "code")
    _repo = git.Repo.clone_from(PYTHON_API_GIT, to_path=_proj_dir)
    _repo.git.checkout("dev")
    with _cli_runner.isolated_filesystem(_proj_dir):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(_proj_dir, FAIR_FOLDER), exist_ok=True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))

            with capsys.disabled():
                print("\tRUNNING: fair add testing:SEIRS_model/parameters@v1.0.0")

            _res = _cli_runner.invoke(cli, ["add", "testing:SEIRS_model/parameters@v1.0.0"])
            
            with capsys.disabled():
                print("\tRUNNING: fair push")

            _res = _cli_runner.invoke(cli, ["push", "--debug"])

            assert _res.exit_code == 0
            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                {"name": "SEIRS_model/parameters", "version": "1.0.0"},
                token=remote_registry._token
            )
