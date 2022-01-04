import os.path
import pathlib
import typing
import yaml

import click.testing
import git
import pytest
import pytest_mock

from fair.cli import cli
from fair.common import FAIR_FOLDER, default_data_dir
from fair.registry.requests import get
from tests.conftest import RegistryTest
import fair.registry.server as fdp_serv

PYTHON_API_GIT = "https://github.com/FAIRDataPipeline/pyDataPipeline.git"
REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(os.path.dirname(__file__), "data", "test_pull_config.yaml")


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
            _data = os.path.join(local_registry._install, "data")
            os.makedirs(_data, exist_ok=True)
            fdp_serv.update_registry_post_setup(_proj_dir, True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            _cfg_path = os.path.join(
                _proj_dir,
                "src",
                "org",
                "fairdatapipeline",
                "simpleModel",
                "ext",
                "SEIRSconfig.yaml"
            )
            with open(_cfg_path) as cfg_file:
                _cfg = yaml.safe_load(cfg_file)
            
            _cfg["run_metadata"]["write_data_store"] = _data
            with open(_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)
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

            assert get(
                "http://127.0.0.1:8000/api/",
                "user_author"
            )


@pytest.mark.with_api
def test_pull_new(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    capsys):
    _manage = os.path.join(remote_registry._install, "manage.py")
    remote_registry._venv.run(f"python {_manage} add_example_data")
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
            _data = os.path.join(local_registry._install, "data")
            os.makedirs(_data, exist_ok=True)
            fdp_serv.update_registry_post_setup(_proj_dir, True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            with open(PULL_TEST_CFG) as cfg_file:
                _cfg = yaml.safe_load(cfg_file)
            _cfg_path = os.path.join(remote_registry._install, "config.yaml")
            
            _cfg["run_metadata"]["write_data_store"] = _data
            with open(_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)
            with capsys.disabled():
                print(f"\tRUNNING: fair pull {_cfg_path} --debug")
            _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])

            assert not _res.output
            assert _res.output
            assert _res.exit_code == 0
            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                params={
                    "name": "disease/sars_cov2/SEINRD_model/parameters/efoi",
                }
            )

@pytest.mark.with_api
@pytest.mark.dependency(name='run', depends=['pull'])
def test_run(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    capsys):
    try:
        import org.fairdatapipeline
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
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
            _data = os.path.join(local_registry._install, "data")
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            os.makedirs(_data, exist_ok=True)

            assert os.path.exists(
                os.path.join(
                    _data,
                    "testing",
                    "SEIRS_model",
                    "parameters",
                    "1.0.0.csv"
                )
            )
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {"testing:SEIRS_model/parameters@v1.0.0": False}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            
            assert get(
                "http://127.0.0.1:8000/api/",
                "user_author"
            )
            

            _cfg_path = os.path.join(
                _proj_dir,
                "src",
                "org",
                "fairdatapipeline",
                "simpleModel",
                "ext",
                "SEIRSconfig.yaml"
            )

            with open(_cfg_path) as cfg_file:
                _cfg = yaml.safe_load(cfg_file)
            
            _cfg["run_metadata"]["script"] = _cfg["run_metadata"]["script"].replace("src", f"{_proj_dir}/src")
            _cfg["run_metadata"]["write_data_store"] = _data

            with open(_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)

            with capsys.disabled():
                print(f"\tRUNNING: fair run {_cfg_path} --debug")

            _res = _cli_runner.invoke(cli, ["run", _cfg_path, "--debug", "--dirty"])

            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                params={
                    "name": "SEIRS_model/results/figure/python",
                    "version": "0.0.1"
                }
            )


@pytest.mark.with_api
@pytest.mark.dependency(name='push', depends=['pull'])
def test_push_initial(local_config: typing.Tuple[str, str],
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
            _data = os.path.join(local_registry._install, "data")
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            os.makedirs(_data, exist_ok=True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {"testing:SEIRS_model/parameters@v1.0.0": False}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            fdp_serv.update_registry_post_setup(_proj_dir, True)

            with capsys.disabled():
                print("\tRUNNING: fair add testing:SEIRS_model/parameters@v1.0.0")

            _res = _cli_runner.invoke(cli, ["add", "testing:SEIRS_model/parameters@v1.0.0"])

            assert _res.exit_code == 0

            with capsys.disabled():
                print("\tRUNNING: fair push")

            _res = _cli_runner.invoke(cli, ["push", "--debug"])

            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                params={"name": "SEIRS_model/parameters", "version": "1.0.0"},
                token=remote_registry._token
            )


@pytest.mark.with_api
@pytest.mark.dependency(name='push', depends=['pull', 'run'])
def test_push_postrun(local_config: typing.Tuple[str, str],
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
                yaml.dump({"data_product": {"testing:SEIRS_model/results/figure/python@v0.0.1": False}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(_proj_dir, FAIR_FOLDER, "staging"))
            fdp_serv.update_registry_post_setup(_proj_dir, True)
            with open(os.path.join(_proj_dir, FAIR_FOLDER, "staging")) as cfg:
                _staging = yaml.safe_load(cfg)
            assert "testing:SEIRS_model/results/figure/python@v0.0.1" in _staging["data_product"]
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: os.path.join(local_registry._install, "data"))
            with capsys.disabled():
                print("\tRUNNING: fair add testing:SEIRS_model/results/figure/python@v0.0.1")

            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                params={
                    "name": "SEIRS_model/results/figure/python",
                    "version": "0.0.1"
                }
            )

            _res = _cli_runner.invoke(cli, ["add", "testing:SEIRS_model/results/figure/python@v0.0.1"])

            assert _res.exit_code == 0

            with capsys.disabled():
                print("\tRUNNING: fair push")

            _res = _cli_runner.invoke(cli, ["push", "--debug"])

            assert _res.output
            assert not _res.output
            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                params={"name": "SEIRS_model/results/figure/python", "version": "0.0.1"},
                token=remote_registry._token
            )
