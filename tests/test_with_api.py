import pathlib
import typing
import yaml
import shutil
import os

import click.testing
import pytest
import pytest_mock

from fair.cli import cli
from fair.common import FAIR_FOLDER
from fair.registry.requests import get, url_get
from tests.conftest import RegistryTest, get_example_entries
import fair.registry.server as fdp_serv

REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(os.path.dirname(__file__), "data", "test_pull_config.yaml")


@pytest.mark.faircli_pull
@pytest.mark.dependency(name='pull_new')
def test_pull_new(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys):
    _manage = os.path.join(remote_registry._install, "manage.py")
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    mocker.patch("fair.registry.sync.fetch_data_product", lambda *args, **kwargs: None)
    _cli_runner = click.testing.CliRunner()
    with _cli_runner.isolated_filesystem(pyDataPipeline):
        with remote_registry, local_registry:
            remote_registry._venv.run(f"python {_manage} add_example_data", capture=True)
            os.makedirs(os.path.join(pyDataPipeline, FAIR_FOLDER), exist_ok=True)
            _data = os.path.join(local_registry._install, "data")
            os.makedirs(_data, exist_ok=True)
            fdp_serv.update_registry_post_setup(pyDataPipeline, True)
            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"))
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            _namespace, _path, _version = get_example_entries(remote_registry._install)[0]

            with open(PULL_TEST_CFG) as cfg_file:
                _cfg_str = cfg_file.read()

            _cfg_str = _cfg_str.replace("<NAMESPACE>", _namespace)
            _cfg_str = _cfg_str.replace("<VERSION>", _version)
            _cfg_str = _cfg_str.replace("<PATH>", _path)
            
            _cfg = yaml.safe_load(_cfg_str)

            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                remote_registry._token,
                params={
                    "name": _path,
                }
            )
            
            _cfg["run_metadata"]["write_data_store"] = _data
            _cfg["run_metadata"]["local_repo"] = pyDataPipeline

            _new_cfg_path = os.path.join(os.path.dirname(pyDataPipeline), 'config.yaml')

            with open(_new_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)

            with capsys.disabled():
                print(f"\tRUNNING: fair pull {_new_cfg_path} --debug")
            _res = _cli_runner.invoke(cli, ["pull", _new_cfg_path, "--debug"])

            assert not _res.output
            assert _res.output
            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                local_registry._token,
                params={
                    "name": _path,
                }
            )


@pytest.mark.faircli_run
@pytest.mark.faircli_push
@pytest.mark.faircli_pull
@pytest.mark.dependency(name='pull_existing')
def test_pull_existing(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys):
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    mocker.patch("fair.registry.sync.fetch_data_product", lambda *args, **kwargs: None)
    _cli_runner = click.testing.CliRunner()
    with _cli_runner.isolated_filesystem(pyDataPipeline):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(pyDataPipeline, FAIR_FOLDER), exist_ok=True)
            _data = os.path.join(local_registry._install, "data")
            if os.path.exists(_data):
                shutil.rmtree(_data)
            os.makedirs(_data)
            fdp_serv.update_registry_post_setup(pyDataPipeline, True)
            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"))
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            _cfg_path = os.path.join(
                pyDataPipeline,
                "simpleModel",
                "ext",
                "SEIRSconfig.yaml"
            )
            with open(_cfg_path) as cfg_file:
                _cfg = yaml.safe_load(cfg_file)
            
            _cfg["run_metadata"]["write_data_store"] = _data
            _cfg["run_metadata"]["local_repo"] = pyDataPipeline

            _new_cfg_path = os.path.join(os.path.dirname(pyDataPipeline), 'config.yaml')

            with open(_new_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)

            with capsys.disabled():
                print(f"\tRUNNING: fair pull {_new_cfg_path} --debug")
            _res = _cli_runner.invoke(cli, ["pull", _new_cfg_path, "--debug"])

            assert _res.exit_code == 0

            _param_files = get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                local_registry._token,
                params={
                    "name": "SEIRS_model/parameters",
                    "version": "1.0.0"
                }
            )

            assert _param_files

            assert get(
                "http://127.0.0.1:8000/api/",
                "namespace",
                local_registry._token,
                params={
                    "name": "PSU"
                }
            )


@pytest.mark.faircli_pull
@pytest.mark.skipif('CI' in os.environ, reason="Fails on GH CI")
@pytest.mark.dependency(name='check_local_files', depends=['pull_existing'])
def test_local_files_present(
    local_registry: RegistryTest
    ):
    with local_registry:
        _param_files = get(
            "http://127.0.0.1:8000/api/",
            "data_product",
            local_registry._token,
            params={
                "name": "SEIRS_model/parameters",
                "version": "1.0.0"
            }
        )
        _param_file_obj = url_get(_param_files[0]["object"], local_registry._token)
        _store = url_get(_param_file_obj["storage_location"], local_registry._token)
        _path = _store["path"]
        _root = url_get(_store["storage_root"], local_registry._token)
        _root = _root["root"]

    assert os.path.exists(os.path.join(_root.replace("file://", ""), _path))


@pytest.mark.faircli_run
@pytest.mark.faircli_push
@pytest.mark.dependency(name='run', depends=['pull_existing'])
def test_run(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys):
    try:
        import fairdatapipeline
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    _cli_runner = click.testing.CliRunner()
    with _cli_runner.isolated_filesystem(pyDataPipeline):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(pyDataPipeline, FAIR_FOLDER), exist_ok=True)
            _data = os.path.join(local_registry._install, "data")
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            os.makedirs(_data, exist_ok=True)

            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump(
                    {
                        "data_product": {
                            "testing:SEIRS_model/parameters@v1.0.0": False
                        },
                        "file": {},
                        "job": {}
                    },
                    sf
                )

            mocker.patch(
                "fair.common.staging_cache",
                lambda *args: os.path.join(pyDataPipeline, FAIR_FOLDER, "staging")
            )
            
            assert get(
                "http://127.0.0.1:8000/api/",
                "user_author",
                local_registry._token
            )
            

            _cfg_path = os.path.join(
                pyDataPipeline,
                "simpleModel",
                "ext",
                "SEIRSconfig.yaml"
            )

            _new_cfg_path = os.path.join(os.path.dirname(pyDataPipeline), 'config.yaml')

            with open(_cfg_path) as cfg_file:
                _cfg = yaml.safe_load(cfg_file)
            
            _cfg["run_metadata"]["local_repo"] = pyDataPipeline
            _cfg["run_metadata"]["write_data_store"] = _data

            with open(_new_cfg_path, "w") as cfg_file:
                yaml.dump(_cfg, cfg_file)

            assert os.path.exists(os.path.join(pyDataPipeline, "simpleModel", "ext", "SEIRSModelRun.py"))

            with capsys.disabled():
                print(f"\tRUNNING: fair run {_new_cfg_path} --debug")

            _res = _cli_runner.invoke(cli, ["run", _new_cfg_path, "--debug", "--dirty"])

            assert _res.exit_code == 0

            print(
                get("http://127.0.0.1:8000/api/",
                "data_product",
                local_registry._token)
            )

            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                local_registry._token,
                params={
                    "name": "SEIRS_model/results/figure/python",
                    "version": "0.0.1"
                }
            )


@pytest.mark.faircli_push
@pytest.mark.dependency(name='push', depends=['pull_existing'])
def test_push_initial(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys):
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    _cli_runner = click.testing.CliRunner()
    with _cli_runner.isolated_filesystem(pyDataPipeline):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(pyDataPipeline, FAIR_FOLDER), exist_ok=True)
            _data = os.path.join(local_registry._install, "data")
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: _data)
            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {"PSU:SEIRS_model/parameters@v1.0.0": False}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"))
            fdp_serv.update_registry_post_setup(pyDataPipeline, True)

            with capsys.disabled():
                print("\tRUNNING: fair add PSU:SEIRS_model/parameters@v1.0.0")

            _res = _cli_runner.invoke(cli, ["add", "PSU:SEIRS_model/parameters@v1.0.0"])

            assert _res.exit_code == 0

            with capsys.disabled():
                print("\tRUNNING: fair push")

            _res = _cli_runner.invoke(cli, ["push", "--debug"])

            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                remote_registry._token,
                params={"name": "SEIRS_model/parameters", "version": "1.0.0"},
            )


@pytest.mark.faircli_push
@pytest.mark.dependency(name='push', depends=['pull_existing', 'run'])
def test_push_postrun(local_config: typing.Tuple[str, str],
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys):
    mocker.patch("fair.configuration.get_remote_token", lambda *args: remote_registry._token)
    mocker.patch("fair.registry.requests.local_token", lambda *args: local_registry._token)
    mocker.patch("fair.registry.server.launch_server", lambda *args, **kwargs: True)
    mocker.patch("fair.registry.server.stop_server", lambda *args: True)
    _cli_runner = click.testing.CliRunner()
    with _cli_runner.isolated_filesystem(pyDataPipeline):
        with remote_registry, local_registry:
            os.makedirs(os.path.join(pyDataPipeline, FAIR_FOLDER), exist_ok=True)
            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"), "w") as sf:
                yaml.dump({"data_product": {"testing:SEIRS_model/results/figure/python@v0.0.1": False}, "file": {}, "job": {}}, sf)
            mocker.patch("fair.common.staging_cache", lambda *args: os.path.join(pyDataPipeline, FAIR_FOLDER, "staging"))
            fdp_serv.update_registry_post_setup(pyDataPipeline, True)
            with open(os.path.join(pyDataPipeline, FAIR_FOLDER, "staging")) as cfg:
                _staging = yaml.safe_load(cfg)
            assert "testing:SEIRS_model/results/figure/python@v0.0.1" in _staging["data_product"]
            mocker.patch("fair.configuration.get_local_data_store", lambda *args: os.path.join(local_registry._install, "data"))
            with capsys.disabled():
                print("\tRUNNING: fair add testing:SEIRS_model/results/figure/python@v0.0.1")

            assert get(
                "http://127.0.0.1:8000/api/",
                "data_product",
                local_registry._token,
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

            assert _res.exit_code == 0

            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                remote_registry._token,
                params={"name": "SEIRS_model/results/figure/python", "version": "0.0.1"},
            )
