import os
import pathlib
import typing

import click.testing
import pytest
import pytest_mock
import yaml

import fair.registry.sync as fdp_sync
import fair.registry.server as fdp_serv
from fair.cli import cli
from fair.common import FAIR_FOLDER
from fair.registry.requests import get, url_get
from tests.conftest import RegistryTest
from tests.conftest import MotoTestServer
import fair.session as fdp_session
import fair.common as fdp_com
import fair.testing as fdp_test

REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(
    os.path.dirname(__file__), "data", "test_pull_config.yaml"
)

@pytest.mark.faircli_sync
def test_pull_download():

    _root = "https://github.com/"
    _path = "FAIRDataPipeline/FAIR-CLI/blob/main/README.md"

    _file = fdp_sync.download_from_registry(
        "http://127.0.0.1:8000", _root, _path
    )

    assert open(_file).read()


@pytest.mark.faircli_sync
def test_fetch_data_product(mocker: pytest_mock.MockerFixture, tmp_path):

    tempd = tmp_path.__str__()
    _dummy_data_product_name = "test"
    _dummy_data_product_version = "2.3.0"
    _dummy_data_product_namespace = "testing"
    def mock_get(url, obj, *args, **kwargs):
        if obj == "storage_location":
            return [
                {
                    "path": "/this/is/a/dummy/path",
                    "storage_root": "https://dummyurl/",
                }
            ]
        elif obj == "storage_root":
            return [{"root": "https://fake/root/"}]
        elif obj == "namespace":
            return [
                {"name": _dummy_data_product_namespace, "url": "namespace"}
            ]
        elif obj == "data_product":
            return [
                {
                    "data_product": _dummy_data_product_name,
                    "version": _dummy_data_product_version,
                    "namespace": "namespace",
                }
            ]
    def mock_url_get(url, *args, **kwargs):
        if "storage_location" in url:
            return {
                "path": "FAIRDataPipeline/FAIR-CLI/archive/refs/heads/main.zip",
                "storage_root": "storage_root",
            }
        elif "storage_root" in url:
            return {"root": "https://github.com/"}
        elif "namespace" in url:
            return {
                "name": _dummy_data_product_namespace,
                "url": "namespace",
            }
        elif "object" in url:
            return {
                "storage_location": "storage_location",
                "url": "object",
            }
    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    _example_data_product = {
        "version": _dummy_data_product_version,
        "namespace": "namespace",
        "name": _dummy_data_product_name,
        "data_product": _dummy_data_product_name,
        "object": "object",
    }
    fdp_sync.fetch_data_product("", tempd, _example_data_product)

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="init")
def test_init(
    global_config,
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    monkeypatch_module.chdir(pyDataPipeline)
    monkeypatch_module.setattr(
        "fair.registry.server.launch_server", lambda *args, **kwargs: False
    )
    _cli_runner = click.testing.CliRunner()
    config_path = os.path.join(
        pyDataPipeline, fdp_com.FAIR_CLI_CONFIG
    )
    _config = fdp_test.create_configurations(local_registry._install, pyDataPipeline, remote_registry._install, global_config, True)
    yaml.dump(_config, open(config_path, "w"))
    with capsys.disabled():
        print (_config)
        print(f"\tRUNNING: fair init --debug")
    with local_registry, remote_registry:
        _res = _cli_runner.invoke(
            cli,
            ["init", "--debug", "--using", config_path],
            catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
    assert _res.exit_code == 0

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="pull", depends= ["init"])
def test_pull(
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    _cli_runner = click.testing.CliRunner()
    _cfg_path = os.path.join(
        pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
    )
    with capsys.disabled():
        print(f"\tRUNNING: fair pull {_cfg_path} --debug")
    with local_registry, remote_registry:
        _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])
    assert _res.exit_code == 0

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="run", depends=["pull"])
def test_run(
    global_config: str,
    local_registry: str,
    remote_registry: str,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
        
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry:
        _cfg_path = os.path.join(
                pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
            )
        with capsys.disabled():
            print(f"\tRUNNING: fair pull {_cfg_path} --debug --dirty")
        _res = _cli_runner.invoke(
            cli, ["run", _cfg_path, "--debug", "--dirty"]
        )
        assert _res.exit_code == 0
        assert get(
            "http://127.0.0.1:8000/api/",
            "data_product",
            local_registry._token,
            params={
                "name": "SEIRS_model/results/figure/python",
                "version": "0.0.1",
            },
        )

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="push", depends=["run"])
def test_push(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(
            cli, ["list"]
        )
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["add", "testing:SEIRS_model/results/figure/python@v0.0.1"]
        )
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["push", "--debug"],  catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
        assert _res.exit_code == 0
        assert get(
            "http://127.0.0.1:8001/api/",
            "data_product",
            remote_registry._token,
            params={
                "name": "SEIRS_model/results/figure/python",
                "version": "0.0.1",
            },
        )

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="find", depends=["push"])
def test_find(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(
            cli, ["find", "--debug", "testing:SEIRS_model/results/figure/python@v0.0.1"],  catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
        assert _res.exit_code == 0

        _res = _cli_runner.invoke(
            cli, ["find", "--debug", "--local", "testing:SEIRS_model/results/figure/python@v0.0.1"],  catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
        assert _res.exit_code == 0

@pytest.mark.faircli_sync
@pytest.mark.dependency(name="identify", depends=["push"])
def test_identify(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )

        _seirs_parameters_file = os.path.join(
        pyDataPipeline, "simpleModel", "ext", "static_params_SEIRS.csv"
        )

        _res = _cli_runner.invoke(
            cli, ["identify", "--debug", _seirs_parameters_file],  catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
        assert _res.exit_code == 0

        _res = _cli_runner.invoke(
            cli, ["identify", "--debug", "--local", _seirs_parameters_file],  catch_exceptions = True
        )
        with capsys.disabled():
            print(f'exit code: {_res.exit_code}')
            print(f'exc info: {_res.exc_info}')
            print(f'exception: {_res.exception}')
        assert _res.exit_code == 0

