import os

import click.testing
import pytest
import yaml

from fair.cli import cli
import fair.common as fdp_com
import fair.testing as fdp_test

TEST_DATA_DIR = f"file://{os.path.dirname(__file__)}{os.path.sep}data{os.path.sep}"

TEST_REGISTER_CFG = os.path.join(
    os.path.dirname(__file__), "data", "test_register.yaml"
)


@pytest.mark.faircli_register
def test_register(
    global_config,
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    tmp_path,
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
    config_path = os.path.join(pyDataPipeline, fdp_com.FAIR_CLI_CONFIG)
    _config = fdp_test.create_configurations(
        local_registry._install,
        pyDataPipeline,
        remote_registry._install,
        global_config,
        True,
    )
    yaml.dump(_config, open(config_path, "w"))
    with capsys.disabled():
        print(_config)
        print("\tRUNNING: fair init --debug")
    with local_registry, remote_registry:
        _res = _cli_runner.invoke(
            cli, ["init", "--debug", "--using", config_path], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0

        _cfg_path = os.path.join(
            pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
        )
        _res = _cli_runner.invoke(
            cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0

        _working_yaml_path = os.path.join(tmp_path, "working_yaml.yaml")
        _cfg_str = {}

        with open(TEST_REGISTER_CFG) as cfg_file:
            _cfg_str = cfg_file.read()

        print(f"Test Data Directory {TEST_DATA_DIR}")
        _cfg_str = _cfg_str.replace("<TEST_DATA_DIR>", TEST_DATA_DIR)

        _cfg = yaml.safe_load(_cfg_str)

        with open(_working_yaml_path, "w") as f:
            yaml.dump(_cfg, f, sort_keys=False)

        _res = _cli_runner.invoke(
            cli, ["pull", _working_yaml_path, "--debug"], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0
