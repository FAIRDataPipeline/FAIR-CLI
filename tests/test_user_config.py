import os.path
import typing

import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.user_config as fdp_user

from . import conftest as conf

TEST_CONFIG_WC = os.path.join(
    os.path.dirname(__file__), "data", "test_wildcards_config.yaml"
)


@pytest.fixture
def make_config(local_config: typing.Tuple[str, str], pyDataPipeline: str):
    _cfg_path = os.path.join(pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml")
    _config = fdp_user.JobConfiguration(_cfg_path)
    _config.update_from_fair(os.path.join(local_config[1], "project"))
    return _config


@pytest.mark.faircli_user_config
def test_get_value(
    local_config: typing.Tuple[str, str],
    make_config: fdp_user.JobConfiguration,
):
    assert make_config["run_metadata.description"] == "SEIRS Model python"
    assert make_config["run_metadata.local_repo"] == os.path.join(
        local_config[1], "project"
    )


@pytest.mark.faircli_user_config
def test_set_value(make_config: fdp_user.JobConfiguration):
    make_config["run_metadata.description"] = "a new description"
    assert make_config._config["run_metadata"]["description"] == "a new description"


@pytest.mark.faircli_user_config
def test_is_public(make_config: fdp_user.JobConfiguration):
    assert make_config.is_public_global
    make_config["run_metadata.public"] = False
    assert not make_config.is_public_global


@pytest.mark.faircli_user_config
def test_default_input_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_input_namespace == "rfield"


@pytest.mark.faircli_user_config
def test_default_output_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_output_namespace == "testing"


@pytest.mark.faircli_user_config
def test_preparation(
    mocker: pytest_mock.MockerFixture,
    make_config: fdp_user.JobConfiguration,
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        os.makedirs(os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "logs"))
        make_config.prepare(fdp_com.CMD_MODE.RUN, True)

        _out_dir = os.path.join(conf.TEST_OUT_DIR, "test_preparation")
        os.makedirs(_out_dir, exist_ok=True)

        make_config.write(os.path.join(_out_dir, "out.yaml"))


@pytest.mark.faircli_user_config
def test_wildcard_unpack_local(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    local_registry: conf.RegistryTest,
):
    with local_registry:
        os.makedirs(os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "logs"))
        _manage = os.path.join(local_registry._install, "manage.py")
        local_registry._venv.run(
            "python", f"{_manage}", "add_example_data", capture=True
        )
        mocker.patch(
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
        )
        _data = os.path.join(local_registry._install, "data")
        _example_entries = conf.get_example_entries(local_registry._install)

        _out_dir = os.path.join(conf.TEST_OUT_DIR, "test_wildcard_unpack_local")
        os.makedirs(_out_dir, exist_ok=True)

        _namespace, _path, _ = _example_entries[0]

        _split_key = _path.split("/")[-1]

        _wildcard_path = _path.split(_split_key)[0] + "*"

        with open(TEST_CONFIG_WC) as cfg_file:
            _cfg_str = cfg_file.read()

        _cfg_str = _cfg_str.replace("<NAMESPACE>", _namespace)
        _cfg_str = _cfg_str.replace("<WILDCARD-PATH>", _wildcard_path)

        _cfg = yaml.safe_load(_cfg_str)
        _cfg["run_metadata"]["write_data_store"] = _data

        _new_cfg_path = os.path.join(_out_dir, "in.yaml")

        yaml.dump(_cfg, open(_new_cfg_path, "w"))

        _config = fdp_user.JobConfiguration(_new_cfg_path)
        _config.update_from_fair(os.path.join(local_config[1], "project"))
        _config.prepare(fdp_com.CMD_MODE.RUN, True)
        assert len(_config["read"]) > 1

        _config.write(os.path.join(_out_dir, "out.yaml"))


@pytest.mark.faircli_user_config
def test_wildcard_unpack_remote(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    local_registry: conf.RegistryTest,
    remote_registry: conf.RegistryTest,
):
    with local_registry, remote_registry:
        os.makedirs(os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "logs"))
        _manage = os.path.join(remote_registry._install, "manage.py")
        remote_registry._venv.run(
            "python", f"{_manage}", "add_example_data", capture=True
        )
        mocker.patch(
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
        )
        mocker.patch(
            "fair.configuration.get_remote_token",
            lambda *args: remote_registry._token,
        )
        _data = os.path.join(local_registry._install, "data")
        _example_entries = conf.get_example_entries(remote_registry._install)

        _out_dir = os.path.join(conf.TEST_OUT_DIR, "test_wildcard_unpack_remote")
        os.makedirs(_out_dir, exist_ok=True)

        _namespace, _path, _ = _example_entries[0]

        _split_key = _path.split("/")[-1]

        _wildcard_path = _path.split(_split_key)[0] + "*"

        with open(TEST_CONFIG_WC) as cfg_file:
            _cfg_str = cfg_file.read()

        _cfg_str = _cfg_str.replace("<NAMESPACE>", _namespace)
        _cfg_str = _cfg_str.replace("<WILDCARD-PATH>", _wildcard_path)

        _cfg = yaml.safe_load(_cfg_str)
        _cfg["run_metadata"]["write_data_store"] = _data

        _new_cfg_path = os.path.join(_out_dir, "in.yaml")

        yaml.dump(_cfg, open(_new_cfg_path, "w"))

        _config = fdp_user.JobConfiguration(_new_cfg_path)
        _config.update_from_fair(os.path.join(local_config[1], "project"))
        _config.prepare(
            fdp_com.CMD_MODE.PULL,
            True,
            remote_registry._url,
            remote_registry._token,
        )
        assert len(_config["read"]) > 1

        _config.write(os.path.join(_out_dir, "out.yaml"))
