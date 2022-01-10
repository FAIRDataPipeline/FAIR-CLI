import os.path
import typing

import pytest
import pytest_mock

import fair.user_config as fdp_user
import fair.common as fdp_com

from . import conftest as conf


@pytest.fixture
def make_config(local_config: typing.Tuple[str, str], pyDataPipeline: str):
    _cfg_path = os.path.join(
        pyDataPipeline,
        "simpleModel",
        "ext",
        "SEIRSconfig.yaml"
    )
    _config = fdp_user.JobConfiguration(_cfg_path)
    _config.update_from_fair(os.path.join(local_config[1], "project"))
    return _config


@pytest.mark.user_config
def test_get_value(
    local_config: typing.Tuple[str, str],
    make_config: fdp_user.JobConfiguration,
):
    assert make_config["run_metadata.description"] == "SEIRS Model python"
    assert make_config["run_metadata.local_repo"] == os.path.join(
        local_config[1], "project"
    )


@pytest.mark.user_config
def test_set_value(make_config: fdp_user.JobConfiguration):
    make_config["run_metadata.description"] = "a new description"
    assert (
        make_config._config["run_metadata"]["description"]
        == "a new description"
    )


@pytest.mark.user_config
def test_is_public(make_config: fdp_user.JobConfiguration):
    assert make_config.is_public_global
    make_config["run_metadata.public"] = False
    assert not make_config.is_public_global


@pytest.mark.user_config
def test_default_input_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_input_namespace == "rfield"


@pytest.mark.user_config
def test_default_output_namespace(make_config: fdp_user.JobConfiguration):
    assert make_config.default_output_namespace == "testing"


@pytest.mark.user_config
def test_preparation(
    mocker: pytest_mock.MockerFixture,
    make_config: fdp_user.JobConfiguration,
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        os.makedirs(os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "logs"))
        make_config.prepare(fdp_com.CMD_MODE.PULL, True)
        make_config.write("test.yaml")
