"""
CLI Tests
---------

As tests are provided for the CLI backend itself, the following only check that
all CLI commands run without any errors

"""

import glob
import os
import shutil
import sys
import typing
import uuid
import platform
from urllib.parse import urljoin
from pathlib import Path

import click.testing
import git
import pytest
import pytest_mock
import requests
import yaml

import fair.common as fdp_com
import fair.staging
from fair.cli import cli
from tests import conftest as conf

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"


@pytest.fixture
def click_test():
    click_test = click.testing.CliRunner()
    with click_test.isolated_filesystem():
        _repo = git.Repo.init(os.getcwd())
        _repo.create_remote("origin", "git@notagit.com")
        yield click_test


@pytest.mark.faircli_cli
def test_status(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
):
    os.makedirs(
        os.path.join(local_config[0], fdp_com.FAIR_FOLDER, "sessions"),
        exist_ok=True,
    )
    os.makedirs(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER), exist_ok=True)
    os.makedirs(os.path.join(os.getcwd(), "jobs"))

    with open(
        os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "staging"), "w"
    ) as staged:
        yaml.dump({"job": {}, "data_product": {}, "code_run": {}}, staged)
    mocker.patch("fair.run.get_job_dir", lambda x: os.path.join(os.getcwd(), "jobs", x))
    _dummy_config = {"run_metadata": {"script": 'echo "Hello World!"'}}
    _dummy_job_staging = {
        "job": {
            str(uuid.uuid4()): True,
            str(uuid.uuid4()): True,
            str(uuid.uuid4()): True,
            str(uuid.uuid4()): False,
            str(uuid.uuid4()): False,
        },
        "file": {},
        "data_product": {},
        "code_run": {},
    }

    _urls_list = {i: "https://dummyurl.com" for i in _dummy_job_staging["job"]}
    mocker.patch.object(fair.staging.Stager, "get_job_data", lambda *args: _urls_list)

    mocker.patch("fair.registry.server.stop_server", lambda *args: None)
    for identifier in _dummy_job_staging["job"]:
        os.makedirs(os.path.join(os.getcwd(), "jobs", identifier))
        yaml.dump(
            _dummy_config,
            open(
                os.path.join(os.getcwd(), "jobs", identifier, fdp_com.USER_CONFIG_FILE),
                "w",
            ),
        )
    yaml.dump(
        _dummy_job_staging,
        open(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER, "staging"), "w"),
    )
    with local_registry:
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        mocker.patch(
            "fair.registry.requests.local_token", lambda: local_registry._token
        )
        _result = click_test.invoke(
            cli, ["status", "--debug", "--verbose"], catch_exceptions=True
        )
        assert _result.exit_code == 0


@pytest.mark.faircli_cli
def test_create(
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
):
    with local_registry:
        os.makedirs(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
        shutil.copy(
            os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "cli-config.yaml"),
            os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER, "cli-config.yaml"),
        )
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        _out_config = os.path.join(os.getcwd(), fdp_com.USER_CONFIG_FILE)
        _result = click_test.invoke(cli, ["create", "--debug", _out_config])
        assert _result.exit_code == 0
        assert os.path.exists(_out_config)


@pytest.mark.faircli_cli
def test_init_from_existing(
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    pySimpleModel,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)

    _out_config = os.path.join(os.getcwd(), fdp_com.USER_CONFIG_FILE)

    tempd = tmp_path.__str__()
    _out_cli_config = os.path.join(tempd, "cli-config.yaml")
    with local_registry:
        _result = click_test.invoke(
            cli,
            [
                "init",
                "--debug",
                "--ci",
                "--registry",
                local_registry._install,
                "--config",
                _out_config,
                "--export",
                _out_cli_config,
            ],
        )
        assert _result.exit_code == 0
        assert os.path.exists(_out_cli_config)
        assert os.path.exists(_out_config)
        assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
        click_test = click.testing.CliRunner()
        click_test.isolated_filesystem()
        _result = click_test.invoke(
            cli, ["init", "--debug", "--using", _out_cli_config]
        )
    assert _result.exit_code == 0
    assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
def test_init_from_env(
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    pySimpleModel,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)

    _out_config = os.path.normpath(os.path.join(os.getcwd(), fdp_com.USER_CONFIG_FILE))

    tempd = tmp_path.__str__()
    _out_cli_config = os.path.join(tempd, "cli-config.yaml")
    _env = os.environ.copy()
    _env["FAIR_REGISTRY_DIR"] = local_registry._install
    with local_registry:
        _result = click_test.invoke(
            cli,
            [
                "init",
                "--debug",
                "--ci",
                "--config",
                _out_config,
                "--export",
                _out_cli_config,
            ],
            env=_env,
        )
        assert _result.exit_code == 0
        assert os.path.exists(_out_cli_config)
        assert os.path.exists(_out_config)
        assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
        click_test = click.testing.CliRunner()
        click_test.isolated_filesystem()
        _result = click_test.invoke(
            cli, ["init", "--debug", "--using", _out_cli_config]
        )
    assert _result.exit_code == 0
    assert os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
def test_init_full(
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    pySimpleModel,
):
    mocker.patch("fair.registry.server.update_registry_post_setup", lambda *args: None)
    mocker.patch(
        "fair.registry.requests.get_remote_user",
        lambda *args, **kwargs: "FAIRDataPipeline",
    )
    with local_registry:
        mocker.patch("fair.common.USER_FAIR_DIR", pySimpleModel)
        _dummy_name = "Joseph Bloggs"
        _dummy_email = "jbloggs@nowhere.com"
        _args = [
            "8007",  # port
            "",  # remote api url
            "",  # remote data url
            "0123456789012345678901234567890123456789",  # remote token
            "",  # default data store
            _dummy_email,  # email
            "NONE",  # id system
            _dummy_name,  # Full name
            "",  # Output namespace
            "",  # Input namespace
            pySimpleModel,  # Repo
            "",
        ]
        monkeypatch.chdir(pySimpleModel)
        print(os.getcwd())
        click_test.invoke(
            cli,
            ["init", "--debug", "--registry", local_registry._install],
            input="\n".join(_args),
        )
        assert os.path.exists(fair.common.global_config_dir())
        assert os.path.exists(os.path.join(pySimpleModel, fair.common.FAIR_FOLDER))
        _cli_cfg = yaml.safe_load(
            open(
                os.path.join(pySimpleModel, fair.common.FAIR_FOLDER, "cli-config.yaml")
            )
        )
        _cli_glob_cfg = yaml.safe_load(
            open(os.path.join(fair.common.global_config_dir(), "cli-config.yaml"))
        )
        _expected_url = fdp_com.DEFAULT_LOCAL_REGISTRY_URL.replace(":8000", ":8007")
        assert _cli_cfg
        assert _cli_cfg["git"]["local_repo"] == pySimpleModel
        assert _cli_cfg["git"]["remote"] == "origin"
        assert (
            _cli_cfg["git"]["remote_repo"]
            == "https://github.com/FAIRDataPipeline/pySimpleModel.git"
        )
        assert _cli_cfg["namespaces"]["input"] == "josephbloggs"
        assert _cli_cfg["namespaces"]["output"] == "josephbloggs"
        assert _cli_cfg["registries"]["origin"]["data_store"] == urljoin(
            fair.common.DEFAULT_REGISTRY_DOMAIN, "data/"
        )
        assert _cli_cfg["registries"]["origin"]["uri"] == urljoin(
            fair.common.DEFAULT_REGISTRY_DOMAIN, "api/"
        )
        assert _cli_glob_cfg["registries"]["local"]["uri"] == _expected_url
        assert _cli_cfg["user"]["email"] == _dummy_email
        assert _cli_cfg["user"]["family_name"] == "Bloggs"
        assert _cli_cfg["user"]["given_names"] == "Joseph"
        assert _cli_cfg["user"]["uuid"]


@pytest.mark.faircli_cli
def test_init_local(
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    pySimpleModel,
):
    with local_registry:
        mocker.patch(
            "fair.registry.server.update_registry_post_setup", lambda *args: None
        )
        mocker.patch("fair.common.USER_FAIR_DIR", pySimpleModel)
        _dummy_name = "Joseph Bloggs"
        _dummy_email = "jbloggs@nowhere.com"
        _args = [
            "8000",
            "",
            _dummy_email,
            "NONE",
            _dummy_name,
            "",
            "",
            pySimpleModel,
            "",
        ]
        monkeypatch.chdir(pySimpleModel)
        print(os.getcwd())
        click_test.invoke(
            cli,
            ["init", "--debug", "--local", "--registry", local_registry._install],
            input="\n".join(_args),
        )
        assert os.path.exists(fair.common.global_config_dir())
        assert os.path.exists(os.path.join(pySimpleModel, fair.common.FAIR_FOLDER))
        _cli_cfg = yaml.safe_load(
            open(
                os.path.join(pySimpleModel, fair.common.FAIR_FOLDER, "cli-config.yaml")
            )
        )
        _cli_glob_cfg = yaml.safe_load(
            open(os.path.join(fair.common.global_config_dir(), "cli-config.yaml"))
        )
        _expected_url = "http://127.0.0.1:8000/api/"
        assert _cli_cfg
        assert _cli_cfg["git"]["local_repo"] == pySimpleModel
        assert _cli_cfg["git"]["remote"] == "origin"
        assert (
            _cli_cfg["git"]["remote_repo"]
            == "https://github.com/FAIRDataPipeline/pySimpleModel.git"
        )
        assert _cli_cfg["namespaces"]["input"] == "josephbloggs"
        assert _cli_cfg["namespaces"]["output"] == "josephbloggs"
        if platform.system() == "Windows":
            assert _cli_cfg["registries"]["origin"]["data_store"] == ".\\data_store\\"
        else:
            assert _cli_cfg["registries"]["origin"]["data_store"] == "./data_store/"
        assert _cli_cfg["registries"]["origin"]["uri"] == _expected_url
        assert _cli_glob_cfg["registries"]["local"]["uri"] == _expected_url
        assert _cli_cfg["user"]["email"] == _dummy_email
        assert _cli_cfg["user"]["family_name"] == "Bloggs"
        assert _cli_cfg["user"]["given_names"] == "Joseph"
        assert _cli_cfg["user"]["remote_user"] == "FAIRDataPipeline"
        assert _cli_cfg["user"]["uuid"]


@pytest.mark.faircli_cli
def test_purge(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
):
    if sys.version_info.major == 3 and sys.version_info.minor == 7:
        pytest.skip("Python3.7 issues with shutil.rmtree in local_config fixture")
    mocker.patch("fair.common.global_config_dir", lambda *args: local_config[0])
    mocker.patch("fair.common.find_fair_root", lambda *args: local_config[1])
    assert os.path.exists(os.path.join(local_config[0], fdp_com.FAIR_FOLDER))
    assert os.path.exists(os.path.join(local_config[1], fdp_com.FAIR_FOLDER))

    _result = click_test.invoke(cli, ["purge", "--debug"], input="Y")
    assert _result.exit_code == 0
    assert os.path.exists(os.path.join(local_config[0], fdp_com.FAIR_FOLDER))
    assert not os.path.exists(os.path.join(local_config[1], fdp_com.FAIR_FOLDER))

    _result = click_test.invoke(cli, ["purge", "--debug", "--global"], input="Y")
    assert _result.exit_code == 0
    assert not os.path.exists(os.path.join(local_config[0], fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
def test_registry_cli(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    mocker.patch("fair.common.global_config_dir", lambda *args: local_config[0])
    tempd = tmp_path.__str__()
    _reg_dir = os.path.join(tempd, "registry")
    _result = click_test.invoke(
        cli, ["registry", "install", "--directory", _reg_dir, "--debug"]
    )
    assert _result.exit_code == 0
    _result = click_test.invoke(cli, ["registry", "start", "--debug"])
    print(_result.exit_code)
    assert _result.exit_code == 0
    _registry_status_result = click_test.invoke(cli, ["registry", "status", "--debug"])
    assert _registry_status_result.exit_code == 0
    assert (
        "Server running at: http://127.0.0.1:8000/api/"
        in _registry_status_result.output
    )
    assert requests.get(LOCAL_REGISTRY_URL).status_code == 200
    _result = click_test.invoke(cli, ["registry", "stop", "--debug"])
    assert _result.exit_code == 0
    _registry_status_result = click_test.invoke(cli, ["registry", "status", "--debug"])
    assert _registry_status_result.exit_code == 0
    assert "Server is not running" in _registry_status_result.output
    with pytest.raises(requests.ConnectionError):
        requests.get(LOCAL_REGISTRY_URL)
    _result = click_test.invoke(cli, ["registry", "uninstall", "--debug"], input="Y")
    assert _result.exit_code == 0
    if platform.system() == "Windows":
        tempd = Path(f"{tempd}")
    assert not glob.glob(os.path.join(tempd, "registry", "*"))


def test_cli_run(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
):
    with local_registry:
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        mocker.patch(
            "fair.registry.requests.local_token", lambda: local_registry._token
        )
        with open(
            os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "staging"), "w"
        ) as staged:
            yaml.dump({"job": {}}, staged)
        _result = click_test.invoke(
            cli,
            ["run", "--debug", "--dirty", "--script", 'echo "Hello World!"'],
        )
        assert _result.output

        assert _result.exit_code == 0


def test_cli_run_local(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
):
    with local_registry:
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        mocker.patch(
            "fair.registry.requests.local_token", lambda: local_registry._token
        )
        with open(
            os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "staging"), "w"
        ) as staged:
            yaml.dump({"job": {}}, staged)
        _result = click_test.invoke(
            cli,
            ["run", "--local", "--script", 'echo "Hello World!"'],
        )
        assert _result.output

        assert _result.exit_code == 0
