"""
CLI Tests
---------

As tests are provided for the CLI backend itself, the following only check that
all CLI commands run without any errors

"""

import glob
import inspect
import json
import os
import shutil
import sys
import traceback
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
import fair.exceptions as fdp_exc
import fair.registry.lifecycle
import fair.registry.server
import fair.session
import fair.staging
import fair.testing as fdp_test
import fair.user_config
from fair.cli import cli
from tests import conftest as conf

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"

# The local_config fixture mocks this; tests run from a subdirectory need the
# real search upwards for the .fair folder
_FIND_FAIR_ROOT = fdp_com.find_fair_root


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
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
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
            "n",  # not to keep the registry running on that port
            "",  # default data store
            "NONE",  # id system
            _dummy_name,  # Full name
            _dummy_email,  # email
            "",  # Output namespace
            "",  # Input namespace
            pySimpleModel,  # Repo
            "",  # git remote name
            "",  # remote api url (this repo)
            "",  # remote api token file
            "",  # Output namespace (this repo)
            "",  # Input namespace (this repo)
        ]
        monkeypatch.chdir(pySimpleModel)
        print(os.getcwd())
        click_test.invoke(
            cli,
            ["init", "--debug", "--registry", local_registry._install],
            input="\n".join(_args) + "\n",
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
            "NONE",
            _dummy_name,
            _dummy_email,
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
            input="\n".join(_args) + "\n",
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
@pytest.mark.parametrize("options", (["--ci"], []))
def test_init_keeps_existing_repository(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    options: typing.List[str],
):
    # The repository is left as it is, and the local registry is brought up
    # to date with it once
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    monkeypatch.chdir(local_config[1])
    _data_file = os.path.join(
        local_config[1], fdp_com.FAIR_FOLDER, "data_store", "pulled.csv"
    )
    os.makedirs(os.path.dirname(_data_file))
    with open(_data_file, "w") as data_f:
        data_f.write("a,b\n1,2\n")

    _result = click_test.invoke(cli, ["init", *options])
    assert _result.exit_code == 0
    assert "already initialised" in _result.output
    with open(_data_file) as data_f:
        assert data_f.read() == "a,b\n1,2\n"
    _update.assert_called_once()
    assert os.path.samefile(_update.call_args.args[0], local_config[1])


@pytest.mark.faircli_cli
def test_init_again_registers_user(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    # Whether or not the registry held the repository's user before, it does
    # after, and a second time changes nothing
    def _user_authors() -> typing.List[typing.Dict]:
        return requests.get(
            f"{LOCAL_REGISTRY_URL}/user_author/",
            headers={"Authorization": f"token {local_registry._token}"},
        ).json()["results"]

    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    monkeypatch.chdir(local_config[1])
    with local_registry:
        _result = click_test.invoke(cli, ["init"])
        assert _result.exit_code == 0, _result.output
        assert "registered in the local registry" in _result.output
        _registered = _user_authors()
        assert len(_registered) == 1
        _author = requests.get(_registered[0]["author"]).json()
        assert _author["name"] == "Interface Test"

        _result = click_test.invoke(cli, ["init"])
        assert _result.exit_code == 0, _result.output
        assert _user_authors() == _registered


@pytest.mark.faircli_cli
def test_init_in_subdirectory_names_existing_repository(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    monkeypatch: pytest.MonkeyPatch,
):
    _subdir = os.path.join(local_config[1], "subdir")
    os.makedirs(_subdir)
    monkeypatch.chdir(_subdir)

    _result = click_test.invoke(cli, ["init"], input="n\n")
    assert _result.exit_code == 0
    assert f"initialised for this location at '{local_config[1]}'" in (
        _result.output
    )
    assert "Aborted" in _result.output
    assert not os.path.exists(os.path.join(_subdir, fdp_com.FAIR_FOLDER))


# A FAIR directory of the test's own, holding no global configuration yet
@pytest.fixture
def fresh_fair_dir(tmp_path, mocker: pytest_mock.MockerFixture) -> str:
    _dir = os.path.join(tmp_path, "home", fdp_com.FAIR_FOLDER)
    mocker.patch("fair.common.USER_FAIR_DIR", _dir)
    return _dir


# A CLI configuration such as 'fair init --using' takes, for the repository in
# the working directory
def _cli_configuration(tmp_path) -> typing.Dict:
    return fdp_test.create_configurations(
        str(tmp_path), os.getcwd(), None, str(tmp_path), True
    )


# Write a CLI configuration for 'fair init --using', returning its file
def _using_file(tmp_path, config: typing.Dict) -> str:
    _file = os.path.join(tmp_path, "using.yaml")
    with open(_file, "w") as out_f:
        yaml.dump(config, out_f)
    return _file


# What the global configuration file holds, None where there is none
def _global_configuration() -> typing.Optional[bytes]:
    if not os.path.exists(fdp_com.global_fdpconfig()):
        return None
    with open(fdp_com.global_fdpconfig(), "rb") as in_f:
        return in_f.read()


# The staging file a new repository starts with
_EMPTY_STAGING = {"job": {}, "file": {}, "data_product": {}, "code_run": {}}


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "failure",
    [
        fdp_exc.RegistryError("The remote registry refused the token"),
        KeyboardInterrupt(),
        OSError("Permission denied"),
        click.exceptions.Abort(),
    ],
    ids=[
        "refused token",
        "interrupted",
        "unreadable file",
        "aborted at a question",
    ],
)
def test_init_that_fails_in_set_up_leaves_nothing(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    failure: BaseException,
):
    mocker.patch("fair.configuration.global_config_query", side_effect=failure)
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    _result = click_test.invoke(cli, ["init"])
    assert _result.exit_code != 0
    assert not os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
    assert not os.path.exists(fdp_com.global_config_dir())
    _update.assert_not_called()


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "failure",
    [
        fdp_exc.RegistryError("The remote registry refused the token"),
        KeyboardInterrupt(),
    ],
    ids=["refused token", "interrupted"],
)
def test_init_that_fails_keeps_the_global_configuration(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    failure: BaseException,
):
    os.makedirs(fdp_com.global_config_dir())
    _config = _cli_configuration(tmp_path)
    del _config["git"]
    with open(fdp_com.global_fdpconfig(), "w") as out_f:
        yaml.dump(_config, out_f)
    _before = _global_configuration()
    mocker.patch("fair.configuration.local_config_query", side_effect=failure)
    _result = click_test.invoke(cli, ["init"])
    assert _result.exit_code != 0
    assert not os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
    assert _global_configuration() == _before


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "remote_token", [None, "2" * 40], ids=["new", "replaced"]
)
def test_init_that_fails_puts_the_global_directory_back(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    remote_token: typing.Optional[str],
):
    # A token the user keeps there, and the remote's token, which the set-up
    # writes before it fails
    os.makedirs(fdp_com.global_config_dir())
    _kept = os.path.join(fdp_com.global_config_dir(), "token.txt")
    with open(_kept, "w") as out_f:
        out_f.write("0" * 40)
    _remote = os.path.join(fdp_com.global_config_dir(), "remotetoken.txt")
    if remote_token:
        with open(_remote, "w") as out_f:
            out_f.write(remote_token)

    def _set_up(*args, **kwargs):
        with open(_remote, "w") as out_f:
            out_f.write("1" * 40)
        raise click.exceptions.Abort()

    mocker.patch("fair.configuration.global_config_query", side_effect=_set_up)
    _result = click_test.invoke(cli, ["init"])
    assert _result.exit_code == 1
    assert not os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
    with open(_kept) as in_f:
        assert in_f.read() == "0" * 40
    if remote_token:
        with open(_remote) as in_f:
            assert in_f.read() == remote_token
    else:
        assert not os.path.exists(_remote)
    assert _global_configuration() is None


@pytest.mark.faircli_cli
def test_init_using_a_configuration_missing_a_key_leaves_nothing(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    _config = _cli_configuration(tmp_path)
    del _config["registries"]["local"]["directory"]
    _result = click_test.invoke(
        cli, ["init", "--using", _using_file(tmp_path, _config)]
    )
    assert _result.exit_code == 1
    assert "Expected key 'directory'" in _result.output
    assert not os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
    assert _global_configuration() is None
    _update.assert_not_called()


@pytest.mark.faircli_cli
def test_init_using_a_configuration_that_does_not_validate_leaves_nothing(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # '--using' writes the global configuration over the one there before
    os.makedirs(fdp_com.global_config_dir())
    _config = _cli_configuration(tmp_path)
    del _config["git"]
    with open(fdp_com.global_fdpconfig(), "w") as out_f:
        yaml.dump(_config, out_f)
    _before = _global_configuration()
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    _config = _cli_configuration(tmp_path)
    _config["user"]["email"] = "not an address"
    _result = click_test.invoke(
        cli, ["init", "--using", _using_file(tmp_path, _config)]
    )
    assert _result.exit_code == 1
    assert "validation" in _result.output
    assert not os.path.exists(os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER))
    assert _global_configuration() == _before
    _update.assert_not_called()


@pytest.mark.faircli_cli
@pytest.mark.parametrize("failure", ["refused token", "invalid configuration"])
def test_init_that_fails_in_a_subdirectory_leaves_the_repository(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    failure: str,
):
    mocker.patch("fair.common.find_fair_root", _FIND_FAIR_ROOT)
    mocker.patch("fair.registry.server.update_registry_post_setup")
    _subdir = os.path.join(local_config[1], "subdir")
    os.makedirs(_subdir)
    monkeypatch.chdir(_subdir)
    _repo_config = os.path.join(
        local_config[1], fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )
    with open(_repo_config) as in_f:
        _before = yaml.safe_load(in_f)
    _global_before = yaml.safe_load(_global_configuration())
    if failure == "refused token":
        mocker.patch(
            "fair.configuration.local_config_query",
            side_effect=fdp_exc.RegistryError(
                "The remote registry refused the token"
            ),
        )
    else:
        # The questions answered, and their configuration then refused
        _local = _cli_configuration(tmp_path)
        del _local["registries"]["local"]
        _local["user"]["email"] = "not an address"
        mocker.patch(
            "fair.configuration.local_config_query", return_value=_local
        )
    _result = click_test.invoke(cli, ["init"], input="y\n")
    assert _result.exit_code == 1
    assert not os.path.exists(os.path.join(_subdir, fdp_com.FAIR_FOLDER))
    with open(_repo_config) as in_f:
        assert yaml.safe_load(in_f) == _before
    assert yaml.safe_load(_global_configuration()) == _global_before


@pytest.mark.faircli_cli
def test_init_in_a_subdirectory_makes_its_own_staging_file(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    mocker.patch("fair.common.find_fair_root", _FIND_FAIR_ROOT)
    mocker.patch("fair.registry.server.update_registry_post_setup")
    _subdir = os.path.join(local_config[1], "subdir")
    os.makedirs(_subdir)
    monkeypatch.chdir(_subdir)
    _repo_staging = os.path.join(
        local_config[1], fdp_com.FAIR_FOLDER, "staging"
    )
    assert not os.path.exists(_repo_staging)
    _using = _using_file(tmp_path, _cli_configuration(tmp_path))
    _result = click_test.invoke(cli, ["init", "--using", _using], input="y\n")
    assert _result.exit_code == 0, _result.output
    with open(os.path.join(_subdir, fdp_com.FAIR_FOLDER, "staging")) as in_f:
        assert yaml.safe_load(in_f) == _EMPTY_STAGING
    assert not os.path.exists(_repo_staging)


@pytest.mark.faircli_cli
def test_init_that_fails_registering_its_user_is_completed_by_init_again(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    _update = mocker.patch(
        "fair.registry.server.update_registry_post_setup",
        side_effect=[
            fdp_exc.RegistryError("The local registry is not running"),
            None,
        ],
    )
    _using = _using_file(tmp_path, _cli_configuration(tmp_path))
    _result = click_test.invoke(cli, ["init", "--using", _using])
    assert _result.exit_code == 1
    _repo_config = os.path.join(
        os.getcwd(), fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )
    with open(_repo_config) as in_f:
        assert yaml.safe_load(in_f)["user"]["given_names"] == "Interface"

    _result = click_test.invoke(cli, ["init"])
    assert _result.exit_code == 0, _result.output
    assert "already initialised" in _result.output
    assert _update.call_count == 2


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "left",
    [
        {fdp_com.FAIR_CLI_CONFIG: {}, "staging": _EMPTY_STAGING},
        {fdp_com.FAIR_CLI_CONFIG: {}},
        {"staging": _EMPTY_STAGING},
    ],
    ids=["a failure", "a refused configuration", "a killed process"],
)
def test_init_begins_again_where_one_did_not_finish(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    left: typing.Dict[str, typing.Dict],
):
    mocker.patch("fair.registry.server.update_registry_post_setup")
    _fair_dir = os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER)
    os.makedirs(_fair_dir)
    for _name, _contents in left.items():
        with open(os.path.join(_fair_dir, _name), "w") as out_f:
            yaml.dump(_contents, out_f)
    _using = _using_file(tmp_path, _cli_configuration(tmp_path))
    _result = click_test.invoke(cli, ["init", "--using", _using])
    assert _result.exit_code == 0, _result.output
    assert "did not finish" in _result.output
    with open(os.path.join(_fair_dir, fdp_com.FAIR_CLI_CONFIG)) as in_f:
        assert yaml.safe_load(in_f)["user"]["given_names"] == "Interface"


@pytest.mark.faircli_cli
@pytest.mark.parametrize("more", ["data", "staged"])
def test_init_refuses_an_unfinished_repository_holding_more(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    more: str,
):
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    _fair_dir = os.path.join(os.getcwd(), fdp_com.FAIR_FOLDER)
    os.makedirs(_fair_dir)
    with open(os.path.join(_fair_dir, fdp_com.FAIR_CLI_CONFIG), "w") as out_f:
        yaml.dump({}, out_f)
    _staging = {**_EMPTY_STAGING, "job": {"a-job": more == "staged"}}
    with open(os.path.join(_fair_dir, "staging"), "w") as out_f:
        yaml.dump(_staging if more == "staged" else _EMPTY_STAGING, out_f)
    if more == "data":
        os.makedirs(os.path.join(_fair_dir, "data_store"))
    _before = sorted(os.listdir(_fair_dir))

    _using = _using_file(tmp_path, _cli_configuration(tmp_path))
    _result = click_test.invoke(cli, ["init", "--using", _using])
    assert _result.exit_code == 1
    assert f"'{_fair_dir}' holds no configuration" in _result.output
    assert sorted(os.listdir(_fair_dir)) == _before
    _update.assert_not_called()


@pytest.mark.faircli_cli
def test_init_replaces_an_empty_global_configuration(
    click_test: click.testing.CliRunner,
    fresh_fair_dir: str,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    os.makedirs(fdp_com.global_config_dir())
    with open(fdp_com.global_fdpconfig(), "w") as out_f:
        yaml.dump({}, out_f)
    _global = _cli_configuration(tmp_path)
    del _global["git"]
    _local = _cli_configuration(tmp_path)
    del _local["registries"]["local"]
    _query = mocker.patch(
        "fair.configuration.global_config_query", return_value=_global
    )
    mocker.patch("fair.configuration.local_config_query", return_value=_local)
    _update = mocker.patch("fair.registry.server.update_registry_post_setup")
    _result = click_test.invoke(cli, ["init"])
    assert _result.exit_code == 0, _result.output
    _query.assert_called_once()
    assert yaml.safe_load(_global_configuration()) == _global
    # A first set-up, in which the registry is given its file types
    assert _update.call_args.args[1]


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
    assert _result.exit_code == 0, _result.output + "".join(
        traceback.format_exception(*_result.exc_info)
    )
    assert not os.path.exists(os.path.join(local_config[0], fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
@pytest.mark.parametrize("options", [["--all"], ["--data"], []])
def test_purge_answered_no_removes_nothing(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    options: typing.List[str],
):
    mocker.patch(
        "fair.common.global_config_dir", lambda *args: local_config[0]
    )
    mocker.patch("fair.common.find_fair_root", lambda *args: local_config[1])
    _purge = mocker.spy(fair.session.FAIR, "purge")

    _result = click_test.invoke(cli, ["purge", *options], input="n\n")
    assert _result.exit_code == 0, _result.output
    assert "Are you sure" in _result.output
    _purge.assert_not_called()
    assert os.path.exists(os.path.join(local_config[1], fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
def test_purge_yes_asks_nothing(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch(
        "fair.common.global_config_dir", lambda *args: local_config[0]
    )
    mocker.patch("fair.common.find_fair_root", lambda *args: local_config[1])

    _result = click_test.invoke(cli, ["purge", "--yes"])
    assert _result.exit_code == 0, _result.output
    assert "Are you sure" not in _result.output
    assert not os.path.exists(
        os.path.join(local_config[1], fdp_com.FAIR_FOLDER)
    )
    assert os.path.exists(os.path.join(local_config[0], fdp_com.FAIR_FOLDER))


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "options, removes",
    [
        (
            ["--all"],
            {"global_cfg": False, "clear_data": False, "clear_all": True},
        ),
        (
            ["--data"],
            {"global_cfg": False, "clear_data": True, "clear_all": False},
        ),
    ],
)
def test_purge_yes_answers_every_question(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    options: typing.List[str],
    removes: typing.Dict[str, bool],
):
    # What would be removed is checked, not removed: with --all that is the
    # whole FAIR directory
    mocker.patch(
        "fair.common.global_config_dir", lambda *args: local_config[0]
    )
    mocker.patch("fair.common.find_fair_root", lambda *args: local_config[1])
    _purge = mocker.patch("fair.session.FAIR.purge")

    _result = click_test.invoke(cli, ["purge", "--yes", *options])
    assert _result.exit_code == 0, _result.output
    assert "Are you sure" not in _result.output
    _purge.assert_called_once_with(**removes)


@pytest.mark.faircli_cli
def test_remote_add(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
):
    monkeypatch.chdir(local_config[1])
    _lconfig_path = os.path.join(
        local_config[1], fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )
    _token_file = os.path.join(tmp_path, "token")
    with open(_token_file, "w") as token_f:
        token_f.write("0123456789012345678901234567890123456789")
    _url = "http://127.0.0.1:8002/api/"

    _result = click_test.invoke(
        cli, ["remote", "add", "other", _url, "--token", _token_file]
    )
    assert _result.exit_code == 0
    _registries = yaml.safe_load(open(_lconfig_path))["registries"]
    assert _registries["other"] == {"uri": _url, "token": _token_file}
    assert "origin" in _registries

    _result = click_test.invoke(
        cli, ["remote", "add", "another", _url, "--token", "no-such-file"]
    )
    assert _result.exit_code != 0
    assert "another" not in yaml.safe_load(open(_lconfig_path))["registries"]


@pytest.mark.faircli_cli
def test_remote_remove(
    local_config: typing.Tuple[str, str],
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    mocker.patch("fair.common.find_fair_root", _FIND_FAIR_ROOT)
    # From a subdirectory, the configuration must still be found and saved
    _subdir = os.path.join(local_config[1], "subdir")
    os.makedirs(_subdir)
    monkeypatch.chdir(_subdir)
    _lconfig_path = os.path.join(
        local_config[1], fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
    )

    _result = click_test.invoke(cli, ["remote", "remove", "origin", "--debug"])
    assert _result.exit_code == 0
    _registries = yaml.safe_load(open(_lconfig_path))["registries"]
    assert "origin" not in _registries
    assert "local" in _registries

    _result = click_test.invoke(cli, ["remote", "remove", "origin", "--debug"])
    assert _result.exit_code != 0


@pytest.mark.faircli_cli
def test_session_file_removed_from_subdirectory(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_config[0])
    mocker.patch("fair.registry.server.launch_server", lambda **kwargs: None)
    mocker.patch("fair.registry.lifecycle._server_here", return_value=None)
    mocker.patch("fair.common.find_fair_root", _FIND_FAIR_ROOT)
    _subdir = os.path.join(local_config[1], "subdir")
    os.makedirs(_subdir)

    with fair.session.FAIR(
        _subdir, server_mode=fair.registry.server.SwitchMode.CLI
    ):
        assert len(fair.registry.lifecycle._session_records(local_config[0])) == 1
    # A session left open would keep the registry from being stopped
    assert not fair.registry.lifecycle._session_records(local_config[0])


@pytest.mark.faircli_cli
@pytest.mark.parametrize("passive", [False, True])
def test_run_passive_does_not_execute(
    mocker: pytest_mock.MockerFixture, passive: bool
):
    # Only run()'s own control flow is under test, so the session is built
    # without __init__ and its collaborators are mocked
    _session = object.__new__(fair.session.FAIR)
    _session._allow_dirty = False
    _session._session_id = None
    _session._session_config = mocker.MagicMock()
    mocker.patch.object(_session, "_pre_job_setup")
    mocker.patch.object(_session, "check_git_repo_state")
    _breakdown = mocker.patch.object(_session, "_post_job_breakdown")

    _session.run(passive=passive)

    assert _session._session_config.execute.called is not passive
    _session._session_config.write.assert_called_once()
    _breakdown.assert_called_once_with(add_run=True)


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "error, shown, fails",
    [
        (fdp_exc.InternalError("a fault"), "InternalError: a fault", True),
        (
            fdp_exc.RegistryAPICallError("request forbidden", 403),
            "Error: [HTTP 403]: request forbidden",
            True,
        ),
        (fdp_exc.FAIRCLIException("a note", level="Warning"), "Warning: a note", False),
    ],
)
def test_what_a_calling_script_sees(
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    error: fdp_exc.FAIRCLIException,
    shown: str,
    fails: bool,
):
    # Anything but a warning is a failure, a registry's refusal included
    mocker.patch("fair.session.FAIR", side_effect=error)
    _result = click_test.invoke(cli, ["status"])
    assert (_result.exit_code != 0) is fails, _result.output
    assert shown in _result.output


@pytest.mark.faircli_cli
@pytest.mark.parametrize(
    "token, answer, expected",
    [
        ("a-token", "admin", "admin"),
        ("a-token", fdp_exc.UnexpectedRegistryServerState("down"), "FAIRDataPipeline"),
        (None, AssertionError("the remote was asked"), "FAIRDataPipeline"),
    ],
    ids=["answers", "unreachable", "no token"],
)
def test_ci_remote_user(
    tmp_path, mocker: pytest_mock.MockerFixture, token, answer, expected
):
    _config = fdp_test.create_configurations(
        str(tmp_path), str(tmp_path), str(tmp_path), str(tmp_path)
    )
    if token:
        with open(_config["registries"]["origin"]["token"], "w") as token_f:
            token_f.write(f"{token}\n")
    _asked = mocker.patch(
        "fair.registry.requests.get_remote_user",
        side_effect=answer if isinstance(answer, Exception) else None,
        return_value=None if isinstance(answer, Exception) else answer,
    )
    assert fair.session.FAIR._ci_remote_user(_config) == expected
    if token:
        assert _asked.call_args.args == (
            _config["registries"]["origin"]["uri"],
            token,
        )


# A git remote and what was pushed to it: a branch, an annotated tag, and a
# pull request's merge commit under refs/pull/1/merge, as GitHub keeps one.
# A commit may be fetched by its hash, as from GitHub
@pytest.fixture
def git_remote(tmp_path, monkeypatch) -> typing.Dict[str, str]:
    for _key, _value in (
        ("GIT_AUTHOR_NAME", "Test"),
        ("GIT_COMMITTER_NAME", "Test"),
        ("GIT_AUTHOR_EMAIL", "test@noreply"),
        ("GIT_COMMITTER_EMAIL", "test@noreply"),
    ):
        monkeypatch.setenv(_key, _value)
    _bare = git.Repo.init(os.path.join(tmp_path, "remote.git"), bare=True)
    _bare.git.config("uploadpack.allowAnySHA1InWant", "true")
    # Its default branch is the one pushed, whatever git's own default is
    _bare.git.symbolic_ref("HEAD", "refs/heads/main")
    _url = Path(_bare.git_dir).as_uri()
    _work = git.Repo.init(os.path.join(tmp_path, "work"))
    _work.git.checkout("-b", "main")
    _work.create_remote("origin", _url)
    for _name in ("a", "b"):
        with open(os.path.join(_work.working_dir, _name), "w") as out_f:
            out_f.write(_name)
        _work.git.add(_name)
        _work.git.commit("-m", _name)
    _work.git.tag("-a", "v1", "-m", "v1")
    _work.git.push("origin", "main", "v1")
    _main = _work.head.commit.hexsha
    _work.git.checkout("-b", "feature")
    with open(os.path.join(_work.working_dir, "c"), "w") as out_f:
        out_f.write("c")
    _work.git.add("c")
    _work.git.commit("-m", "c")
    _work.git.push("origin", "feature")
    _work.git.checkout("main")
    _work.git.merge("--no-ff", "-m", "Merge pull request", "feature")
    _work.git.push("origin", "HEAD:refs/pull/1/merge")
    _merge = _work.head.commit.hexsha
    return {"url": _url, "main": _main, "merge": _merge, "dir": str(tmp_path)}


# A session that checks the git repository at a location, built without
# __init__ since only the check is under test
def _session_at(location: str) -> fair.session.FAIR:
    _session = object.__new__(fair.session.FAIR)
    _session._session_loc = location
    return _session


def _clone(remote: typing.Dict[str, str], name: str) -> git.Repo:
    return git.Repo.clone_from(remote["url"], os.path.join(remote["dir"], name))


# What actions/checkout does for a pull request or a tag: one commit fetched
# by its hash, depth 1, into a ref of its own, then checked out
def _ci_checkout(
    remote: typing.Dict[str, str], name: str, commit: str, ref: str
) -> git.Repo:
    _repo = git.Repo.init(os.path.join(remote["dir"], name))
    _repo.create_remote("origin", remote["url"])
    _repo.git.fetch("--no-tags", "--depth=1", "origin", f"+{commit}:{ref}")
    _repo.git.checkout("--force", ref)
    return _repo


@pytest.mark.faircli_cli
def test_git_check_on_a_branch(git_remote: typing.Dict[str, str]):
    _repo = _clone(git_remote, "clone")
    _session_at(_repo.working_dir).check_git_repo_state()

    with open(os.path.join(_repo.working_dir, "d"), "w") as out_f:
        out_f.write("d")
    _repo.git.add("d")
    _repo.git.commit("-m", "d")
    with pytest.raises(fdp_exc.FDPRepositoryError, match="not level with remote"):
        _session_at(_repo.working_dir).check_git_repo_state()


@pytest.mark.faircli_cli
@pytest.mark.parametrize("target", ["v1", "main~1"])
def test_git_check_detached_at_a_pushed_commit(
    git_remote: typing.Dict[str, str], target: str
):
    _repo = _clone(git_remote, "clone")
    _repo.git.checkout(target)
    assert _repo.head.is_detached
    # Its remote-tracking branches say so: the remote is not asked
    _repo.remotes.origin.set_url(os.path.join(git_remote["dir"], "nowhere"))
    _session_at(_repo.working_dir).check_git_repo_state()


@pytest.mark.faircli_cli
def test_git_check_detached_at_a_local_commit(
    git_remote: typing.Dict[str, str], capsys
):
    _repo = _clone(git_remote, "clone")
    with open(os.path.join(_repo.working_dir, "d"), "w") as out_f:
        out_f.write("d")
    _repo.git.add("d")
    _repo.git.commit("-m", "d")
    _repo.git.checkout("--detach")
    with pytest.raises(fdp_exc.FDPRepositoryError, match="on no branch or tag"):
        _session_at(_repo.working_dir).check_git_repo_state()
    _session_at(_repo.working_dir).check_git_repo_state(allow_dirty=True)
    assert "Warning: commit" in capsys.readouterr().out


@pytest.mark.faircli_cli
@pytest.mark.parametrize("checkout", ["pull_request", "tag"])
def test_git_check_detached_as_ci_checks_out(
    git_remote: typing.Dict[str, str], checkout: str
):
    # Neither leaves a branch of the remote to look at, so the remote is asked
    if checkout == "pull_request":
        _commit, _ref = git_remote["merge"], "refs/remotes/pull/1/merge"
    else:
        _commit, _ref = git_remote["main"], "refs/tags/v1"
    _repo = _ci_checkout(git_remote, checkout, _commit, _ref)
    assert _repo.head.is_detached
    _session_at(_repo.working_dir).check_git_repo_state()

    _repo.remotes.origin.set_url(os.path.join(git_remote["dir"], "nowhere"))
    with pytest.raises(fdp_exc.FDPRepositoryError, match="could not be asked"):
        _session_at(_repo.working_dir).check_git_repo_state()


@pytest.mark.faircli_cli
def test_git_branch_variable_on_a_detached_checkout(
    git_remote: typing.Dict[str, str], mocker: pytest_mock.MockerFixture
):
    _repo = _clone(git_remote, "clone")
    _config = fair.user_config.JobConfiguration()
    _config["run_metadata.local_repo"] = _repo.working_dir
    mocker.patch("fair.configuration.local_git_repo", lambda *args: _repo.working_dir)
    assert _config.git_branch == "main"
    _repo.git.checkout("v1")
    with pytest.raises(fdp_exc.UserConfigError, match="on no branch"):
        _config.git_branch


@pytest.mark.faircli_cli
def test_run_rehearsal_runs_checks_and_stages_nothing(
    mocker: pytest_mock.MockerFixture,
):
    # As above: run()'s own control flow alone
    _session = object.__new__(fair.session.FAIR)
    _session._allow_dirty = False
    _session._session_id = None
    _session._session_config = mocker.MagicMock()
    mocker.patch.object(_session, "_pre_job_setup")
    _check = mocker.patch.object(_session, "check_git_repo_state")
    _breakdown = mocker.patch.object(_session, "_post_job_breakdown")

    _session.run(rehearse=True)

    # The commit is recorded as it is, uncommitted changes and all
    assert _session._session_config.prepare.call_args.kwargs["allow_dirty"]
    _check.assert_not_called()
    _session._session_config.write.assert_called_once()
    _session._session_config.execute.assert_not_called()
    _breakdown.assert_not_called()
    _session._session_config.close_log.assert_called_once()


# A command line runner that keeps what a command writes to stderr apart from
# its stdout, which click before 8.2 does only when asked
def _runner() -> click.testing.CliRunner:
    if "mix_stderr" in inspect.signature(click.testing.CliRunner).parameters:
        return click.testing.CliRunner(mix_stderr=False)
    return click.testing.CliRunner()


@pytest.mark.faircli_cli
def test_cli_run_rehearse(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    click_test: click.testing.CliRunner,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    _staging = os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "staging")
    _ran = os.path.join(tmp_path, "ran")
    with local_registry:
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        mocker.patch(
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
        )
        with open(_staging, "w") as staged:
            yaml.dump({"job": {}, "data_product": {}, "code_run": {}}, staged)
        _result = _runner().invoke(
            cli, ["run", "--rehearse", "--script", f'touch "{_ran}"']
        )
    assert _result.exit_code == 0, _result.output
    _job = json.loads(_result.stdout)
    assert sorted(_job) == ["config", "data_store", "job_dir", "script", "token_file"]
    assert os.path.dirname(_job["config"]) == _job["job_dir"]
    assert os.path.dirname(_job["script"]) == _job["job_dir"]
    assert os.path.isfile(_job["config"])
    assert os.path.isfile(_job["script"])
    assert _job["token_file"] == os.path.join(local_registry._install, "token")
    assert not os.path.exists(_ran)
    with open(_staging) as staged:
        assert not yaml.safe_load(staged)["job"]


@pytest.mark.faircli_cli
def test_cli_run_rehearse_with_ci_is_refused(click_test: click.testing.CliRunner):
    _result = _runner().invoke(cli, ["run", "--rehearse", "--ci"])
    assert _result.exit_code == 2
    assert not _result.stdout
    assert "cannot be given together" in _result.stderr


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
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
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
            "fair.registry.requests.local_token",
            lambda *args: local_registry._token,
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
