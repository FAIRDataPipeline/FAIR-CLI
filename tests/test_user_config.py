import datetime
import io
import os.path
import sys
import typing

import git
import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc
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


@pytest.mark.faircli_user_config
def test_subst_formatted_datetime():
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {},
        "write": [
            {
                "data_product": "out/${{DATETIME-%Y%m%d}}",
                "description": "at ${{ DATETIME-%H%M }}",
            }
        ],
    }
    _config._subst_cli_vars(datetime.datetime(2026, 9, 22, 14, 5))
    assert _config["write"][0]["data_product"] == "out/20260922"
    assert _config["write"][0]["description"] == "at 1405"


@pytest.mark.faircli_user_config
def test_run_id_left_for_api():
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {},
        "write": [{"data_product": "out/run-${{RUN_ID}}"}],
    }
    # RUN_ID is filled in by the language API at finalise, so it must reach
    # the working config unsubstituted
    _config._subst_cli_vars(datetime.datetime(2026, 9, 22))
    _config._check_for_unparsed()
    assert _config["write"][0]["data_product"] == "out/run-${{RUN_ID}}"

    _config["write"][0]["data_product"] = "out/run-${{NOT_A_VARIABLE}}"
    with pytest.raises(fdp_exc.InternalError):
        _config._check_for_unparsed()


@pytest.mark.faircli_user_config
def test_execute_uses_run_metadata_shell(mocker: pytest_mock.MockerFixture):
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": os.getcwd(),
            "script_path": "script.py",
            "shell": "python3",
        }
    }
    _config.env = {"PATH": ""}
    _config._log_file = io.StringIO()
    mocker.patch(
        "fair.configuration.get_current_user_name", lambda *args: [""]
    )
    mocker.patch("fair.configuration.get_current_user_email", lambda *args: "")
    _popen = mocker.patch("subprocess.Popen")
    _popen.return_value.stdout.readline.return_value = ""
    _popen.return_value.returncode = 0

    _config.execute()
    assert _popen.call_args.args[0] == ["python3", "script.py"]


@pytest.mark.faircli_user_config
def test_execute_output_unencodable(
    mocker: pytest_mock.MockerFixture, monkeypatch: pytest.MonkeyPatch
):
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {"local_repo": os.getcwd(), "script_path": "script.sh"}
    }
    _config.env = {"PATH": ""}
    _config._log_file = io.StringIO()
    mocker.patch(
        "fair.configuration.get_current_user_name", lambda *args: [""]
    )
    mocker.patch("fair.configuration.get_current_user_email", lambda *args: "")
    _popen = mocker.patch("subprocess.Popen")
    _popen.return_value.stdout.readline.side_effect = [
        "Progress \u2305 done\n",
        "",
    ]
    _popen.return_value.returncode = 0
    # A Windows console redirected to a pipe or file, as on a CI runner
    _stdout = io.TextIOWrapper(io.BytesIO(), encoding="cp1252")
    monkeypatch.setattr(sys, "stdout", _stdout)

    _config.execute()
    _stdout.flush()
    assert _stdout.buffer.getvalue() == b"Progress ? done\n"
    assert "\u2305" in _config._log_file.getvalue()


@pytest.mark.faircli_user_config
def test_subst_git_tag(mocker: pytest_mock.MockerFixture, tmp_path):
    _repo = git.Repo.init(tmp_path)
    mocker.patch(
        "fair.configuration.local_git_repo", lambda *args: str(tmp_path)
    )

    def _git_tag() -> str:
        _config = fdp_user.JobConfiguration()
        _config._config = {
            "run_metadata": {"local_repo": str(tmp_path)},
            "write": [{"data_product": "${{GIT_TAG}}"}],
        }
        _config._subst_cli_vars(datetime.datetime(2026, 9, 22))
        return _config["write"][0]["data_product"]

    def _commit(message: str) -> git.Commit:
        _actor = git.Actor("Test", "test@noreply.com")
        return _repo.index.commit(message, author=_actor, committer=_actor)

    with pytest.raises(fdp_exc.UserConfigError, match="no git tags found"):
        _git_tag()
    _first = _commit("first")
    with pytest.raises(fdp_exc.UserConfigError, match="no git tags found"):
        _git_tag()

    # Tags sort by name as v0.10.0 < v0.9.9, so this checks history is used
    _repo.create_tag("v0.9.9")
    _commit("second")
    _repo.create_tag("v0.10.0")
    assert _git_tag() == "v0.10.0"
    _commit("third")
    assert _git_tag() == "v0.10.0"

    _repo.head.reference = _first
    assert _git_tag() == "v0.9.9"


@pytest.mark.faircli_user_config
def test_update_from_fair_without_git_remote(
    local_config: typing.Tuple[str, str],
):
    """A missing git remote is reported, not raised as a bare IndexError"""
    _project = os.path.join(local_config[1], "project")
    _repo = git.Repo(_project)
    _repo.delete_remote(_repo.remotes["origin"])

    _cfg_path = os.path.join(local_config[1], "no_remote.yaml")
    yaml.dump({"run_metadata": {"description": "no remote"}}, open(_cfg_path, "w"))

    with pytest.raises(fdp_exc.FDPRepositoryError, match="has no remote 'origin'"):
        fdp_user.JobConfiguration(_cfg_path).update_from_fair(_project)

    # Given explicitly, the git remote is never consulted
    yaml.dump(
        {"run_metadata": {"description": "n", "remote_repo": "https://x/y.git"}},
        open(_cfg_path, "w"),
    )
    _config = fdp_user.JobConfiguration(_cfg_path)
    _config.update_from_fair(_project)
    assert _config["run_metadata.remote_repo"] == "https://x/y.git"
