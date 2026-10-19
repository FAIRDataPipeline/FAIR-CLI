import datetime
import fnmatch
import io
import os.path
import platform
import re
import shutil
import sys
import typing

import git
import pydantic
import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.user_config as fdp_user
import fair.user_config.globbing as fdp_glob
import fair.user_config.validation as fdp_valid

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
def test_preparation_of_a_register_entry_never_pulled(
    mocker: pytest_mock.MockerFixture,
    make_config: fdp_user.JobConfiguration,
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
):
    # A run reads what 'fair pull' registered for its 'register' block; with
    # nothing registered under the name, it is refused before the model is
    # started. The name is one nothing in these tests registers
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    for _entry in make_config["register"]:
        if "external_object" in _entry:
            _entry["external_object"] = "never/pulled"
    with local_registry:
        os.makedirs(os.path.join(local_config[1], fdp_com.FAIR_FOLDER, "logs"))
        with pytest.raises(fdp_exc.UserConfigError, match="'never/pulled'"):
            make_config.prepare(fdp_com.CMD_MODE.RUN, True)


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
        # What the 'register' block names is read from the local registry.
        # The registry's example data holds it, as 'fair pull' would leave it
        _manage = os.path.join(local_registry._install, "manage.py")
        local_registry._venv.run(
            "python", f"{_manage}", "add_example_data", capture=True
        )
        make_config.prepare(fdp_com.CMD_MODE.RUN, True)

        _out_dir = os.path.join(conf.TEST_OUT_DIR, "test_preparation")
        os.makedirs(_out_dir, exist_ok=True)

        make_config.write(os.path.join(_out_dir, "out.yaml"))


# The namespace holding the data products a pattern matches in a registry: a
# pattern stands for names in one namespace
def _namespace_of_matches(registry: conf.RegistryTest, pattern: str) -> str:
    _matches = [
        product
        for product in fdp_req.get(
            registry._url,
            "data_product",
            registry._token,
            params={"name": pattern},
        )
        if fdp_glob.matches_wildcard(pattern, product["name"])
    ]
    return fdp_req.url_get(_matches[0]["namespace"], registry._token)["name"]


# The entries a pattern was expanded to, its own entry, which must be there
# once, left out
def _without_pattern(
    block: typing.List[typing.Dict], pattern: str
) -> typing.List[typing.Dict]:
    assert [entry["data_product"] for entry in block].count(pattern) == 1
    return [entry for entry in block if entry["data_product"] != pattern]


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

        _, _path, _ = _example_entries[0]

        _split_key = _path.split("/")[-1]

        # Each '*' matches one segment of a name; the example data products
        # have several names two segments below this one, in one namespace
        _wildcard_path = _path.split(_split_key)[0] + "*/*"
        _namespace = _namespace_of_matches(local_registry, _wildcard_path)

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
        _matches = _without_pattern(_config["read"], _wildcard_path)
        assert len(_matches) > 1
        assert all(
            fdp_glob.matches_wildcard(_wildcard_path, entry["data_product"])
            and entry["use"]["namespace"] == _namespace
            for entry in _matches
        )

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

        _, _path, _ = _example_entries[0]

        _split_key = _path.split("/")[-1]

        # Each '*' matches one segment of a name; the example data products
        # have several names two segments below this one, in one namespace
        _wildcard_path = _path.split(_split_key)[0] + "*/*"
        _namespace = _namespace_of_matches(remote_registry, _wildcard_path)

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
        _matches = _without_pattern(_config["read"], _wildcard_path)
        assert len(_matches) > 1
        assert all(
            fdp_glob.matches_wildcard(_wildcard_path, entry["data_product"])
            and entry["use"]["namespace"] == _namespace
            for entry in _matches
        )
        # 'fair pull' fetches the matches, and nothing for the pattern's entry
        assert len(_config.get_readables()) == len(_matches)

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
def test_subst_windows_config_dir():
    _job_dir = "C:\\Users\\runner\\.fair\\data\\jobs\\2026-09-27_14_00"
    _scripts = [
        'gradle run --args "${{CONFIG_DIR}}"\n',  # as javaSimpleModel's
        "java -jar model.jar ${{CONFIG_DIR}}",
        'echo \'a\' "${{CONFIG_DIR}}"\n\tdone',  # dumped double-quoted
    ]
    for _script in _scripts:
        _config = fdp_user.JobConfiguration()
        _config._config = {"run_metadata": {"script": _script}}
        _config._job_dir = _job_dir
        _config._subst_cli_vars(datetime.datetime(2026, 9, 27))
        assert _config["run_metadata.script"] == _script.replace(
            "${{CONFIG_DIR}}", _job_dir + os.path.sep
        )

    # A value that is only a variable keeps the type it was substituted as
    _config = fdp_user.JobConfiguration()
    _config._config = {"run_metadata": {"description": "${{DATE}}"}}
    _config._subst_cli_vars(datetime.datetime(2026, 9, 27))
    assert _config["run_metadata.description"] == "20260927"


@pytest.mark.faircli_user_config
def test_register_entry_with_no_file():
    # A source with no file is given no 'public', since there is no file for
    # it to describe, and refused one written; and nothing of 'no_file'
    # reaches the working config, whose read of it is like any other
    _entry = {
        "external_object": "deposit/whole",
        "namespace_name": "ECMWF",
        "identifier": "https://doi.org/10.24381/cds.f17050d7",
        "title": "A deposit of files",
        "release_date": "2021-09-20T12:00",
        "primary": True,
        "version": "1.0.0",
        "no_file": True,
    }
    _config = fdp_user.JobConfiguration()
    _config._config = {"run_metadata": {}}
    _filled = _config._fill_block_item("register", _entry)
    assert "public" not in _filled
    with pytest.raises(fdp_exc.UserConfigError, match="Unexpected key 'public'"):
        _config._fill_block_item("register", dict(_entry, public=True))
    _with_file = {
        k: v for k, v in _entry.items() if k != "no_file"
    } | {"root": "https://example.com/", "path": "a.nc", "file_type": "nc"}
    assert "public" in _config._fill_block_item("register", _with_file)

    _config._config["read"] = _config._register_to_read([_filled])
    _read = _config._clean()["read"]
    assert [_entry["data_product"] for _entry in _read] == ["deposit/whole"]
    assert set(_read[0]) <= {"data_product", "description", "use", "registered"}


@pytest.mark.faircli_user_config
def test_register_version_token_left_for_pull(
    mocker: pytest_mock.MockerFixture,
):
    # A 'register' entry's token is resolved once its file is known, not here;
    # a version given outright is looked up as before
    mocker.patch(
        "fair.registry.requests.get",
        side_effect=AssertionError("the registry was asked"),
    )
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {},
        "register": [
            {
                "data_product": "a/b",
                "use": {
                    "data_product": "a/b",
                    "namespace": "PSU",
                    "version": "${{PATCH}}",
                },
            }
        ],
    }
    _filled = _config._fill_versions("register")
    assert _filled[0]["use"]["version"] == "${{PATCH}}"


@pytest.mark.faircli_user_config
def test_resolve_registered_reads(mocker: pytest_mock.MockerFixture):
    # A read made from a 'register' entry is of what the local registry holds:
    # a token the highest version, a version given outright one that is there;
    # anything else is refused, and other reads are left as they are
    _held = {"a/b": ["0.0.1", "0.0.2"]}

    def mock_get(uri, obj, token, params=None):
        return [
            {"version": v}
            for v in _held.get(params["name"], [])
            if params.get("version", v) == v
        ]

    mocker.patch("fair.registry.requests.local_token", return_value="")
    mocker.patch("fair.register.convert_key_value_to_id", return_value=3)
    mocker.patch("fair.registry.requests.get", mock_get)
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_data_registry_url": "http://127.0.0.1:8000/api/"
        }
    }

    def _read(name, version, registered=True):
        _entry = {
            "data_product": name,
            "use": {
                "data_product": name,
                "namespace": "PSU",
                "version": version,
            },
        }
        if registered:
            _entry["registered"] = True
        return _entry

    _other = _read("c/d", "${{ LATEST }}", registered=False)
    _resolved = _config._resolve_registered_reads(
        [_read("a/b", "${{ LATEST }}"), _read("a/b", "0.0.1"), _other]
    )
    assert [r["use"]["version"] for r in _resolved] == [
        "0.0.2",
        "0.0.1",
        "${{ LATEST }}",
    ]
    for _missing in (_read("a/b", "0.0.3"), _read("x/y", "${{ LATEST }}")):
        with pytest.raises(fdp_exc.UserConfigError, match="was not found"):
            _config._resolve_registered_reads([_missing])


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
@pytest.mark.parametrize(
    "shell, command",
    [
        ("julia --project=@.", ["julia", "--project=@.", "script"]),
        ("bash -x", ["bash", "-x", "-eo", "pipefail", "script"]),
        ("pwsh -NoProfile", ["pwsh", "-NoProfile", "-command", ". 'script'"]),
        (
            "R  --vanilla --quiet ",
            ["R", "--vanilla", "--quiet", "-f", "script"],
        ),
        ("python3 -X{0}", ["python3", "-X{0}", "script"]),
    ],
    ids=["julia", "bash", "pwsh", "R", "brace"],
)
def test_execute_puts_a_shells_flags_after_its_program(
    shell: str, command: typing.List[str], mocker: pytest_mock.MockerFixture
):
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": os.getcwd(),
            "script_path": "script",
            "shell": shell,
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
    assert _popen.call_args.args[0] == command


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "shell, said",
    [
        ("jula", "Unknown shell 'jula'"),
        ("", "Unknown shell ''"),
        ("batch -x", "The shell 'batch' takes no flags"),
    ],
    ids=["unknown", "empty", "batch"],
)
def test_a_shell_that_cannot_run_the_script_is_refused(
    shell: str, said: str, tmp_path
):
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": str(tmp_path),
            "script": "echo",
            "shell": shell,
        }
    }
    _config._job_dir = str(tmp_path)
    with pytest.raises(fdp_exc.UserConfigError, match=re.escape(said)):
        _config.setup_job_script()
    assert not os.listdir(tmp_path)


@pytest.mark.faircli_user_config
def test_a_script_takes_its_extension_from_the_shells_name(tmp_path):
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": str(tmp_path),
            "script": 'println("fair-ran-it")',
            "shell": "julia --project=@.",
        }
    }
    _config._job_dir = str(tmp_path)
    _job = _config.setup_job_script()
    assert _job["script"] == os.path.join(tmp_path, "script.jl")
    assert _config.shell == "julia --project=@."


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "shell, valid",
    [
        ("julia", True),
        ("julia --project=@.", True),
        ("jula --project=@.", False),
        ("--project=@. julia", False),
        ("", False),
    ],
)
def test_the_schema_takes_a_shell_with_flags(shell: str, valid: bool):
    _metadata = {
        "local_repo": os.getcwd(),
        "latest_commit": "0" * 40,
        "remote_repo": "https://github.com/FAIRDataPipeline/FAIR-CLI",
        "local_data_registry_url": "http://127.0.0.1:8000/api/",
        "remote_data_registry_url": "http://127.0.0.1:8001/api/",
        "default_input_namespace": "testing",
        "default_output_namespace": "testing",
        "write_data_store": os.getcwd(),
        "shell": shell,
    }
    if valid:
        assert fdp_valid.RunMetadata(**_metadata).shell == shell
    else:
        with pytest.raises(pydantic.ValidationError, match="must start with"):
            fdp_valid.RunMetadata(**_metadata)


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
    # newline="\n" so the bytes are the same on every platform
    _stdout = io.TextIOWrapper(io.BytesIO(), encoding="cp1252", newline="\n")
    monkeypatch.setattr(sys, "stdout", _stdout)

    _config.execute()
    _stdout.flush()
    assert _stdout.buffer.getvalue() == b"Progress ? done\n"
    assert "\u2305" in _config._log_file.getvalue()


# A script for each shell that prints one line, and the shells to try on each
# kind of machine: what 'sh', 'bash' or 'python3' find on a Windows machine is
# not the CLI's doing
_SHELL_SCRIPTS = {
    "sh": 'echo "fair-ran-it"\n',
    "bash": 'echo "fair-ran-it"\n',
    "python3": 'print("fair-ran-it")\n',
    "julia": 'println("fair-ran-it")\n',
    "pwsh": 'Write-Output "fair-ran-it"\n',
    "powershell": 'Write-Output "fair-ran-it"\n',
    "batch": "@echo fair-ran-it\n",
}
_WINDOWS_SHELLS = ("batch", "powershell", "pwsh")
_OTHER_SHELLS = ("sh", "bash", "python3", "julia", "pwsh")


@pytest.mark.faircli_user_config
@pytest.mark.parametrize("shell", sorted(_SHELL_SCRIPTS))
def test_execute_script_under_a_path_with_a_space(
    shell: str, mocker: pytest_mock.MockerFixture, tmp_path
):
    _windows = platform.system() == "Windows"
    if shell not in (_WINDOWS_SHELLS if _windows else _OTHER_SHELLS):
        pytest.skip(f"Shell '{shell}' is not one for this platform")
    # 'batch' has no program: the script itself is the command
    _program = fdp_user.SHELLS[shell]["exec"][0]
    if shell != "batch" and not shutil.which(_program):
        pytest.skip(f"Shell '{shell}' is not installed")

    _job_dir = tmp_path / "job dir"
    _job_dir.mkdir()
    _script = _job_dir / f"script.{fdp_user.SHELLS[shell]['extension']}"
    _script.write_text(_SHELL_SCRIPTS[shell])
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": str(_job_dir),
            "script_path": str(_script),
            "shell": shell,
        }
    }
    _config.env = dict(os.environ)
    _config._log_file = io.StringIO()
    mocker.patch(
        "fair.configuration.get_current_user_name", lambda *args: [""]
    )
    mocker.patch("fair.configuration.get_current_user_email", lambda *args: "")

    _config.execute()
    assert "fair-ran-it" in _config._log_file.getvalue()


# A shell with flags, a script for it, and what the script prints: for sh,
# bash, python3 and julia, only when the flags reach the program
_FLAGGED_SHELLS = {
    "sh -x": ('echo "fair-ran-it"\n', "+ echo"),
    "bash -x": ('echo "fair-ran-it"\n', "+ echo"),
    "python3 -O": (
        'print("asserts", "off" if not __debug__ else "on")\n',
        "asserts off",
    ),
    "julia --project=@.": (
        'println(Base.active_project() == joinpath(pwd(), "Project.toml") '
        '? "in its project" : "elsewhere")\n',
        "in its project",
    ),
    "pwsh -NoProfile": ('Write-Output "fair-ran-it"\n', "fair-ran-it"),
    "powershell -NoProfile": ('Write-Output "fair-ran-it"\n', "fair-ran-it"),
}


@pytest.mark.faircli_user_config
@pytest.mark.parametrize("shell", sorted(_FLAGGED_SHELLS))
def test_execute_gives_a_shell_its_flags(
    shell: str, mocker: pytest_mock.MockerFixture, tmp_path
):
    _name = shell.split()[0]
    _windows = platform.system() == "Windows"
    if _name not in (_WINDOWS_SHELLS if _windows else _OTHER_SHELLS):
        pytest.skip(f"Shell '{_name}' is not one for this platform")
    if not shutil.which(_name):
        pytest.skip(f"Shell '{_name}' is not installed")

    # The repository is a Julia project, for '--project=@.' to find
    _repo = tmp_path / "repo dir"
    _repo.mkdir()
    (_repo / "Project.toml").write_text("")
    _text, _printed = _FLAGGED_SHELLS[shell]
    _script = _repo / f"script.{fdp_user.SHELLS[_name]['extension']}"
    _script.write_text(_text)
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_repo": str(_repo),
            "script_path": str(_script),
            "shell": shell,
        }
    }
    _config.env = dict(os.environ)
    _config._log_file = io.StringIO()
    mocker.patch(
        "fair.configuration.get_current_user_name", lambda *args: [""]
    )
    mocker.patch("fair.configuration.get_current_user_email", lambda *args: "")

    _config.execute()
    assert _printed in _config._log_file.getvalue()


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


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "pattern,name,matches",
    [
        ("era5/t2m/*", "era5/t2m/1940-1949", True),
        ("era5/t2m/*", "era5/t2m/1940/x", False),
        ("era5/t2m/*", "other/era5/t2m/x", False),
        ("era5/t2m/*", "era5/t2m/", False),
        ("a/*/c", "a/b/c", True),
        ("a/*/c", "a/b/d/c", False),
        ("a.b/*", "aXb/1", False),
        # A pattern never matches itself, nor a name the registry refuses
        ("era5/t2m/*", "era5/t2m/*", False),
        ("era5/t2m/*", "era5/t2m/[1]", False),
    ],
)
def test_matches_wildcard(pattern: str, name: str, matches: bool):
    assert fdp_glob.matches_wildcard(pattern, name) is matches


@pytest.mark.faircli_user_config
def test_wildcard_write(mocker: pytest_mock.MockerFixture):
    # Registered: a/1 at 0.0.1, and a/thing/1 at 2.0.0, which the registry's
    # own filter matches to 'a/*' but a wildcard (one segment) does not
    _products = [
        {"name": "a/1", "version": "0.0.1", "namespace": "ns_url"},
        {"name": "a/thing/1", "version": "2.0.0", "namespace": "ns_url"},
    ]
    # The other fields of a registry row, none of which belongs in a config
    for _n, _product in enumerate(_products):
        _product.update(
            url=f"data_product/{_n}/",
            object=f"object/{_n}/",
            ro_crate=f"ro-crate/data-product/{_n}/",
            prov_report=f"prov-report/{_n}/",
            external_object=None,
            internal_format=False,
        )

    def dummy_get(uri, obj_path, token, params=None, **kwargs):
        _pattern = params["name"].replace("*", ".*")
        return [p for p in _products if re.fullmatch(_pattern, p["name"])]

    mocker.patch("fair.registry.requests.get", dummy_get)
    mocker.patch(
        "fair.registry.requests.url_get", lambda *args: {"name": "testing"}
    )
    mocker.patch("fair.registry.requests.local_token", lambda: "")
    mocker.patch(
        "fair.register.convert_key_value_to_id", lambda *args, **kwargs: 1
    )

    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_data_registry_url": "http://127.0.0.1:8000/api/",
            "default_input_namespace": "testing",
            "default_output_namespace": "testing",
            "default_write_version": "${{PATCH}}",
        },
        "write": [
            {
                "data_product": "a/*",
                "description": "A csv file",
                "file_type": "csv",
                "use": {"version": "${{MAJOR}}"},
            }
        ],
    }
    # As prepare() does
    _config._update_namespaces()
    _config._fill_all_block_types()
    _config._expand_wildcards("http://127.0.0.1:8000/api/", "")
    _config["write"] = _config._fill_versions("write")
    _config._config = _config._clean()

    _written = {
        entry["use"]["data_product"]: entry["use"]["version"]
        for entry in _config["write"]
    }
    # The existing match, and the pattern itself for new names, each a
    # major bump from what matches it
    assert _written == {"a/1": "1.0.0", "a/*": "1.0.0"}
    # Each is a valid write entry, described as the pattern entry describes it
    for entry in _config["write"]:
        fdp_valid.DataProductWrite(**entry)
        assert entry["file_type"] == "csv"
        assert entry["description"] == "A csv file"
        assert entry["public"] is True


_LOCAL = "http://127.0.0.1:8000/api/"
_REMOTE = "http://127.0.0.1:8001/api/"


# Stand in for registries holding only namespaces and data products, each
# registry given as (namespace, name, version) of its data products, newest
# last. Answers as a registry does: rows whole, newest first, a name filter
# in which '*' also matches '/', and a namespace filter by id.
def _mock_registries(
    mocker: pytest_mock.MockerFixture,
    registries: typing.Dict[str, typing.List[typing.Tuple[str, str, str]]],
    namespaces: typing.Dict[str, typing.List[str]] = None,
):
    _namespaces = {
        uri: list(
            dict.fromkeys(
                (namespaces or {}).get(uri, []) + [p[0] for p in products]
            )
        )
        for uri, products in registries.items()
    }

    def _namespace_url(uri: str, namespace: str) -> str:
        return f"{uri}namespace/{_namespaces[uri].index(namespace) + 1}/"

    def dummy_get(uri, obj_path, token, params=None, **kwargs):
        params = params or {}
        if obj_path == "namespace":
            return [
                {"url": _namespace_url(uri, name), "name": name}
                for name in _namespaces[uri]
                if fnmatch.fnmatchcase(name, params.get("name", "*"))
            ]
        assert obj_path == "data_product"
        return [
            {
                "url": f"{uri}data_product/{_n + 1}/",
                "name": _name,
                "version": _version,
                "namespace": _namespace_url(uri, _namespace),
                "object": f"{uri}object/{_n + 1}/",
                "ro_crate": f"{uri}ro-crate/data-product/{_n + 1}/",
                "prov_report": f"{uri}prov-report/{_n + 1}/",
                "external_object": None,
                "internal_format": False,
                "last_updated": "2026-10-05T10:00:00Z",
                "updated_by": f"{uri}users/1/",
            }
            for _n, (_namespace, _name, _version) in reversed(
                list(enumerate(registries[uri]))
            )
            if fnmatch.fnmatchcase(_name, params.get("name", "*"))
            and int(
                params.get("namespace", _namespaces[uri].index(_namespace) + 1)
            )
            == _namespaces[uri].index(_namespace) + 1
            and params.get("version", _version) == _version
        ]

    def dummy_url_get(url, token=None):
        _uri, _, _id = url.rstrip("/").rpartition("namespace/")
        return {"url": url, "name": _namespaces[_uri][int(_id) - 1]}

    mocker.patch("fair.registry.requests.get", dummy_get)
    mocker.patch("fair.registry.requests.url_get", dummy_url_get)
    mocker.patch("fair.registry.requests.local_token", lambda: "")


# The working config's read or write block, made as prepare() makes it
def _prepared_block(
    block_type: str,
    entries: typing.List[typing.Dict],
    remote_uri: str = None,
) -> typing.List[typing.Dict]:
    _config = fdp_user.JobConfiguration()
    _config._config = {
        "run_metadata": {
            "local_data_registry_url": _LOCAL,
            "default_input_namespace": "testing",
            "default_output_namespace": "testing",
        },
        block_type: entries,
    }
    _config._update_namespaces()
    _config._fill_all_block_types()
    _config._expand_wildcards(remote_uri or _LOCAL, "")
    _config[block_type] = _config._fill_versions(block_type, remote_uri, "")
    if block_type == "read":
        _config["read"] = _config._update_use_sections(_config["read"])
    # A block left with nothing in it is left out
    _block = _config._clean().get(block_type, [])
    for entry in _block:
        if block_type == "read":
            fdp_valid.DataProduct(**entry)
        else:
            fdp_valid.DataProductWrite(**entry)
    return _block


def _used(block: typing.List[typing.Dict]) -> typing.List[typing.Tuple]:
    return [
        (
            entry["use"]["namespace"],
            entry["use"]["data_product"],
            entry["use"]["version"],
        )
        for entry in block
    ]


# One family in two namespaces: in 'testing' a name at two versions, and a
# name a segment too deep to match
_FAMILY = [
    ("testing", "fetched/era5/1940", "0.0.1"),
    ("other", "fetched/era5/1940", "9.0.0"),
    ("testing", "fetched/era5/1941", "0.0.1"),
    ("other", "fetched/era5/1999", "1.0.0"),
    ("testing", "fetched/era5/monthly/1940", "0.0.1"),
    ("testing", "fetched/era5/1940", "0.0.2"),
]


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "use,expected",
    [
        # In its namespace only, and the highest version of each name; the
        # pattern's own entry is kept, last, at its matches' highest version,
        # the version it names, or 0.0.0
        (
            {},
            [
                ("testing", "fetched/era5/1940", "0.0.2"),
                ("testing", "fetched/era5/1941", "0.0.1"),
                ("testing", "fetched/era5/*", "0.0.2"),
            ],
        ),
        (
            {"namespace": "other"},
            [
                ("other", "fetched/era5/1999", "1.0.0"),
                ("other", "fetched/era5/1940", "9.0.0"),
                ("other", "fetched/era5/*", "9.0.0"),
            ],
        ),
        # A version given is the version read, of each name that has it
        (
            {"version": "0.0.1"},
            [
                ("testing", "fetched/era5/1941", "0.0.1"),
                ("testing", "fetched/era5/1940", "0.0.1"),
                ("testing", "fetched/era5/*", "0.0.1"),
            ],
        ),
        (
            {"version": "0.0.2"},
            [
                ("testing", "fetched/era5/1940", "0.0.2"),
                ("testing", "fetched/era5/*", "0.0.2"),
            ],
        ),
        (
            {"version": "${{LATEST}}", "namespace": "other"},
            [
                ("other", "fetched/era5/1999", "1.0.0"),
                ("other", "fetched/era5/1940", "9.0.0"),
                ("other", "fetched/era5/*", "9.0.0"),
            ],
        ),
        # A pattern asks for whatever matches it, which may be nothing: then
        # its own entry alone says it was given
        ({"version": "3.0.0"}, [("testing", "fetched/era5/*", "3.0.0")]),
        (
            {"namespace": "unregistered"},
            [("unregistered", "fetched/era5/*", "0.0.0")],
        ),
    ],
)
def test_wildcard_read(mocker: pytest_mock.MockerFixture, use, expected):
    _mock_registries(mocker, {_LOCAL: _FAMILY})
    _read = _prepared_block(
        "read", [{"data_product": "fetched/era5/*", "use": dict(use)}]
    )
    assert _used(_read) == expected
    assert all(
        entry["data_product"] == entry["use"]["data_product"]
        for entry in _read
    )


@pytest.mark.faircli_user_config
def test_readables_leave_out_a_patterns_own_entry():
    _config = fdp_user.JobConfiguration()
    _config["read"] = [
        {
            "data_product": "fetched/era5/1940",
            "use": {"namespace": "testing", "version": "0.0.2"},
        },
        {
            "data_product": "fetched/era5/*",
            "use": {"namespace": "testing", "version": "0.0.2"},
        },
    ]
    assert _config.get_readables() == ["testing:fetched/era5/1940@v0.0.2"]


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "use,expected",
    [
        # One entry for a name however many versions it has, and the pattern
        # for names that are new
        (
            {},
            [
                ("testing", "fetched/era5/1940", "0.0.3"),
                ("testing", "fetched/era5/1941", "0.0.2"),
                ("testing", "fetched/era5/*", "0.0.3"),
            ],
        ),
        # A name in another namespace is not this pattern's to write
        (
            {"namespace": "other"},
            [
                ("other", "fetched/era5/1999", "1.0.1"),
                ("other", "fetched/era5/1940", "9.0.1"),
                ("other", "fetched/era5/*", "9.0.1"),
            ],
        ),
        # The first write to a namespace, which the registry does not have
        ({"namespace": "ECMWF"}, [("ECMWF", "fetched/era5/*", "0.0.1")]),
    ],
)
def test_wildcard_write_namespace(
    mocker: pytest_mock.MockerFixture, use, expected
):
    _mock_registries(mocker, {_LOCAL: _FAMILY})
    _write = _prepared_block(
        "write",
        [
            {
                "data_product": "fetched/era5/*",
                "description": "A year of ERA5",
                "file_type": "nc",
                "use": dict(use),
            }
        ],
    )
    assert _used(_write) == expected


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "use,expected",
    [
        ({}, ("testing", "fetched/era5/1940", "0.0.2")),
        ({"version": "0.0.1"}, ("testing", "fetched/era5/1940", "0.0.1")),
        ({"namespace": "other"}, ("other", "fetched/era5/1940", "9.0.0")),
    ],
)
def test_read_of_a_name(mocker: pytest_mock.MockerFixture, use, expected):
    _mock_registries(mocker, {_LOCAL: _FAMILY})
    _read = _prepared_block(
        "read", [{"data_product": "fetched/era5/1940", "use": dict(use)}]
    )
    assert _used(_read) == [expected]


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "name,use,wanted",
    [
        # No such name in a namespace the registry has
        ("fetched/era5/1850", {}, "any version"),
        # The name is there, but in another namespace
        ("fetched/era5/1999", {}, "any version"),
        # A namespace the registry does not have
        ("fetched/era5/1940", {"namespace": "unregistered"}, "any version"),
        # No such version
        ("fetched/era5/1940", {"version": "9.9.9"}, "version '9.9.9'"),
    ],
)
def test_read_of_nothing(mocker: pytest_mock.MockerFixture, name, use, wanted):
    _mock_registries(mocker, {_LOCAL: _FAMILY})
    with pytest.raises(fdp_exc.UserConfigError) as _error:
        _prepared_block("read", [{"data_product": name, "use": dict(use)}])
    _namespace = use.get("namespace", "testing")
    assert _error.value.msg == (
        f"Cannot read data product '{name}': {wanted} of it was not found in "
        f"namespace '{_namespace}' on registry '{_LOCAL}'"
    )


@pytest.mark.faircli_user_config
@pytest.mark.parametrize(
    "name,use,version",
    [
        # Only on the remote: its latest version there, not 0.0.0
        ("shared/elevation", {"namespace": "PSU"}, "1.2.0"),
        # On both: what the remote has is what there is to fetch
        ("shared/landcover", {"namespace": "PSU"}, "2.0.0"),
        (
            "shared/landcover",
            {"namespace": "PSU", "version": "1.0.0"},
            "1.0.0",
        ),
        # Only here, as what a local run wrote is
        ("fetched/era5/1940", {}, "0.0.2"),
    ],
)
def test_pull_reads_from_remote(
    mocker: pytest_mock.MockerFixture, name, use, version
):
    _mock_registries(
        mocker,
        {
            _REMOTE: [
                ("PSU", "shared/elevation", "1.0.0"),
                ("PSU", "shared/elevation", "1.2.0"),
                ("PSU", "shared/landcover", "1.0.0"),
                ("PSU", "shared/landcover", "2.0.0"),
            ],
            _LOCAL: _FAMILY + [("PSU", "shared/landcover", "1.0.0")],
        },
        # 'pull' copies the remote's namespaces first
        namespaces={_LOCAL: ["PSU"]},
    )
    _read = _prepared_block(
        "read", [{"data_product": name, "use": dict(use)}], remote_uri=_REMOTE
    )
    assert _used(_read) == [(use.get("namespace", "testing"), name, version)]


@pytest.mark.faircli_user_config
def test_pull_of_nothing(mocker: pytest_mock.MockerFixture):
    _mock_registries(mocker, {_REMOTE: [], _LOCAL: _FAMILY})
    with pytest.raises(fdp_exc.UserConfigError) as _error:
        _prepared_block(
            "read", [{"data_product": "shared/elevation"}], remote_uri=_REMOTE
        )
    assert _error.value.msg.endswith(
        f"in namespace 'testing' on registry '{_REMOTE}' or '{_LOCAL}'"
    )
