import inspect
import json
import os
import subprocess
import sys
import time
import typing

import click.testing
import pytest
import pytest_mock
import requests
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.registry.lifecycle as fdp_life
import fair.registry.server as fdp_serv
import fair.session
import fair.user_config
from fair.cli import cli

URI = "http://127.0.0.1:8000/api/"


class _Server:
    """A stand-in for the registry's server, started and stopped by mocks"""

    def __init__(self, mocker: pytest_mock.MockerFixture, running: bool = False):
        self.running = running
        self.launch = mocker.patch(
            "fair.registry.server.launch_server", side_effect=self._launch
        )
        self.stop = mocker.patch(
            "fair.registry.server.stop_server", side_effect=self._stop
        )
        mocker.patch(
            "fair.registry.lifecycle._server_here",
            side_effect=lambda *args: True if self.running else None,
        )

    def _launch(self, **kwargs):
        self.running = True

    def _stop(self, **kwargs):
        self.running = False


@pytest.fixture
def registry_dir(tmp_path, mocker: pytest_mock.MockerFixture) -> str:
    _dir = os.path.join(tmp_path, "registry")
    os.makedirs(_dir)
    mocker.patch("fair.common.registry_home", lambda: _dir)
    mocker.patch("fair.configuration.get_local_uri", lambda: URI)
    return _dir


# The session records of an install
def _sessions(registry_dir: str) -> typing.List[str]:
    return sorted(fdp_life._session_records(registry_dir))


def _server_record(registry_dir: str) -> typing.Optional[typing.Dict]:
    return fdp_life._read_record(fdp_life.server_record_file(registry_dir))


# A child process that runs until it is killed
@pytest.fixture
def child() -> typing.Iterator[subprocess.Popen]:
    _child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"]
    )
    yield _child
    _child.kill()
    _child.wait()


@pytest.mark.faircli_lifecycle
def test_a_session_that_starts_the_registry_stops_it(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _session = fdp_life.open_session()
    assert _server.running
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_SESSIONS
    assert _session["registry_url"] == URI
    assert _session["token_file"] == os.path.join(registry_dir, "token")
    assert len(_sessions(registry_dir)) == 1

    assert fdp_life.close_session(_session["session"])
    assert not _server.running
    assert not _server_record(registry_dir)
    assert not _sessions(registry_dir)


@pytest.mark.faircli_lifecycle
def test_a_session_that_finds_the_registry_running_leaves_it(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    # This install's server, running with no record: started by the
    # registry's own script, say
    _server = _Server(mocker, running=True)
    _session = fdp_life.open_session()
    _server.launch.assert_not_called()
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_NOBODY

    fdp_life.close_session(_session["session"])
    _server.stop.assert_not_called()
    assert _server.running


@pytest.mark.faircli_lifecycle
def test_the_last_of_two_sessions_stops_the_registry(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _first = fdp_life.open_session()
    _second = fdp_life.open_session()
    assert _server.launch.call_count == 1

    fdp_life.close_session(_first["session"])
    assert _server.running
    fdp_life.close_session(_second["session"])
    assert not _server.running


@pytest.mark.faircli_lifecycle
def test_the_user_adopts_a_registry_a_session_started(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _session = fdp_life.open_session()
    assert fdp_life.start_for_user(port=8000) == URI
    assert _server.launch.call_count == 1
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_USER

    fdp_life.close_session(_session["session"])
    assert _server.running

    with pytest.raises(fdp_exc.UnexpectedRegistryServerState, match="already"):
        fdp_life.start_for_user(port=8000)

    fdp_life.stop_for_user()
    assert not _server.running
    assert not _server_record(registry_dir)


@pytest.mark.faircli_lifecycle
def test_the_user_adopts_a_registry_nothing_recorded(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker, running=True)
    fdp_life.start_for_user(port=8000)
    _server.launch.assert_not_called()
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_USER


@pytest.mark.faircli_lifecycle
def test_a_session_of_a_process_that_has_gone_is_removed(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child
):
    _server = _Server(mocker)
    _gone = fdp_life.open_session(pid=child.pid)
    _ours = fdp_life.open_session()
    child.kill()
    child.wait()

    # Its process gone, the child's session no longer holds the registry
    fdp_life.close_session(_ours["session"])
    assert not _server.running
    assert not _sessions(registry_dir)
    assert not fdp_life.close_session(_gone["session"])


@pytest.mark.faircli_lifecycle
def test_a_session_whose_process_id_was_reused_is_removed(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _session = fdp_life.open_session()
    _path = os.path.join(
        fdp_life.records_directory(registry_dir), f"{_session['session']}.json"
    )
    _record = fdp_life._read_record(_path)
    # This process, but not the one that opened the session
    _record["process_started"] -= 3600
    fdp_life._write_record(_path, _record)

    assert not fdp_life.live_sessions()
    _other = fdp_life.open_session()
    assert _sessions(registry_dir) == [
        os.path.join(
            fdp_life.records_directory(registry_dir), f"{_other['session']}.json"
        )
    ]
    fdp_life.close_session(_other["session"])
    assert not _server.running


@pytest.mark.faircli_lifecycle
def test_a_session_for_a_process_that_does_not_exist_is_refused(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child
):
    _Server(mocker)
    child.kill()
    child.wait()
    with pytest.raises(fdp_exc.RegistryError, match="No process"):
        fdp_life.open_session(pid=child.pid)


# A server record written as though on another host
def _record_elsewhere(registry_dir: str) -> str:
    os.makedirs(fdp_life.records_directory(registry_dir), exist_ok=True)
    fdp_life._write_record(
        fdp_life.server_record_file(registry_dir),
        {
            "host": "another-host",
            "uri": URI,
            "holder": fdp_life.HELD_BY_USER,
            "started": "2026-10-18T12:00:00",
        },
    )
    return fdp_life.server_record_file(registry_dir)


@pytest.mark.faircli_lifecycle
@pytest.mark.parametrize(
    "action",
    [
        lambda: fdp_life.open_session(),
        lambda: fdp_life.start_for_user(port=8000),
        lambda: fdp_life.stop_for_user(),
        lambda: fdp_life.stop_for_user(force=True),
        lambda: fdp_life.stop_for_removal(fdp_com.registry_home()),
    ],
    ids=["session", "start", "stop", "stop --force", "removal"],
)
def test_a_server_on_another_host_refuses_everything_and_stays_recorded(
    registry_dir: str, mocker: pytest_mock.MockerFixture, action
):
    _server = _Server(mocker)
    _file = _record_elsewhere(registry_dir)
    with pytest.raises(fdp_exc.RegistryError) as e:
        action()
    assert "another-host" in e.value.msg
    assert f"delete the file '{_file}'" in e.value.hint
    assert os.path.exists(_file)
    _server.launch.assert_not_called()
    _server.stop.assert_not_called()


@pytest.mark.faircli_lifecycle
def test_another_installs_server_on_the_port_is_refused(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.registry.lifecycle._server_here", return_value=False)
    _launch = mocker.patch("fair.registry.server.launch_server")
    with pytest.raises(fdp_exc.RegistryError, match="is not the local registry"):
        fdp_life.open_session()
    with pytest.raises(fdp_exc.RegistryError, match="is not the local registry"):
        fdp_life.start_for_user(port=8000)
    _launch.assert_not_called()
    assert not _server_record(registry_dir)
    assert not _sessions(registry_dir)


@pytest.mark.faircli_lifecycle
def test_a_start_that_fails_leaves_no_record(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.registry.lifecycle._server_here", return_value=None)
    mocker.patch(
        "fair.registry.server.launch_server",
        side_effect=fdp_exc.RegistryError("Failed to start local registry"),
    )
    with pytest.raises(fdp_exc.RegistryError, match="Failed to start"):
        fdp_life.open_session()
    with pytest.raises(fdp_exc.RegistryError, match="Failed to start"):
        fdp_life.start_for_user(port=8000)
    assert not _server_record(registry_dir)
    assert not _sessions(registry_dir)


@pytest.mark.faircli_lifecycle
def test_a_port_that_never_answers_is_an_error(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    mocker.patch("requests.get", side_effect=requests.exceptions.ReadTimeout)
    _launch = mocker.patch("fair.registry.server.launch_server")
    with pytest.raises(fdp_exc.RegistryError, match="does not answer"):
        fdp_life.open_session()
    _launch.assert_not_called()


@pytest.mark.faircli_lifecycle
def test_a_check_of_the_registry_has_a_time_limit(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _get = mocker.patch(
        "requests.get", side_effect=requests.exceptions.ConnectionError
    )
    assert fdp_life._server_here(registry_dir, URI) is None
    assert _get.call_args.kwargs["timeout"] == fdp_serv.REQUEST_SECONDS


@pytest.mark.faircli_lifecycle
def test_the_user_cannot_stop_a_registry_in_use_without_force(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    fdp_life.start_for_user(port=8000)
    _session = fdp_life.open_session()
    with pytest.raises(
        fdp_exc.UnexpectedRegistryServerState, match="still be running"
    ):
        fdp_life.stop_for_user()
    assert _server.running

    fdp_life.stop_for_user(force=True)
    assert not _server.running
    assert not _sessions(registry_dir)
    assert not fdp_life.close_session(_session["session"])


@pytest.mark.faircli_lifecycle
def test_stopping_a_registry_that_is_not_running(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _Server(mocker)
    with pytest.raises(fdp_exc.UnexpectedRegistryServerState, match="not running"):
        fdp_life.stop_for_user()


@pytest.mark.faircli_lifecycle
def test_removal_waits_for_the_sessions(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _session = fdp_life.open_session()
    with pytest.raises(fdp_exc.RegistryError, match="is in use"):
        fdp_life.stop_for_removal(registry_dir)
    assert _server.running

    fdp_life.start_for_user(port=8000)
    fdp_life.close_session(_session["session"])
    fdp_life.stop_for_removal(registry_dir)
    assert not _server.running
    assert not _server_record(registry_dir)


@pytest.mark.faircli_lifecycle
def test_the_lock_holds_between_processes(registry_dir: str):
    # A second process holds the install's lock for a while; this one waits
    _child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys, time\n"
            "import fair.registry.lifecycle as fdp_life\n"
            "with fdp_life._locked(sys.argv[1]):\n"
            "    print('locked', flush=True)\n"
            "    time.sleep(2)\n",
            registry_dir,
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    try:
        assert _child.stdout.readline().strip() == "locked"
        _start = time.monotonic()
        with fdp_life._locked(registry_dir):
            _waited = time.monotonic() - _start
    finally:
        _child.wait()
    assert _waited > 1


@pytest.mark.faircli_lifecycle
def test_status_names_the_holder_and_the_sessions(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _Server(mocker)
    _session = fdp_life.open_session()
    _status = fdp_life.status()
    assert _status["server"]["holder"] == fdp_life.HELD_BY_SESSIONS
    assert not _status["elsewhere"]
    assert [s["id"] for s in _status["sessions"]] == [_session["session"]]
    assert _status["sessions"][0]["pid"] == os.getpid()

    _record_elsewhere(registry_dir)
    _status = fdp_life.status()
    assert _status["elsewhere"]
    assert _status["server"]["host"] == "another-host"


# A command line runner that keeps what a command writes to stderr apart from
# its stdout, which click before 8.2 does only when asked
def _runner() -> click.testing.CliRunner:
    if "mix_stderr" in inspect.signature(click.testing.CliRunner).parameters:
        return click.testing.CliRunner(mix_stderr=False)
    return click.testing.CliRunner()


@pytest.mark.faircli_lifecycle
def test_session_commands(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child
):
    _server = _Server(mocker)
    _result = _runner().invoke(
        cli, ["registry", "session", "open", "--pid", str(child.pid), "--json"]
    )
    assert _result.exit_code == 0, _result.output
    _session = json.loads(_result.stdout)
    assert sorted(_session) == ["registry_url", "session", "token_file"]
    assert _session["registry_url"] == URI
    assert _session["token_file"] == os.path.join(registry_dir, "token")
    assert _server.running

    _result = _runner().invoke(
        cli, ["registry", "session", "close", _session["session"]]
    )
    assert _result.exit_code == 0, _result.output
    assert not _server.running

    # A session closed already, as one cleaned up after its process ended
    _result = _runner().invoke(
        cli, ["registry", "session", "close", _session["session"]]
    )
    assert _result.exit_code == 0
    assert not _result.stdout
    assert "no session" in _result.stderr


@pytest.mark.faircli_lifecycle
def test_session_open_that_fails_prints_nothing_on_stdout(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child
):
    _record_elsewhere(registry_dir)
    _result = _runner().invoke(
        cli, ["registry", "session", "open", "--pid", str(child.pid), "--json"]
    )
    assert _result.exit_code == 1
    assert not _result.stdout
    assert "another-host" in _result.stderr


@pytest.mark.faircli_lifecycle
def test_session_open_needs_a_pid(registry_dir: str):
    _result = _runner().invoke(cli, ["registry", "session", "open", "--json"])
    assert _result.exit_code != 0
    assert "--pid" in _result.output + _result.stderr


@pytest.mark.faircli_lifecycle
@pytest.mark.parametrize("port, expected", [(None, 8010), (8020, 8020)])
def test_registry_start_takes_the_configured_port(
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    port: typing.Optional[int],
    expected: int,
):
    with open(fdp_com.global_fdpconfig()) as conf_f:
        _conf = yaml.safe_load(conf_f)
    _conf["registries"]["local"]["uri"] = "http://127.0.0.1:8010/api/"
    with open(fdp_com.global_fdpconfig(), "w") as conf_f:
        yaml.dump(_conf, conf_f)
    _start = mocker.patch(
        "fair.registry.lifecycle.start_for_user", return_value=URI
    )
    _options = ["--port", str(port)] if port else []
    _result = click.testing.CliRunner().invoke(
        cli, ["registry", "start", *_options]
    )
    assert _result.exit_code == 0, _result.output
    assert _start.call_args.kwargs["port"] == expected


@pytest.mark.faircli_lifecycle
def test_a_command_holds_a_session_and_names_it_to_its_script(
    mocker: pytest_mock.MockerFixture,
):
    # Only run()'s handling of the session is under test
    _session = object.__new__(fair.session.FAIR)
    _session._allow_dirty = False
    _session._session_id = "a-session"
    _session._session_config = mocker.MagicMock()
    mocker.patch.object(_session, "_pre_job_setup")
    mocker.patch.object(_session, "check_git_repo_state")
    mocker.patch.object(_session, "_post_job_breakdown")
    _session.run(passive=True)
    assert _session._session_config.session_id == "a-session"


@pytest.mark.faircli_lifecycle
def test_the_environment_of_a_script_names_its_session(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    _config = fair.user_config.JobConfiguration()
    for _name, _value in (
        ("local_repository", str(tmp_path)),
        ("default_data_store", str(tmp_path)),
        ("script", "script.sh"),
    ):
        mocker.patch.object(
            fair.user_config.JobConfiguration,
            _name,
            new_callable=mocker.PropertyMock,
            return_value=_value,
        )
    mocker.patch("fair.registry.requests.local_token", lambda *args: "token")
    assert "FDP_SESSION" not in _config._create_environment()
    _config.session_id = "a-session"
    assert _config._create_environment()["FDP_SESSION"] == "a-session"


@pytest.mark.faircli_lifecycle
@pytest.mark.parametrize(
    "command, answer",
    [
        (["purge", "--all", "--yes"], None),
        (["registry", "uninstall"], "y\n"),
        (["registry", "install", "--force", "--directory", "{registry}"], None),
    ],
    ids=["purge", "uninstall", "install"],
)
def test_removing_a_registry_running_elsewhere_removes_nothing(
    local_config: typing.Tuple[str, str],
    registry_dir: str,
    mocker: pytest_mock.MockerFixture,
    command: typing.List[str],
    answer: typing.Optional[str],
):
    _install = mocker.patch("fair.registry.server.install_registry")
    _file = _record_elsewhere(registry_dir)
    _command = [c.replace("{registry}", registry_dir) for c in command]
    _result = click.testing.CliRunner().invoke(cli, _command, input=answer)
    assert _result.exit_code == 1, _result.output
    assert "another-host" in _result.output
    assert _file in _result.output
    assert os.path.exists(_file)
    assert os.path.exists(os.path.join(local_config[1], fdp_com.FAIR_FOLDER))
    _install.assert_not_called()


@pytest.mark.faircli_lifecycle
def test_registry_status_command(
    local_config: typing.Tuple[str, str],
    registry_dir: str,
    mocker: pytest_mock.MockerFixture,
):
    _Server(mocker)
    fdp_life.start_for_user(port=8000)
    _session = fdp_life.open_session()
    _result = click.testing.CliRunner().invoke(cli, ["registry", "status"])
    assert _result.exit_code == 0, _result.output
    assert f"Server running at: {URI}" in _result.output
    assert "Held by the user" in _result.output
    assert fdp_life.server_record_file(registry_dir) in _result.output
    assert f"Session {_session['session']}: process {os.getpid()}" in _result.output


@pytest.mark.faircli_lifecycle
def test_closing_a_session_removes_nothing_else(registry_dir: str):
    _outside = os.path.join(registry_dir, "outside.json")
    open(_outside, "w").close()
    assert not fdp_life.close_session("../outside")
    assert os.path.exists(_outside)
