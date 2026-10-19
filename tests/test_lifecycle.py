import inspect
import json
import logging
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
        mocker.patch("fair.registry.server._refuse_failing")

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


# Leave the records a reboot leaves: those of the user's registry and of a
# session whose command was running, with the server and the session's process
# ended
def _reboot(server: _Server, child: subprocess.Popen) -> None:
    fdp_life.start_for_user(port=8000)
    fdp_life.open_session(pid=child.pid)
    child.kill()
    child.wait()
    server.running = False


@pytest.mark.faircli_lifecycle
def test_after_a_reboot_a_session_starts_the_registry_again(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child, capsys
):
    _server = _Server(mocker)
    _reboot(_server, child)
    capsys.readouterr()
    _session = fdp_life.open_session()
    _said = capsys.readouterr().err
    assert f"recorded as running at {URI}" in _said
    assert "started by 'fair registry start'" in _said
    assert f"A session not closed by its process, {child.pid}," in _said
    assert _server.launch.call_count == 2
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_SESSIONS
    assert len(_sessions(registry_dir)) == 1

    fdp_life.close_session(_session["session"])
    assert not _server.running
    assert not _server_record(registry_dir)
    assert not _sessions(registry_dir)


@pytest.mark.faircli_lifecycle
def test_after_a_reboot_the_user_starts_the_registry_again(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child, capsys
):
    _server = _Server(mocker)
    _reboot(_server, child)
    capsys.readouterr()
    _status = fdp_life.status()
    assert not _status["server"]
    assert not _status["sessions"]
    assert not _sessions(registry_dir)
    assert "has stopped outside 'fair'" in capsys.readouterr().err

    # Said once, as the records are removed
    fdp_life.start_for_user(port=8000)
    assert not capsys.readouterr().err
    assert _server.launch.call_count == 2
    assert _server_record(registry_dir)["holder"] == fdp_life.HELD_BY_USER


@pytest.mark.faircli_lifecycle
def test_records_removed_in_normal_use_are_not_said(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child, capsys
):
    _Server(mocker)
    _first = fdp_life.open_session()
    _second = fdp_life.open_session(pid=child.pid)
    fdp_life.close_session(_second["session"])
    fdp_life.close_session(_first["session"])
    fdp_life.start_for_user(port=8000)
    fdp_life.open_session()
    fdp_life.stop_for_user(force=True)
    assert not capsys.readouterr().err


@pytest.mark.faircli_lifecycle
def test_a_record_whose_writing_was_cut_short_is_removed(
    registry_dir: str, mocker: pytest_mock.MockerFixture, capsys
):
    _Server(mocker)
    _dir = fdp_life.records_directory(registry_dir)
    os.makedirs(_dir)
    _cut_short = os.path.join(_dir, "tmpcutshort.tmp")
    with open(_cut_short, "w") as out_f:
        out_f.write('{"id": "')
    fdp_life.status()
    assert not os.path.exists(_cut_short)
    assert (
        "half-written is removed: tmpcutshort.tmp" in capsys.readouterr().err
    )


@pytest.mark.faircli_lifecycle
def test_a_record_that_cannot_be_removed_is_said_and_left(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child, capsys
):
    _server = _Server(mocker)
    _reboot(_server, child)
    _stale = _sessions(registry_dir)
    capsys.readouterr()
    mocker.patch.object(
        fdp_life.os,
        "remove",
        side_effect=PermissionError(13, "Permission denied"),
    )
    mocker.patch.object(fdp_life.time, "sleep")

    # The command carries on, the registry started again
    fdp_life.open_session()
    assert _server.launch.call_count == 2
    _said = capsys.readouterr().err
    assert _said.count("could not be removed: Permission denied") == 2
    assert "is removed" not in _said
    assert all(os.path.exists(_path) for _path in _stale)


@pytest.mark.faircli_lifecycle
def test_on_windows_a_file_held_open_is_waited_for(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    mocker.patch.object(fdp_life.platform, "system", return_value="Windows")
    _sleep = mocker.patch.object(fdp_life.time, "sleep")
    _in_use = PermissionError(13, "in use by another process")
    _remove = mocker.patch.object(
        fdp_life.os, "remove", side_effect=[_in_use, _in_use, None]
    )
    assert fdp_life._remove(os.path.join(tmp_path, "record.json"))
    assert _remove.call_count == 3
    assert _sleep.call_count == 2


@pytest.mark.faircli_lifecycle
def test_on_windows_a_file_already_gone_is_not_waited_for(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    mocker.patch.object(fdp_life.platform, "system", return_value="Windows")
    _sleep = mocker.patch.object(fdp_life.time, "sleep")
    assert fdp_life._remove(os.path.join(tmp_path, "record.json"))
    _sleep.assert_not_called()


@pytest.mark.faircli_lifecycle
def test_on_windows_a_record_held_open_is_waited_for(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    mocker.patch.object(fdp_life.platform, "system", return_value="Windows")
    _sleep = mocker.patch.object(fdp_life.time, "sleep")
    _replace = os.replace
    _in_use = [PermissionError(13, "in use by another process")] * 2

    def _replace_once_closed(*paths):
        if _in_use:
            raise _in_use.pop()
        _replace(*paths)

    mocker.patch.object(
        fdp_life.os, "replace", side_effect=_replace_once_closed
    )
    _path = os.path.join(tmp_path, "record.json")
    fdp_life._write_record(_path, {"id": "a"})
    assert fdp_life._read_record(_path) == {"id": "a"}
    assert _sleep.call_count == 2
    assert os.listdir(tmp_path) == ["record.json"]


@pytest.mark.faircli_lifecycle
def test_a_record_that_cannot_be_written_is_an_error_and_leaves_nothing(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    _server = _Server(mocker)
    _replace = mocker.patch.object(
        fdp_life.os,
        "replace",
        side_effect=PermissionError(13, "Access is denied"),
    )
    with pytest.raises(fdp_exc.RegistryError) as e:
        fdp_life.open_session()
    _file = fdp_life.server_record_file(registry_dir)
    assert f"'{_file}' could not be written: Access is denied" in e.value.msg
    assert os.listdir(fdp_life.records_directory(registry_dir)) == [
        fdp_life.LOCK_FILE
    ]

    # The registry started is used by the next command, as one found running
    mocker.stop(_replace)
    _session = fdp_life.open_session()
    assert _server.launch.call_count == 1
    fdp_life.close_session(_session["session"])


@pytest.mark.faircli_lifecycle
def test_a_session_whose_start_time_cannot_be_read_is_kept_and_said(
    local_config: typing.Tuple[str, str],
    registry_dir: str,
    mocker: pytest_mock.MockerFixture,
    child,
    capsys,
    caplog,
):
    # The session's process has gone, and its id is now a process whose start
    # time the system will not give, as a protected one's on Windows
    _Server(mocker)
    fdp_life.start_for_user(port=8000)
    fdp_life.open_session(pid=child.pid)
    child.kill()
    child.wait()
    _real = fdp_life.psutil.Process

    def _process(pid=None):
        if pid == child.pid:
            raise fdp_life.psutil.AccessDenied(pid)
        return _real(pid)

    mocker.patch.object(fdp_life.psutil, "Process", side_effect=_process)
    capsys.readouterr()

    with caplog.at_level(logging.WARNING, logger="FAIRDataPipeline"):
        _status = fdp_life.status()
        assert [_s["pid"] for _s in _status["sessions"]] == [child.pid]
        assert _status["sessions"][0]["start_unreadable"]
        with pytest.raises(
            fdp_exc.UnexpectedRegistryServerState,
            match="start time cannot be read",
        ):
            fdp_life.stop_for_user()
    # Said where it matters, not on every command
    assert not capsys.readouterr().err
    assert not [_r for _r in caplog.records if _r.levelno >= logging.WARNING]

    _result = click.testing.CliRunner().invoke(cli, ["registry", "status"])
    assert _result.exit_code == 0, _result.output
    assert f"{fdp_life.START_UNREADABLE})" in _result.output


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
@pytest.mark.parametrize(
    "recorded", [True, False], ids=["recorded", "unrecorded"]
)
def test_a_failing_registry_refuses_a_session_and_can_be_stopped(
    registry_dir: str, mocker: pytest_mock.MockerFixture, recorded: bool
):
    # This install's registry, running, fails every request with a token, as
    # one whose database is broken does
    with open(os.path.join(registry_dir, "token"), "w") as out_f:
        out_f.write("0" * 40)
    mocker.patch(
        "requests.get",
        side_effect=lambda url, **kwargs: mocker.Mock(
            status_code=500 if kwargs.get("headers") else 200
        ),
    )
    _stop = mocker.patch("fair.registry.server.stop_server")
    _launch = mocker.patch("fair.registry.server.launch_server")
    if recorded:
        fdp_life.start_for_user(port=8000)

    with pytest.raises(fdp_exc.RegistryError, match="HTTP 500") as e:
        fdp_life.open_session()
    assert os.path.join(registry_dir, "output.log") in e.value.hint
    assert not _sessions(registry_dir)
    _holder = fdp_life.HELD_BY_USER if recorded else None
    assert (fdp_life.status()["server"] or {}).get("holder") == _holder

    fdp_life.stop_for_user()
    _stop.assert_called_once()
    _launch.assert_not_called()
    assert not _server_record(registry_dir)


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
def test_the_lock_of_a_process_killed_holding_it_is_free(
    registry_dir: str, mocker: pytest_mock.MockerFixture
):
    # The lock file stays, as after a reboot, but the system releases the lock;
    # Windows may take a moment to
    mocker.patch.object(fdp_life, "LOCK_SECONDS", 30)
    _child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys, time\n"
            "import fair.registry.lifecycle as fdp_life\n"
            "with fdp_life._locked(sys.argv[1]):\n"
            "    print('locked', flush=True)\n"
            "    time.sleep(300)\n",
            registry_dir,
        ],
        stdout=subprocess.PIPE,
        text=True,
        cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    )
    try:
        assert _child.stdout.readline().strip() == "locked"
    finally:
        _child.kill()
        _child.wait()
    _records = fdp_life.records_directory(registry_dir)
    assert os.path.exists(os.path.join(_records, fdp_life.LOCK_FILE))
    with fdp_life._locked(registry_dir):
        pass


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
def test_session_open_after_a_reboot_keeps_its_json_alone(
    registry_dir: str, mocker: pytest_mock.MockerFixture, child
):
    _reboot(_Server(mocker), child)
    _result = _runner().invoke(
        cli,
        ["registry", "session", "open", "--pid", str(os.getpid()), "--json"],
    )
    assert _result.exit_code == 0, _result.output
    _session = json.loads(_result.stdout)
    assert sorted(_session) == ["registry_url", "session", "token_file"]
    assert "has stopped outside 'fair'" in _result.stderr
    assert (
        f"A session not closed by its process, {child.pid}," in _result.stderr
    )


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
