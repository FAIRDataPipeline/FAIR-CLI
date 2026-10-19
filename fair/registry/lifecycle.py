#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Lifecycle
==================

Who holds the local registry's server, and when it is started and stopped.

An install records the server it runs in its own directory: the host it runs
on, its address, and who holds it - the user, or the sessions that use it.
Every process that uses the registry holds a session for as long as it does,
recorded beside it with the process that owns it. A registry that a session
started is stopped when its last session closes; one the user holds runs until
'fair registry stop'; one that was running before anything recorded it is
never stopped by a session.

Contents
========

Functions
---------

    open_session - hold the local registry, starting it where none runs
    close_session - release a session, stopping a registry a session started
    session - hold a session for the length of a block
    start_for_user - start the registry, or adopt the running one, for the user
    stop_for_user - stop the registry at the user's request
    stop_for_removal - stop the registry before its install is removed
    live_sessions - the sessions whose processes are using the registry
    status - what the records say of the registry and its sessions

"""

__date__ = "2026-10-18"

import contextlib
import datetime
import functools
import json
import logging
import os
import platform
import re
import socket
import subprocess
import tempfile
import time
import typing
import urllib.parse
import uuid

import click
import psutil
import requests

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.server as fdp_serv

if platform.system() == "Windows":
    import msvcrt
else:
    import fcntl

logger = logging.getLogger("FAIRDataPipeline.Lifecycle")

RECORDS_DIRECTORY = "sessions"
SERVER_RECORD = "server.json"
LOCK_FILE = "lock"

# How long a process waits for another to finish starting or stopping the
# registry: a start may take a minute and a half
LOCK_SECONDS = 300

# Who holds a recorded server: the user, by 'fair registry start'; the
# sessions, one of which started it; or nobody, as it was running before
# anything recorded it
HELD_BY_USER = "user"
HELD_BY_SESSIONS = "sessions"
HELD_BY_NOBODY = "nobody"

# What is said of a session whose process exists but whose start time cannot
# be read, as on Windows for a protected process given a dead one's id
START_UNREADABLE = (
    "its start time cannot be read, so it is taken to be running"
)

# The locks this process holds, by file, with how deeply each is held: a
# second lock on the same file from one process would deadlock on Windows,
# and releasing it would release the first on Unix
_held_locks: typing.Dict[str, int] = {}


def records_directory(registry_dir: str) -> str:
    """Return the directory of an install that holds its server's records

    Parameters
    ----------
    registry_dir : str
        directory of the registry install

    Returns
    -------
    str
        directory of the server record and the session records
    """
    return os.path.join(registry_dir, RECORDS_DIRECTORY)


def server_record_file(registry_dir: str) -> str:
    """Return the file that records the server an install runs

    Parameters
    ----------
    registry_dir : str
        directory of the registry install

    Returns
    -------
    str
        path of the server record
    """
    return os.path.join(records_directory(registry_dir), SERVER_RECORD)


# The name this host goes by in the records. On macOS the host name follows
# the network unless it has been set, where the local host name does not, so
# a record written on one network would otherwise name another host on the
# next
@functools.lru_cache(maxsize=None)
def _host() -> str:
    if platform.system() == "Darwin":
        try:
            _name = subprocess.run(
                ["scutil", "--get", "LocalHostName"],
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            if _name:
                return _name
        except (OSError, subprocess.CalledProcessError):
            pass
    return socket.gethostname()


# Take an exclusive lock on an open file, raising OSError where another
# process holds it rather than waiting
def _lock_file(lock_file: typing.IO) -> None:
    lock_file.seek(0)
    if platform.system() == "Windows":
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        fcntl.lockf(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


# Give up the lock that _lock_file took
def _unlock_file(lock_file: typing.IO) -> None:
    lock_file.seek(0)
    if platform.system() == "Windows":
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        fcntl.lockf(lock_file.fileno(), fcntl.LOCK_UN)


# Hold the install's lock for the block, so that one process at a time
# decides whether to start or stop the registry and does it. The lock is on a
# file in the install's directory, so that it holds between the hosts of a
# shared file system as well as between processes
@contextlib.contextmanager
def _locked(registry_dir: str) -> typing.Iterator[None]:
    if not os.path.isdir(registry_dir):
        raise fdp_exc.RegistryError(
            f"No local registry is installed in '{registry_dir}'",
            hint="Install one with 'fair registry install'",
        )
    _path = os.path.join(records_directory(registry_dir), LOCK_FILE)
    if _path in _held_locks:
        _held_locks[_path] += 1
        try:
            yield
        finally:
            _held_locks[_path] -= 1
        return
    os.makedirs(os.path.dirname(_path), exist_ok=True)
    with open(_path, "a+") as lock_f:
        _deadline = time.monotonic() + LOCK_SECONDS
        while True:
            try:
                _lock_file(lock_f)
                break
            except OSError as e:
                if time.monotonic() > _deadline:
                    raise fdp_exc.RegistryError(
                        "Timed out waiting for another process to finish "
                        f"starting or stopping the registry in '{registry_dir}'",
                        hint=f"The lock is held on '{_path}'",
                    ) from e
                time.sleep(0.2)
        _held_locks[_path] = 1
        try:
            yield
        finally:
            del _held_locks[_path]
            _unlock_file(lock_f)


# A record read from its file: None where there is none, or where it cannot
# be read, as a file left half-written could not
def _read_record(path: str) -> typing.Optional[typing.Dict]:
    try:
        with open(path, encoding="utf-8") as in_f:
            return json.load(in_f)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as e:
        logger.debug("Ignoring the unreadable record '%s': %s", path, e)
        return None


# Apply an operation to files, trying again briefly on Windows, where a file
# another process has open - a virus scanner, an indexer - cannot be removed
# or replaced until it is closed. A file that is not there is not waited for
def _retried(operation: typing.Callable[..., None], *paths: str) -> None:
    _tries = 5 if platform.system() == "Windows" else 1
    for _try in range(_tries):
        try:
            operation(*paths)
            return
        except FileNotFoundError:
            raise
        except OSError:
            if _try + 1 == _tries:
                raise
            time.sleep(0.2)


# Write a record whole or not at all, by renaming a complete file into place.
# One that cannot be put in place is an error, and its complete file removed
def _write_record(path: str, record: typing.Dict) -> None:
    _out_fd, _temp = tempfile.mkstemp(dir=os.path.dirname(path), suffix=".tmp")
    with os.fdopen(_out_fd, "w", encoding="utf-8") as out_f:
        json.dump(record, out_f, indent=2)
    try:
        _retried(os.replace, _temp, path)
    except OSError as e:
        _remove(_temp)
        raise fdp_exc.RegistryError(
            f"The record '{path}' could not be written: {e.strerror or e}",
            hint="Try again once no other program has it open",
        ) from e


# Remove a file that may already have gone, saying whether it has. One that
# cannot be removed is said and left: a stale record left is judged again
# next time
def _remove(path: str) -> bool:
    try:
        _retried(os.remove, path)
    except FileNotFoundError:
        pass
    except OSError as e:
        click.echo(
            f"Warning: '{path}' could not be removed: {e.strerror or e}",
            err=True,
        )
        return False
    return True


# Every session record of an install, by its file
def _session_records(registry_dir: str) -> typing.Dict[str, typing.Dict]:
    _dir = records_directory(registry_dir)
    if not os.path.isdir(_dir):
        return {}
    _records = {}
    for _name in os.listdir(_dir):
        if not _name.endswith(".json") or _name == SERVER_RECORD:
            continue
        _path = os.path.join(_dir, _name)
        _records[_path] = _read_record(_path) or {}
    return _records


# Whether the process a record names is running: a process id is reused, so
# its start time must be the one recorded too. A process that cannot be read
# is taken to be running
def _process_alive(pid: int, started: float) -> bool:
    try:
        return abs(psutil.Process(pid).create_time() - started) < 1
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        return True


# Whether a process's start time cannot be read, so that whether it is the
# process a record names cannot be told
def _start_unreadable(pid: typing.Optional[int]) -> bool:
    try:
        psutil.Process(pid).create_time()
    except psutil.AccessDenied:
        return True
    except (psutil.Error, TypeError, ValueError):
        pass
    return False


# When a process started, to tell it from a later one given its id
def _process_start(pid: int) -> float:
    try:
        return psutil.Process(pid).create_time()
    except psutil.NoSuchProcess as e:
        raise fdp_exc.RegistryError(
            f"No process with the id {pid} is running to hold a session"
        ) from e


# Whether a session is using the server recorded: its process runs, on this
# host, which is the server's. A session on another host cannot use a server
# that answers on this one, and with no server recorded there is nothing to use
def _session_is_live(
    session: typing.Dict, server: typing.Optional[typing.Dict]
) -> bool:
    try:
        return (
            bool(server)
            and session["host"] == server["host"] == _host()
            and _process_alive(session["pid"], session["process_started"])
        )
    except (KeyError, TypeError):
        return False


# Whether the server that answers at an address is this install's registry:
# None where nothing answers there, False where something else does
def _server_here(registry_dir: str, uri: str) -> typing.Optional[bool]:
    try:
        _response = requests.get(uri, timeout=fdp_serv.REQUEST_SECONDS)
    except requests.exceptions.ReadTimeout as e:
        raise fdp_exc.RegistryError(
            f"Something at '{uri}' accepts connections and does not answer",
            hint="Stop whatever holds the port, or start the registry on "
            "another with 'fair registry start --port'",
        ) from e
    except requests.exceptions.ConnectionError:
        return None
    if _response.status_code != 200:
        return False
    try:
        _token = fdp_req.local_token(registry_dir)
    except fdp_exc.FileNotFoundError:
        return False
    return fdp_serv._accepts_token(uri, _token)


# The refusal of a server at the registry's address that is another's
def _not_this_install(registry_dir: str, uri: str) -> fdp_exc.RegistryError:
    return fdp_exc.RegistryError(
        f"The server at '{uri}' is not the local registry installed in "
        f"'{registry_dir}', as it refuses that registry's token",
        hint="Another server holds the port: stop it, or start this "
        "registry on another port with 'fair registry start --port'",
    )


# The refusal of anything that would act on a server recorded on another
# host, naming the record, which is the user's to delete when that host has
# stopped running it: this host cannot tell
def _on_another_host(
    registry_dir: str, server: typing.Dict
) -> fdp_exc.RegistryError:
    _file = server_record_file(registry_dir)
    return fdp_exc.RegistryError(
        f"The local registry installed in '{registry_dir}' is recorded as "
        f"running on the host '{server['host']}', at '{server['uri']}', and "
        f"this host is '{_host()}': one install's registry runs on one host "
        "at a time, and cannot be started, stopped or used from another",
        hint=f"Stop it on '{server['host']}' with 'fair registry stop'. If "
        f"'{server['host']}' no longer runs it - it has shut down or "
        f"restarted - delete the file '{_file}', which records it, and try "
        "again",
    )


# The server record, with every record that no longer holds removed: the
# server's where it no longer answers as this install's, the sessions whose
# processes have gone, and any whose writing was cut short. Only something
# ending outside 'fair' leaves one, so each removal is said. A server recorded
# on another host is refused and nothing is removed
def _current_server(registry_dir: str) -> typing.Optional[typing.Dict]:
    _server = _read_record(server_record_file(registry_dir))
    if _server and _server.get("host") != _host():
        raise _on_another_host(registry_dir, _server)
    if _server and not _server_here(registry_dir, _server["uri"]):
        if _remove(server_record_file(registry_dir)):
            click.echo(_server_gone(_server), err=True)
        _server = None
    _pids = []
    for _path, _session in _session_records(registry_dir).items():
        if not _session_is_live(_session, _server) and _remove(_path):
            _pids.append(_session.get("pid", "unknown"))
    if _pids:
        click.echo(_sessions_gone(_pids), err=True)
    _unfinished = [
        _path for _path in _unfinished_records(registry_dir) if _remove(_path)
    ]
    if _unfinished:
        click.echo(_unfinished_gone(_unfinished), err=True)
    return _server


# What is said as the record of a registry that has stopped is removed
def _server_gone(server: typing.Dict) -> str:
    _said = (
        f"The local registry recorded as running at {server['uri']} since "
        f"{server.get('started', 'an unknown time')} has stopped outside "
        "'fair' - after a restart, say - and its record is removed."
    )
    if server.get("holder") == HELD_BY_USER:
        _said += (
            " It was started by 'fair registry start': start it again to keep "
            "it running."
        )
    return _said


# What is said as the records of sessions their processes did not close are
# removed
def _sessions_gone(pids: typing.List) -> str:
    if len(pids) == 1:
        return f"A session not closed by its process, {pids[0]}, is removed."
    return (
        f"{len(pids)} sessions not closed by their processes are removed: "
        f"processes {', '.join(str(_pid) for _pid in pids)}."
    )


# The files of records whose writing was cut short. Every record is written,
# and these are looked for, under the install's lock, so none found is being
# written; a record written without the lock could be removed half-way
def _unfinished_records(registry_dir: str) -> typing.List[str]:
    _dir = records_directory(registry_dir)
    if not os.path.isdir(_dir):
        return []
    return sorted(
        os.path.join(_dir, _name)
        for _name in os.listdir(_dir)
        if _name.endswith(".tmp")
    )


# What is said as the files of records whose writing was cut short are removed
def _unfinished_gone(paths: typing.List[str]) -> str:
    _names = ", ".join(os.path.basename(_path) for _path in paths)
    if len(paths) == 1:
        return f"A record file left half-written is removed: {_names}."
    return (
        f"{len(paths)} record files left half-written are removed: {_names}."
    )


# Record the server this install runs on this host, and who holds it
def _record_server(registry_dir: str, uri: str, holder: str) -> typing.Dict:
    _server = {
        "host": _host(),
        "uri": uri,
        "holder": holder,
        "started": datetime.datetime.now().isoformat(timespec="seconds"),
    }
    _write_record(server_record_file(registry_dir), _server)
    return _server


# The address and port of a registry endpoint, 8000 where it names none
def _address_and_port(uri: str) -> typing.Tuple[str, int]:
    _parsed = urllib.parse.urlparse(uri)
    return _parsed.hostname or "127.0.0.1", _parsed.port or 8000


def open_session(
    registry_dir: str = None, uri: str = None, pid: int = None
) -> typing.Dict[str, str]:
    """Hold the local registry for a process, starting it where none runs

    The session lasts until it is closed, or until its process ends. A
    registry started here is held by the sessions, and stopped when the last
    of them closes; this install's registry found running unrecorded is used
    as it is, and no session stops it.

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    uri : str, optional
        endpoint at which to start the registry where none runs, by default
        the configured one
    pid : int, optional
        process that holds the session, by default this one

    Returns
    -------
    typing.Dict[str, str]
        the session's id ("session"), the registry's endpoint
        ("registry_url") and the file holding its token ("token_file")
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    uri = uri or fdp_conf.get_local_uri()
    pid = pid or os.getpid()
    _started = _process_start(pid)
    with _locked(registry_dir):
        # A registry found running may fail, where one started here was
        # checked as it started
        _server = _current_server(registry_dir)
        if _server:
            fdp_serv._refuse_failing(_server["uri"], registry_dir)
        else:
            _here = _server_here(registry_dir, uri)
            if _here is False:
                raise _not_this_install(registry_dir, uri)
            if _here:
                fdp_serv._refuse_failing(uri, registry_dir)
                _server = _record_server(registry_dir, uri, HELD_BY_NOBODY)
            else:
                _address, _port = _address_and_port(uri)
                fdp_serv.launch_server(
                    port=_port, address=_address, registry_dir=registry_dir
                )
                _server = _record_server(registry_dir, uri, HELD_BY_SESSIONS)
        _id = uuid.uuid4().hex
        _write_record(
            os.path.join(records_directory(registry_dir), f"{_id}.json"),
            {
                "id": _id,
                "pid": pid,
                "process_started": _started,
                "host": _host(),
                "opened": datetime.datetime.now().isoformat(timespec="seconds"),
            },
        )
    logger.debug("Opened session %s for process %s", _id, pid)
    return {
        "session": _id,
        "registry_url": _server["uri"],
        "token_file": os.path.join(registry_dir, "token"),
    }


def close_session(session_id: str, registry_dir: str = None) -> bool:
    """Release a session, stopping a registry a session started if it was the last

    Parameters
    ----------
    session_id : str
        id of the session, as opening it returned
    registry_dir : str, optional
        directory of the registry install, by default the configured one

    Returns
    -------
    bool
        whether the session was open
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    # An id is one open_session made, never a path to elsewhere
    if not re.fullmatch(r"[0-9a-f]{32}", session_id):
        return False
    _path = os.path.join(records_directory(registry_dir), f"{session_id}.json")
    with _locked(registry_dir):
        _was_open = os.path.exists(_path)
        _remove(_path)
        _server = _read_record(server_record_file(registry_dir))
        if not _server or _server.get("host") != _host():
            return _was_open
        _server = _current_server(registry_dir)
        if (
            _server
            and _server["holder"] == HELD_BY_SESSIONS
            and not _session_records(registry_dir)
        ):
            logger.debug("Stopping the registry, its last session closed")
            try:
                fdp_serv.stop_server(
                    registry_dir=registry_dir, local_uri=_server["uri"], force=True
                )
            except fdp_exc.FAIRCLIException as e:
                logger.warning("Failed to stop the local registry: %s", e.msg)
            else:
                _remove(server_record_file(registry_dir))
    return _was_open


@contextlib.contextmanager
def session(registry_dir: str = None, uri: str = None) -> typing.Iterator[str]:
    """Hold a session on the local registry for the length of a block

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    uri : str, optional
        endpoint at which to start the registry where none runs, by default
        the configured one

    Yields
    ------
    str
        the registry's endpoint
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    _session = open_session(registry_dir, uri)
    try:
        yield _session["registry_url"]
    finally:
        close_session(_session["session"], registry_dir)


def start_for_user(
    port: int,
    address: str = "127.0.0.1",
    registry_dir: str = None,
    verbose: bool = False,
) -> str:
    """Start the local registry for the user, or adopt this install's running one

    Either way it runs from then until 'fair registry stop'.

    Parameters
    ----------
    port : int
        port on which to start the registry
    address : str, optional
        address on which to start the registry, by default 127.0.0.1
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    verbose : bool, optional
        show the registry's start output, by default False

    Returns
    -------
    str
        the endpoint of the registry, started or adopted
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    _uri = f"http://{address}:{port}/api/"
    with _locked(registry_dir):
        _server = _current_server(registry_dir)
        if _server and _server["holder"] == HELD_BY_USER:
            raise fdp_exc.UnexpectedRegistryServerState("Server already running.")
        if _server:
            _write_record(
                server_record_file(registry_dir), {**_server, "holder": HELD_BY_USER}
            )
            return _server["uri"]
        _here = _server_here(registry_dir, _uri)
        if _here is False:
            raise _not_this_install(registry_dir, _uri)
        if not _here:
            fdp_serv.launch_server(
                port=port, address=address, registry_dir=registry_dir, verbose=verbose
            )
        _record_server(registry_dir, _uri, HELD_BY_USER)
    return _uri


def stop_for_user(registry_dir: str = None, force: bool = False) -> None:
    """Stop the local registry at the user's request

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    force : bool, optional
        stop it although sessions are using it, by default False
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    with _locked(registry_dir):
        _server = _current_server(registry_dir)
        _uri = _server["uri"] if _server else fdp_conf.get_local_uri()
        _here = _server_here(registry_dir, _uri)
        if _here is False:
            raise _not_this_install(registry_dir, _uri)
        if not _here:
            raise fdp_exc.UnexpectedRegistryServerState("Server is not running.")
        _sessions = list(_session_records(registry_dir).values())
        if _sessions and not force:
            raise fdp_exc.UnexpectedRegistryServerState(
                "Cannot stop registry, a process may still be running: "
                + _describe_sessions(_sessions),
                hint="You can force stop using '--force'",
            )
        fdp_serv.stop_server(registry_dir=registry_dir, local_uri=_uri, force=True)
        _remove_records(registry_dir)


def stop_for_removal(registry_dir: str) -> None:
    """Stop the local registry before its install is removed

    Nothing is stopped while a session uses it, or while it is recorded as
    running on another host, and those are refused, so that the caller
    removes nothing.

    Parameters
    ----------
    registry_dir : str
        directory of the registry install
    """
    if not os.path.isdir(registry_dir):
        return
    with _locked(registry_dir):
        _server = _current_server(registry_dir)
        _sessions = list(_session_records(registry_dir).values())
        if _sessions:
            raise fdp_exc.RegistryError(
                f"The local registry installed in '{registry_dir}' is in use: "
                + _describe_sessions(_sessions),
                hint="Wait for them to finish, or stop the registry with "
                "'fair registry stop --force', then try again",
            )
        _uri = _server["uri"] if _server else fdp_conf.get_local_uri()
        if _server_here(registry_dir, _uri):
            fdp_serv.stop_server(
                registry_dir=registry_dir, local_uri=_uri, force=True
            )
        _remove_records(registry_dir)


# Remove the server record and every session record, the server having stopped
def _remove_records(registry_dir: str) -> None:
    _remove(server_record_file(registry_dir))
    for _path in _session_records(registry_dir):
        _remove(_path)


# The processes of sessions, as a user would know them, with a word on one
# that is only taken to be running
def _describe_sessions(sessions: typing.List[typing.Dict]) -> str:
    return ", ".join(
        f"process {_session.get('pid')} ({_command(_session.get('pid'))}"
        + (
            f"; {START_UNREADABLE}"
            if _start_unreadable(_session.get("pid"))
            else ""
        )
        + ")"
        for _session in sessions
    )


# The command a process is running, as far as it can be read
def _command(pid: typing.Optional[int]) -> str:
    try:
        return " ".join(psutil.Process(pid).cmdline()) or "unknown command"
    except (psutil.Error, TypeError, ValueError):
        return "unknown command"


def live_sessions(registry_dir: str = None) -> typing.List[typing.Dict]:
    """Return the sessions whose processes are using the local registry

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one

    Returns
    -------
    typing.List[typing.Dict]
        the record of each live session
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    _server = _read_record(server_record_file(registry_dir))
    return [
        _session
        for _session in _session_records(registry_dir).values()
        if _session_is_live(_session, _server)
    ]


def status(registry_dir: str = None) -> typing.Dict[str, typing.Any]:
    """Return what the records say of the local registry and its sessions

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one

    Returns
    -------
    typing.Dict[str, typing.Any]
        the server record ("server", None where there is none), whether it
        is on another host ("elsewhere"), the file holding it
        ("record_file"), and the live sessions ("sessions"), each with the
        command its process runs ("command") and whether its start time
        cannot be read ("start_unreadable")
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    _status = {
        "server": None,
        "elsewhere": False,
        "record_file": server_record_file(registry_dir),
        "sessions": [],
    }
    with _locked(registry_dir):
        _server = _read_record(server_record_file(registry_dir))
        if _server and _server.get("host") != _host():
            _status["server"], _status["elsewhere"] = _server, True
        else:
            _status["server"] = _current_server(registry_dir)
        _status["sessions"] = [
            {
                **_session,
                "command": _command(_session.get("pid")),
                "start_unreadable": _start_unreadable(_session.get("pid")),
            }
            for _session in _session_records(registry_dir).values()
        ]
    return _status
