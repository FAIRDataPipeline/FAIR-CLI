#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Registry Server Control
=======================

Contains methods for controlling the starting and stopping of the local
FAIR Data Pipeline registry via the CLI.

Contents
========

Functions
---------

    check_server_running - confirm that the server is running locally
    reset_registry - empty the local registry's database, keeping its install
    resolve_reference - the name a version of the registry is checked out by
    upgrade_registry - rebuild the local registry at a version, keeping its records

"""

__date__ = "2021-06-28"

import contextlib
import enum
import glob
import logging
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import typing
import urllib.parse

import git
import requests
import yaml

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.registry.database as fdp_db
import fair.registry.lifecycle as fdp_life
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.stores as fdp_stores
import fair.staging as fdp_stage
import fair.utilities as fdp_util
import fair.virtualenv as fdp_env


# How long a check of the registry waits for an answer, so that a port held by
# something that never answers is not waited on for ever
REQUEST_SECONDS = 10


def django_environ(environ: typing.Dict = os.environ):
    _environ = environ.copy()
    _environ["DJANGO_SETTINGS_MODULE"] = "drams.local-settings"
    _environ["DJANGO_SUPERUSER_USERNAME"] = "admin"
    _environ["DJANGO_SUPERUSER_PASSWORD"] = "admin"
    return _environ


class SwitchMode(enum.Enum):
    """Server access mode

    The server can be launched either by the user or by the CLI itself. In the
    latter case it will be shut down after use if no other instances are using it.
    However if the user manually starts it themselves then the CLI will not
    alter the state. This class handles all stop/start request possibilities.
    """

    USER_START = 0
    USER_STOP = 1
    CLI = 2
    FORCE_STOP = 3
    NO_SERVER = 4


def check_server_running(local_uri: str = None) -> bool:
    """Check the state of server

    Parameters
    ----------
    local_uri : str
        local registyr endpoint

    Returns
    -------
    bool
        whether server is running
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")

    if not local_uri:
        local_uri = fdp_conf.get_local_uri()

    logger.debug("Checking if server is running on '%s'", local_uri)

    try:
        _status_code = requests.get(
            local_uri, timeout=REQUEST_SECONDS, proxies=fdp_req.proxies_for(local_uri)
        ).status_code
        assert _status_code == 200
        return True
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        AssertionError,
    ):
        return False


# Whether the server at a registry endpoint is the registry a token belongs
# to: a registry refuses a token that is not its own
def _accepts_token(uri: str, token: str) -> bool:
    _response = requests.get(
        f"{uri}users/",
        headers={"Authorization": f"token {token}"},
        timeout=REQUEST_SECONDS,
        proxies=fdp_req.proxies_for(uri),
    )
    return _response.status_code not in (401, 403)


# Refuse a registry that answers and fails: one whose list of users is an
# internal error (HTTP 500) cannot serve a command, and its log says why
def _refuse_failing(uri: str, registry_dir: str) -> None:
    _token = fdp_req.local_token(registry_dir)
    _response = requests.get(
        f"{uri}users/",
        headers={"Authorization": f"token {_token}"},
        timeout=REQUEST_SECONDS,
        proxies=fdp_req.proxies_for(uri),
    )
    if _response.status_code == 500:
        raise fdp_exc.RegistryError(
            f"The local registry at '{uri}' answers with an internal error "
            "(HTTP 500)",
            hint=f"Its log, '{os.path.join(registry_dir, 'output.log')}', "
            "says why; 'fair registry stop' and 'fair registry start' start "
            "it again",
        )


# Whether the server at a registry endpoint answers within the time given,
# asked once a second
def _wait_for_server(uri: str, seconds: int = 30) -> bool:
    for _ in range(seconds):
        if check_server_running(uri):
            return True
        time.sleep(1)
    return False


# Write a registry's token where its start script did not, by the registry's
# own command, as the script runs it
def _write_missing_token(registry_dir: str) -> None:
    try:
        fdp_req.local_token(registry_dir)
        return
    except fdp_exc.FileNotFoundError:
        pass
    _token_file = os.path.join(registry_dir, "token")
    _bin, _exe = ("bin", "python")
    if platform.system() == "Windows":
        _bin, _exe = ("Scripts", "python.exe")
    _token = subprocess.check_output(
        [
            os.path.join(registry_dir, "venv", _bin, _exe),
            os.path.join(registry_dir, "manage.py"),
            "get_token",
        ],
        env=django_environ(),
        cwd=registry_dir,
        text=True,
    )
    with open(_token_file, "w", encoding="utf-8") as token_f:
        token_f.write(f"{_token.strip()}\n")


def launch_server(
    port: int = 8000,
    registry_dir: str = None,
    verbose: bool = False,
    address: str = "127.0.0.1",
) -> int:
    """Start the registry server.

    Parameters
    ----------
    verbose : bool, optional
        show registry start output, by default False
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")

    if not registry_dir:
        registry_dir = fdp_com.registry_home()

    _server_start_script = os.path.join(registry_dir, "scripts", "start_fair_registry")

    if platform.system() == "Windows":
        _server_start_script += "_windows.bat"

    if not os.path.exists(_server_start_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_start_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    _cmd = [_server_start_script, "-p", f"{port}", "-a", f"{address}"]

    os.environ["FAIR_ALLOWED_HOSTS"] = (
        address
        if "FAIR_ALLOWED_HOSTS" not in os.environ
        else os.environ["FAIR_ALLOWED_HOSTS"] + f",{address}"
    )

    logger.debug("Launching server with command '%s'", " ".join(_cmd))

    _start = subprocess.Popen(
        _cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
    )

    if verbose and platform.system() != "Windows":
        for c in iter(lambda: _start.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _start.wait()

    local_uri = fdp_conf.update_local_port(registry_dir)

    # The start script waits for the server and writes its token, but on
    # Windows with no console its waits return at once, and it can give up
    # before either: both are waited for here
    if not _wait_for_server(local_uri):
        raise fdp_exc.RegistryError(
            "Failed to start local registry, no response from server"
        )
    _write_missing_token(registry_dir)

    # A response does not show that this registry started: another one that
    # holds the port answers too, while this one stops for want of it, and
    # the start script's status does not tell the two apart
    if not _accepts_token(local_uri, fdp_req.local_token(registry_dir)):
        raise fdp_exc.RegistryError(
            f"Failed to start local registry: the server at '{local_uri}' "
            f"is not the one installed in '{registry_dir}', as it refuses "
            "that registry's token",
            hint="Another server may hold the port: stop it, or start this "
            "registry on another port with 'fair registry start --port'",
        )
    _refuse_failing(local_uri, registry_dir)


def stop_server(
    registry_dir: str = None,
    local_uri: str = None,
    force: bool = False,
    verbose: bool = False,
) -> None:
    """Stops the FAIR Data Pipeline local server

    Parameters
    ----------
    force : bool, optional
        whether to force server shutdown if it is being used
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")

    registry_dir = registry_dir or fdp_com.registry_home()
    _session_port_file = fdp_com.registry_session_port_file(registry_dir)

    if not os.path.exists(_session_port_file):
        raise fdp_exc.FileNotFoundError(
            "Failed to retrieve current session port from file "
            f"'{_session_port_file}', file does not exist"
        )

    if not force and fdp_life.live_sessions(registry_dir):
        raise fdp_exc.RegistryError(
            "Could not stop registry server, processes still running."
        )

    _server_stop_script = os.path.join(registry_dir, "scripts", "stop_fair_registry")

    if platform.system() == "Windows":
        _server_stop_script += "_windows.bat"

    if not os.path.exists(_server_stop_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_stop_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    logger.debug(f"Stopping local registry server with '{_server_stop_script}'.")

    _stop = subprocess.Popen(
        [_server_stop_script, ""],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
    )

    if verbose:
        for c in iter(lambda: _stop.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _stop.wait()

    if check_server_running(local_uri):
        raise fdp_exc.RegistryError("Failed to stop registry server.")


def rebuild_local(python: str, install_dir: str = None, silent: bool = False):
    """Rebuild the local Django registry

    Parameters
    ----------
    python : str
        python binary of virtual environment
    install_dir : str, optional
        location of registry installation
    silent : bool, optional
        run in silent mode, by default False
    """

    logger = logging.getLogger("FAIRDataPipeline.Server")
    logger.debug("Rebuilding local registry")
    if not install_dir:
        install_dir = fdp_com.DEFAULT_REGISTRY_LOCATION

    _migration_files = glob.glob(os.path.join(install_dir, "*", "migrations", "*.py*"))

    logger.debug("Removing migration files: %s", _migration_files)

    for mf in _migration_files:
        os.remove(mf)

    _db_file = os.path.join(install_dir, "db.sqlite3")

    logger.debug("Removing SQL database file")

    if os.path.exists(_db_file):
        os.remove(_db_file)

    _manage = os.path.join(install_dir, "manage.py")

    _sub_cmds = [
        ("makemigrations", "custom_user"),
        ("makemigrations", "data_management"),
        ("migrate",),
        (
            "graph_models",
            "data_management",
            "--arrow-shape",
            "crow",
            "-x",
            '"BaseModel,DataObject,DataObjectVersion"',
            "-E",
            "-o",
            os.path.join(install_dir, "schema.dot"),
        ),
        ("collectstatic", "--noinput"),
        ("createsuperuser", "--noinput"),
    ]

    logger.debug("Running Django migration commands as setup")

    for sub in _sub_cmds:
        subprocess.check_call(
            [python, _manage, *sub],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None,
            env=django_environ(),
        )

    if shutil.which("dot"):
        logger.debug("Creating schema diagram")
        subprocess.check_call(
            [
                shutil.which("dot"),
                os.path.join(install_dir, "schema.dot"),
                "-Tsvg",
                "-o",
                os.path.join(install_dir, "static", "images", "schema.svg"),
            ],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None,
        )


def _remove_directory(directory: str) -> None:
    """Remove a directory and everything in it"""
    if platform.system() == "Windows":
        fdp_com.set_file_permissions(directory)
    shutil.rmtree(directory, onerror=fdp_com.remove_readonly)


@contextlib.contextmanager
def _replacing_directory(directory: str) -> typing.Iterator[typing.Optional[str]]:
    """Make a directory afresh in the block, keeping any old one until it works

    The old directory is moved aside beside it, so the move is a rename, and
    the block is given where, None where there was none. On any failure,
    Ctrl-C included, what the block made is removed and the old one put back;
    on success the old one is deleted.
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")
    directory = os.path.abspath(directory)
    _aside = None
    _old = None
    if os.path.exists(directory):
        _aside = tempfile.mkdtemp(
            prefix=f".{os.path.basename(directory)}-replaced-",
            dir=os.path.dirname(directory),
        )
        _old = os.path.join(_aside, os.path.basename(directory))
        try:
            os.rename(directory, _old)
        except OSError as e:
            os.rmdir(_aside)
            raise fdp_exc.RegistryError(
                f"Cannot move the registry at '{directory}' aside to "
                f"replace it: {e}",
                hint="If the registry is running, stop it with "
                "'fair registry stop' and try again",
            ) from e
        logger.debug("Moved the existing installation to '%s'", _old)
    try:
        yield _old
    except BaseException:
        if os.path.exists(directory):
            _remove_directory(directory)
        if _aside:
            try:
                os.rename(_old, directory)
                os.rmdir(_aside)
                logger.debug(
                    "Restored the existing installation at '%s'", directory
                )
            except OSError:
                logger.error(
                    "The registry that was installed at '%s' could not be "
                    "put back, and is at '%s'",
                    directory,
                    _old,
                )
        raise
    if _aside:
        _remove_directory(_aside)


def install_registry(
    repository: str = fdp_com.FAIR_REGISTRY_REPO,
    reference: str = None,
    install_dir: str = None,
    silent: bool = False,
    force: bool = False,
    venv_dir: str = None,
) -> str:

    logger = logging.getLogger("FAIRDataPipeline.Server")

    # The registry the configuration names, where one is installed, so that a
    # reinstall replaces it wherever it is
    if not install_dir:
        install_dir = fdp_com.registry_home()

    # Find a Python for the registry before changing anything on disk
    _venv_cmd = None
    if not venv_dir:
        venv_dir = os.path.join(install_dir, "venv")
        _venv_cmd = fdp_env.venv_command(venv_dir)

    if os.path.exists(install_dir) and not force:
        raise fdp_exc.RegistryError(
            f"Local registry is already installed in {install_dir}",
            hint="To change its version keeping its records, run 'fair registry "
            "upgrade --version <version>'; to replace it, its records lost, run "
            "'fair registry install --force'",
        )

    logger.debug("Installing registry to '%s'", install_dir)

    # In case registry installation occurs before setup
    os.makedirs(fdp_com.global_config_dir(), exist_ok=True)

    logger.debug("Creating directories for installation if they do not exist")

    os.makedirs(os.path.dirname(install_dir), exist_ok=True)

    with _replacing_directory(install_dir):
        reference = _build_registry(
            repository, reference, install_dir, silent, venv_dir, _venv_cmd
        )

    logger.debug("Updating registry path in global CLI configuration")
    if os.path.exists(fdp_com.global_fdpconfig()):
        _glob_conf = fdp_util.flatten_dict(fdp_conf.read_global_fdpconfig())
        _glob_conf["registries.local.directory"] = install_dir

        with open(fdp_com.global_fdpconfig(), encoding="utf-8", mode="w") as out_conf:
            yaml.dump(fdp_util.expand_dict(_glob_conf), out_conf)

    return reference


def _build_registry(
    repository: str,
    reference: typing.Optional[str],
    install_dir: str,
    silent: bool,
    venv_dir: str,
    venv_cmd: typing.Optional[typing.List[str]],
) -> str:
    """Clone the registry at a reference, build its environment and database

    Returns the reference checked out: the one given, else the newest tag.
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")

    _repo = git.Repo.clone_from(repository, install_dir)

    logger.debug("Retrieving the reference specified from the repository")

    # If no reference is specified, use the latest tag for the registry
    if not reference:
        if not _repo.tags:
            raise fdp_exc.RegistryError(
                f"Expected tags in local registry repository '{repository}', "
                "but found none"
            )
        _tags = sorted(_repo.tags, key=lambda t: t.commit.committed_datetime)
        reference = _tags[-1].name

    logger.debug("Using reference '%s' for registry checkout", reference)

    _candidates = [t.name for t in _repo.tags + _repo.heads + _repo.remote().refs]

    if reference not in _candidates:
        raise fdp_exc.RegistryError(
            f"No such head or tag '{reference}' in registry repository. "
            f"Candidates are {_candidates}"
        )
    else:
        _repo.git.checkout(reference)

    if venv_cmd:
        logger.debug("Creating virtual environment: %s", venv_cmd)
        subprocess.check_call(venv_cmd)

    _python_exe = "python.exe" if platform.system() == "Windows" else "python"
    _binary_loc = "Scripts" if platform.system() == "Windows" else "bin"

    _venv_python = shutil.which(_python_exe, path=os.path.join(venv_dir, _binary_loc))

    if not _venv_python:
        raise FileNotFoundError(
            f"Failed to find binary '{_python_exe}' in location '{venv_dir}"
        )

    logger.debug("Installing requirements")

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "--upgrade", "pip", "wheel"],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    # subprocess.check_call(
    #     [_venv_python, "-m", "pip", "install", "whitenoise"],
    #     shell=False,
    #     stdout=subprocess.DEVNULL if silent else None,
    # )

    _requirements = os.path.join(install_dir, "requirements.txt")

    if not os.path.exists(_requirements):
        raise FileNotFoundError(f"Expected file '{_requirements}'")

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "-r", _requirements],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    rebuild_local(_venv_python, install_dir, silent)

    return reference


# The tags and branches of a git repository, read from it without cloning it
def _remote_references(
    repository: str,
) -> typing.Tuple[typing.Set[str], typing.Set[str]]:
    try:
        _listing = git.cmd.Git().ls_remote("--heads", "--tags", repository)
    except git.exc.GitCommandError as e:
        raise fdp_exc.RegistryError(
            f"The registry's repository '{repository}' cannot be read: {e}",
            hint="Check the network connection, then try again",
        ) from e
    _tags, _branches = set(), set()
    for _line in _listing.splitlines():
        _ref = _line.split("\t")[-1]
        if _ref.startswith("refs/tags/"):
            _tags.add(_ref[len("refs/tags/") :].removesuffix("^{}"))
        elif _ref.startswith("refs/heads/"):
            _branches.add(_ref[len("refs/heads/") :])
    return _tags, _branches


# A tag in the order of its numbers, so that v1.10.0 comes after v1.9.0
def _tag_order(tag: str) -> typing.List:
    return [
        (0, int(_part)) if _part.isdigit() else (1, _part)
        for _part in re.split(r"(\d+)", tag)
    ]


def resolve_reference(
    reference: str, repository: str = fdp_com.FAIR_REGISTRY_REPO
) -> str:
    """Return the name a version of the registry is checked out by

    It is looked for in the repository before anything is stopped, removed or
    cloned, and refused where the repository has no such tag or branch. A tag
    is checked out by its name, and a branch by its remote's name,
    'origin/<branch>', whether it is given with that prefix or without.

    Parameters
    ----------
    reference : str
        a tag or branch of the registry's repository
    repository : str, optional
        the registry's repository, by default its GitHub repository

    Returns
    -------
    str
        the name to check it out by
    """
    _tags, _branches = _remote_references(repository)
    if reference in _tags:
        return reference
    _branch = reference
    if _branch.startswith("origin/"):
        _branch = _branch[len("origin/") :]
    if _branch in _branches:
        return f"origin/{_branch}"
    _newest = sorted(_tags, key=_tag_order)[-5:]
    raise fdp_exc.RegistryError(
        f"No tag or branch '{reference}' in the registry's repository "
        f"'{repository}'",
        hint=f"Give a tag, such as {', '.join(reversed(_newest))}, or a branch's "
        "name",
    )


# Bring a database up to the install's code where the registry's tables are
# already the same: only Django's own applications can have migrations to apply
def _migrate(python: str, registry_dir: str, silent: bool) -> None:
    subprocess.check_call(
        [python, os.path.join(registry_dir, "manage.py"), "migrate"],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
        env=django_environ(),
        cwd=registry_dir,
    )


def upgrade_registry(
    reference: str = None,
    registry_dir: str = None,
    repository: str = fdp_com.FAIR_REGISTRY_REPO,
    silent: bool = False,
) -> typing.Dict[str, typing.Any]:
    """Rebuild the local registry's install at a version, keeping its records

    The version is built in the install's place, the install moved aside, and
    the new tables compared with the installed ones: where they are the same,
    the install's database and token are put in the new install, which
    replaces the old; where they differ, the old is put back as it was. At the
    version installed, the install is built again, as a registry whose
    virtual environment no longer runs needs.

    Nothing is changed while a session uses the registry, while it runs on
    another host, or while another process holds the install. A registry that
    was running is started again, held by the user, whether the upgrade
    succeeds or fails, Ctrl-C included.

    Parameters
    ----------
    reference : str, optional
        a tag or branch of the registry's repository, by default its newest tag
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    repository : str, optional
        the registry's repository, by default its GitHub repository
    silent : bool, optional
        leave out the output of the build, by default False

    Returns
    -------
    typing.Dict[str, typing.Any]
        the version installed ("version"), whether there was a database to
        keep ("kept"), and the endpoint the registry runs at again
        ("restarted", None where it was not running)
    """
    registry_dir = os.path.abspath(registry_dir or fdp_com.registry_home())
    if not os.path.isdir(registry_dir):
        raise fdp_exc.RegistryError(
            f"No local registry is installed in '{registry_dir}'",
            hint="Install one with 'fair registry install'",
        )
    if reference:
        reference = resolve_reference(reference, repository)
    # A Python for the new install is found before anything is stopped
    fdp_env.venv_command(os.path.join(registry_dir, "venv"))
    _result = {"version": None, "kept": False, "restarted": None}
    with fdp_life.held(registry_dir, "fair registry upgrade"):
        # Read before anything is stopped or moved: a database that cannot be
        # read is refused, and one that is not there leaves nothing to keep
        _old_tables = None
        if os.path.exists(fdp_db.database_file(registry_dir)):
            _old_tables = fdp_db.registry_tables(registry_dir)
        _stopped = fdp_life.stop_for_removal(registry_dir)
        try:
            _result["kept"] = _upgrade_in_place(
                registry_dir, _old_tables, repository, reference, silent, _result
            )
        except BaseException:
            # The install is as it was, and so is the registry
            if _stopped:
                _restart(_stopped, registry_dir)
            raise
        if _stopped:
            _result["restarted"] = _restart(_stopped, registry_dir)
    return _result


# Replace an install with a version built in its place, keeping its database
# and token where the version's tables are the ones given; saying whether it
# kept them, and putting the version built into the result
def _upgrade_in_place(
    registry_dir: str,
    old_tables: typing.Optional[typing.Dict],
    repository: str,
    reference: typing.Optional[str],
    silent: bool,
    result: typing.Dict[str, typing.Any],
) -> bool:
    logger = logging.getLogger("FAIRDataPipeline.Server")
    _venv_dir = os.path.join(registry_dir, "venv")
    _venv_cmd = fdp_env.venv_command(_venv_dir)
    with _replacing_directory(registry_dir) as _old:
        result["version"] = _build_registry(
            repository, reference, registry_dir, silent, _venv_dir, _venv_cmd
        )
        if old_tables is None:
            return False
        if fdp_db.registry_tables(registry_dir) != old_tables:
            raise fdp_exc.RegistryError(
                f"The tables of the registry's version '{result['version']}' are "
                "not those of the one installed, so its records cannot be kept, "
                "and the install is left as it was",
                hint="To install that version with an empty registry, run "
                f"'fair registry install --force --version {result['version']}'",
            )
        logger.debug("Keeping the database of the install replaced")
        for _file in (fdp_db.DATABASE_FILE, "token"):
            if os.path.exists(os.path.join(_old, _file)):
                shutil.copy2(
                    os.path.join(_old, _file), os.path.join(registry_dir, _file)
                )
        _migrate(_install_python(registry_dir), registry_dir, silent)
    return True


# Start again, held by the user, a registry stopped at an endpoint
def _restart(uri: str, registry_dir: str) -> str:
    _parsed = urllib.parse.urlparse(uri)
    return fdp_life.start_for_user(
        port=_parsed.port or 8000,
        address=_parsed.hostname or "127.0.0.1",
        registry_dir=registry_dir,
    )


def uninstall_registry() -> None:
    """Uninstall the local registry from the default location"""
    logger = logging.getLogger("FAIRDataPipeline.Server")
    # First check if the location can be retrieved from a CLI configuration
    # else check the default install location
    if os.path.exists(fdp_com.global_fdpconfig()) and os.path.exists(
        fdp_com.registry_home()
    ):
        fdp_life.stop_for_removal(fdp_com.registry_home())
        logger.debug("Uninstalling registry, removing '%s'", fdp_com.registry_home())
        # On windows file permisions need to be set prior to removing the directory
        if platform.system() == "Windows":
            fdp_com.set_file_permissions(fdp_com.registry_home())
        shutil.rmtree(fdp_com.registry_home(), onerror=fdp_com.remove_readonly)
    elif os.path.exists(fdp_com.DEFAULT_REGISTRY_LOCATION):
        fdp_life.stop_for_removal(fdp_com.DEFAULT_REGISTRY_LOCATION)
        logger.debug(
            "Uninstalling registry, removing '%s'",
            fdp_com.DEFAULT_REGISTRY_LOCATION,
        )
        # On windows file permisions need to be set prior to removing the directory
        if platform.system() == "Windows":
            fdp_com.set_file_permissions(fdp_com.DEFAULT_REGISTRY_LOCATION)
        shutil.rmtree(
            fdp_com.DEFAULT_REGISTRY_LOCATION, onerror=fdp_com.remove_readonly
        )
    else:
        raise fdp_exc.RegistryError(
            "Cannot uninstall registry, no local installation identified"
        )


# The Python of an install's virtual environment
def _install_python(registry_dir: str) -> str:
    _bin, _exe = ("bin", "python")
    if platform.system() == "Windows":
        _bin, _exe = ("Scripts", "python.exe")
    return os.path.join(registry_dir, "venv", _bin, _exe)


# Whether an install's Python runs, with Django: a virtual environment whose
# interpreter has been removed, as by an upgrade of the Python it was made
# with, does not
def _python_runs(python: str) -> bool:
    try:
        subprocess.run([python, "-c", "import django"], check=True, capture_output=True)
    except (OSError, subprocess.CalledProcessError):
        return False
    return True


# Build an install's database afresh, as its registry's code defines it, with
# the registry's administrator. The install's migrations are used as they are
def _build_database(python: str, registry_dir: str) -> None:
    _migrate(python, registry_dir, silent=True)
    subprocess.check_call(
        [
            python,
            os.path.join(registry_dir, "manage.py"),
            "createsuperuser",
            "--noinput",
        ],
        shell=False,
        stdout=subprocess.DEVNULL,
        env=django_environ(),
        cwd=registry_dir,
    )


# Give an install an empty database and a new token in the block. Those it has
# are set aside first, and on any failure, Ctrl-C included, what the block made
# is removed and they are put back
@contextlib.contextmanager
def _replacing_database(
    registry_dir: str,
) -> typing.Generator[None, None, None]:
    _names = (fdp_db.DATABASE_FILE, "token")
    _aside = tempfile.mkdtemp(prefix=".reset-", dir=registry_dir)
    try:
        for _name in _names:
            if os.path.exists(os.path.join(registry_dir, _name)):
                os.rename(
                    os.path.join(registry_dir, _name),
                    os.path.join(_aside, _name),
                )
        yield
    except BaseException:
        for _name in _names:
            if os.path.exists(os.path.join(registry_dir, _name)):
                os.remove(os.path.join(registry_dir, _name))
            if os.path.exists(os.path.join(_aside, _name)):
                os.rename(
                    os.path.join(_aside, _name),
                    os.path.join(registry_dir, _name),
                )
        raise
    finally:
        shutil.rmtree(_aside, onerror=fdp_com.remove_readonly)


def reset_registry(
    registry_dir: str = None, repo_dir: str = None, delete_files: bool = False
) -> typing.Dict[str, typing.Any]:
    """Empty the local registry's database, keeping its install

    Nothing is changed while a session uses the registry, while it runs on
    another host, or while another process holds the install, nor where the
    install's Python does not run. A registry that was running is started
    again, held by the user. The file types are registered again, and, in a
    project, the project's user, whose staging is cleared. Where the database
    cannot be built again, or on Ctrl-C, its records are kept, and a registry
    that was running is started again.

    Parameters
    ----------
    registry_dir : str, optional
        directory of the registry install, by default the configured one
    repo_dir : str, optional
        the project whose user is registered again and whose staging is
        cleared, by default none
    delete_files : bool, optional
        also delete the files in the registry's data stores that it holds,
        but its code repositories' folders, by default False

    Returns
    -------
    typing.Dict[str, typing.Any]
        the endpoint the registry runs at again ("restarted", None where it
        was not running), the files deleted ("deleted") and those that could
        not be, each with why ("failed"), the data stores ("stores"), and
        those too wide to touch ("too_wide")
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    if not os.path.isdir(registry_dir):
        raise fdp_exc.RegistryError(
            f"No local registry is installed in '{registry_dir}'",
            hint="Install one with 'fair registry install'",
        )
    _python = _install_python(registry_dir)
    _result = {
        "restarted": None,
        "deleted": 0,
        "failed": [],
        "stores": [],
        "too_wide": [],
    }
    with fdp_life.held(registry_dir, "fair registry reset"):
        # Its environment is checked before anything is stopped
        if not _python_runs(_python):
            raise fdp_exc.RegistryError(
                f"The registry installed in '{registry_dir}' cannot run its "
                f"Python, '{_python}', so it is left as it is",
                hint="Build its environment again with 'fair registry upgrade', "
                "then try again",
            )
        _stopped = fdp_life.stop_for_removal(registry_dir)
        try:
            # The stores are read before the database that names them is
            # emptied
            _stores = fdp_db.local_stores(registry_dir) if delete_files else []
            with _replacing_database(registry_dir):
                _build_database(_python, registry_dir)
                _write_missing_token(registry_dir)
        except BaseException as e:
            # The database is as it was, and so is the registry
            if _stopped:
                _restart(_stopped, registry_dir)
            if isinstance(e, subprocess.CalledProcessError):
                raise fdp_exc.RegistryError(
                    f"The database of the registry installed in "
                    f"'{registry_dir}' could not be built again, so it is left "
                    f"as it was: {e}",
                    hint="Build its environment again with 'fair registry "
                    "upgrade', then try again",
                ) from e
            raise
        # What the project's staging names is gone, whatever happens next
        if repo_dir:
            fdp_stage.Stager(repo_dir).clear()
        if delete_files:
            _held = fdp_stores.held_files(_stores)
            _result["failed"] = fdp_stores.delete_files(
                _held["files"], _held["stores"]
            )
            _result["deleted"] = len(_held["files"]) - len(_result["failed"])
            _result["stores"] = _held["stores"]
            _result["too_wide"] = _held["too_wide"]
        if _stopped:
            _result["restarted"] = _restart(_stopped, registry_dir)
        if repo_dir:
            update_registry_post_setup(repo_dir, True, registry_dir)
        else:
            with fdp_life.session(registry_dir):
                fdp_store.populate_file_type(
                    fdp_conf.get_local_uri(), fdp_req.local_token(registry_dir)
                )
    return _result


def update_registry_post_setup(
    repo_dir: str, global_setup: bool = False, registry_dir: str = None
) -> None:
    """Add user namespace and file types after CLI setup

    Parameters
    ----------
    repo_dir : str
        current FAIR repository location
    global_setup : bool, optional
        whether this is the first (global) setup or local for a new repository
    """
    logger = logging.getLogger("FAIRDataPipeline.Server")

    logger.debug("Updating registry from latest setup")

    # Held for the updates: a registry started for them is stopped after
    with fdp_life.session(registry_dir):
        if global_setup:
            logger.debug("Populating file types")
            fdp_store.populate_file_type(
                fdp_conf.get_local_uri(), fdp_req.local_token(registry_dir)
            )

        logger.debug("Adding 'author' and 'UserAuthor' entries if not present")
        # Add author and UserAuthor
        _author_url = fdp_store.store_user(
            repo_dir, fdp_conf.get_local_uri(), fdp_req.local_token(registry_dir)
        )

        try:
            _admin_url = fdp_req.get(
                fdp_conf.get_local_uri(),
                "users",
                fdp_req.local_token(registry_dir),
                params={"username": "admin"},
            )[0]["url"]
        except (KeyError, IndexError) as e:
            raise fdp_exc.RegistryError(
                "Failed to retrieve 'admin' user from registry database"
            ) from e

        fdp_req.post_else_get(
            fdp_conf.get_local_uri(),
            "user_author",
            fdp_req.local_token(registry_dir),
            data={"user": _admin_url, "author": _author_url},
        )
