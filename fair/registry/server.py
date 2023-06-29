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

"""

__date__ = "2021-06-28"

import enum
import glob
import logging
import os
import platform
import shutil
import subprocess
import sys
import typing

import git
import requests
import yaml

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.utilities as fdp_util
import fair.virtualenv as fdp_env


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
        _status_code = requests.get(local_uri).status_code
        assert _status_code == 200
        return True
    except (requests.exceptions.ConnectionError, AssertionError):
        return False


def launch_server(
    port: int = 8000, registry_dir: str = None, verbose: bool = False, address: str = "127.0.0.1"
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

    _server_start_script = os.path.join(
        registry_dir, "scripts", "start_fair_registry"
    )

    if platform.system() == "Windows":
        _server_start_script += "_windows.bat"

    if not os.path.exists(_server_start_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_start_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    _cmd = [_server_start_script, "-p", f"{port}", "-a", f"{address}"]

    os.environ["FAIR_ALLOWED_HOSTS"] = address if "FAIR_ALLOWED_HOSTS" not in os.environ else os.environ["FAIR_ALLOWED_HOSTS"] + f",{address}"

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

    local_uri = fdp_conf.update_local_port()

    if not check_server_running(local_uri):
        raise fdp_exc.RegistryError(
            "Failed to start local registry, no response from server"
        )


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

    # If there are no session cache files shut down server
    _run_files = glob.glob(os.path.join(fdp_com.session_cache_dir(), "*.run"))
    if not force and _run_files:
        raise fdp_exc.RegistryError(
            "Could not stop registry server, processes still running."
        )

    _server_stop_script = os.path.join(
        registry_dir, "scripts", "stop_fair_registry"
    )

    if platform.system() == "Windows":
        _server_stop_script += "_windows.bat"

    if not os.path.exists(_server_stop_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_stop_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    logger.debug("Stopping local registry server.")

    _stop = subprocess.Popen(
        _server_stop_script,
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

    _migration_files = glob.glob(
        os.path.join(install_dir, "*", "migrations", "*.py*")
    )

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


def install_registry(
    repository: str = fdp_com.FAIR_REGISTRY_REPO,
    reference: str = None,
    install_dir: str = None,
    silent: bool = False,
    force: bool = False,
    venv_dir: str = None,
) -> str:

    logger = logging.getLogger("FAIRDataPipeline.Server")

    if not install_dir:
        install_dir = fdp_com.DEFAULT_REGISTRY_LOCATION

    if os.path.exists(install_dir):
        raise fdp_exc.RegistryError(
            f"Local registry is already installed in {install_dir}"
        )

    logger.debug("Installing registry to '%s'", install_dir)

    # In case registry installation occurs before setup
    os.makedirs(fdp_com.global_config_dir(), exist_ok=True)

    logger.debug("Updating registry path in global CLI configuration")
    if os.path.exists(fdp_com.global_fdpconfig()):
        _glob_conf = fdp_util.flatten_dict(fdp_conf.read_global_fdpconfig())
        _glob_conf["registries.local.directory"] = install_dir

        with open(fdp_com.global_fdpconfig(), "w") as out_conf:
            yaml.dump(fdp_util.expand_dict(_glob_conf), out_conf)

    if force:
        logger.debug("Removing existing installation at '%s'", install_dir)
        if platform.system() == "Windows":
            fdp_com.set_file_permissions(install_dir)
        shutil.rmtree(install_dir, onerror=fdp_com.remove_readonly)

    logger.debug("Creating directories for installation if they do not exist")

    os.makedirs(os.path.dirname(install_dir), exist_ok=True)

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
            f"No such head or tag '{reference}' in registry repository"
            f"Candidates are {_candidates}"
        )
    else:
        _repo.git.checkout(reference)

    if not venv_dir:
        venv_dir = os.path.join(install_dir, "venv")

        _venv = fdp_env.FAIREnv(with_pip=True)

        _venv.create(venv_dir)

    _python_exe = "python.exe" if platform.system() == "Windows" else "python"
    _binary_loc = "Scripts" if platform.system() == "Windows" else "bin"

    _venv_python = shutil.which(
        _python_exe, path=os.path.join(venv_dir, _binary_loc)
    )

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

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "whitenoise"],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    _requirements = os.path.join(install_dir, "local-requirements.txt")

    if not os.path.exists(_requirements):
        raise FileNotFoundError(f"Expected file '{_requirements}'")

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "-r", _requirements],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    rebuild_local(_venv_python, install_dir, silent)

    return reference


def uninstall_registry() -> None:
    """Uninstall the local registry from the default location"""
    logger = logging.getLogger("FAIRDataPipeline.Server")
    # First check if the location can be retrieved from a CLI configuration
    # else check the default install location
    if os.path.exists(fdp_com.global_fdpconfig()) and os.path.exists(
        fdp_com.registry_home()
    ):
        logger.debug(
            "Uninstalling registry, removing '%s'", fdp_com.registry_home()
        )
        # On windows file permisions need to be set prior to removing the directory
        if platform.system() == "Windows":
            fdp_com.set_file_permissions(fdp_com.registry_home())
        shutil.rmtree(fdp_com.registry_home(), onerror=fdp_com.remove_readonly)
    elif os.path.exists(fdp_com.DEFAULT_REGISTRY_LOCATION):
        logger.debug(
            "Uninstalling registry, removing '%s'",
            fdp_com.DEFAULT_REGISTRY_LOCATION,
        )
        # On windows file permisions need to be set prior to removing the directory
        if platform.system() == "Windows":
            fdp_com.set_file_permissions(fdp_com.DEFAULT_REGISTRY_LOCATION)
        shutil.rmtree(fdp_com.DEFAULT_REGISTRY_LOCATION, onerror=fdp_com.remove_readonly)
    else:
        raise fdp_exc.RegistryError(
            "Cannot uninstall registry, no local installation identified"
        )


def update_registry_post_setup(
    repo_dir: str, global_setup: bool = False
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

    _is_running = check_server_running(fdp_conf.get_local_uri())

    if not _is_running:
        launch_server()

    if global_setup:
        logger.debug("Populating file types")
        fdp_store.populate_file_type(
            fdp_conf.get_local_uri(), fdp_req.local_token()
        )

    logger.debug("Adding 'author' and 'UserAuthor' entries if not present")
    # Add author and UserAuthor
    _author_url = fdp_store.store_user(
        repo_dir, fdp_conf.get_local_uri(), fdp_req.local_token()
    )

    try:
        _admin_url = fdp_req.get(
            fdp_conf.get_local_uri(),
            "users",
            fdp_req.local_token(),
            params={"username": "admin"},
        )[0]["url"]
    except (KeyError, IndexError) as e:
        raise fdp_exc.RegistryError(
            "Failed to retrieve 'admin' user from registry database"
        ) from e

    fdp_req.post_else_get(
        fdp_conf.get_local_uri(),
        "user_author",
        fdp_req.local_token(),
        data={"user": _admin_url, "author": _author_url},
    )

    # Only stop the server if it was not running initially
    if not _is_running:
        stop_server()