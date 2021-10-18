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

import os
import subprocess
import glob
import shutil
import sys
import pathlib
import enum
import tempfile
import typing
import requests
import stat
import logging

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf
import fair.registry.storage as fdp_store
import fair.registry.requests as fdp_req

DEFAULT_LOCAL_REGISTRY_URL = 'http://localhost:8000/api/'


REGISTRY_INSTALL_URL = "https://data.scrc.uk/static/localregistry.sh"
DEFAULT_REGISTRY_LOCATION = os.path.join(pathlib.Path().home(), fdp_com.FAIR_FOLDER, 'registry')
DEFAULT_LOCAL_REGISTRY_URL = "http://localhost:8000/api/"

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
    if not local_uri:
        local_uri = fdp_conf.get_local_uri()

    try:
        _status_code = requests.get(local_uri).status_code
        assert _status_code == 200
        return True
    except (requests.exceptions.ConnectionError, AssertionError):
        return False


def launch_server(local_uri: str = None, registry_dir: str = None, verbose: bool = False) -> int:
    """Start the registry server.

    Parameters
    ----------
    verbose : bool, optional
        show registry start output, by default False
    """

    if not registry_dir:
        registry_dir = fdp_com.registry_home()

    _server_start_script = os.path.join(
        registry_dir, "scripts", "start_fair_registry"
    )

    if not os.path.exists(_server_start_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_start_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    _cmd = [_server_start_script, '-p', fdp_conf.get_local_port(local_uri)]

    _start = subprocess.Popen(
        _cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=False,
    )

    if verbose:
        for c in iter(lambda: _start.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _start.wait()

    if not check_server_running(local_uri):
        raise fdp_exc.RegistryError(
            "Failed to start local registry, no response from server"
        )


def stop_server(
    registry_dir: str = None, local_uri: str = None,
    force: bool = False, verbose: bool = False
) -> None:
    """Stops the FAIR Data Pipeline local server

    Parameters
    ----------
    force : bool, optional
        whether to force server shutdown if it is being used
    """
    registry_dir = registry_dir or fdp_com.registry_home()
    _session_port_file = os.path.join(registry_dir, 'session_port.log')

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

    if not os.path.exists(_server_stop_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_stop_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

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

def install_registry() -> None:
    """Install the local registry"""
    _logger = logging.getLogger('FAIRDataPipeline.Registry.Install')
    _logger.setLevel(logging.INFO)

    with tempfile.NamedTemporaryFile('w+', suffix='.sh', delete=False) as install_script:
        install_script.write(requests.get(REGISTRY_INSTALL_URL).text)

    os.chmod(install_script.name, os.stat(install_script.name).st_mode | stat.S_IREAD)

    _command = [
        shutil.which('bash'),
        install_script.name
    ]

    with subprocess.Popen(
        _command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        shell=False
    ) as _install:
        for b in _install.stdout:
            if b.strip():
                print(b.strip())

    _install.wait()

    if _install.returncode != 0:
        raise fdp_exc.RegistryError(
            f"Registry installation failed with exit code {_install.returncode}"
        )

    _install_failed: typing.List[bool] = [
        os.path.exists(fdp_com.global_fdpconfig())
        and not os.path.exists(fdp_com.registry_home()),
        not os.path.exists(DEFAULT_REGISTRY_LOCATION)
    ]

    if any(_install_failed):
        raise fdp_exc.RegistryError(
            "Failed to find local registry directory after install."
        )

    _logger.info("Registry installed successfully.")

def uninstall_registry() -> None:
    """Uninstall the local registry from the default location"""

    # First check if the location can be retrieved from a CLI configuration
    # else check the default install location
    if (os.path.exists(fdp_com.global_fdpconfig())
        and os.path.exists(fdp_com.registry_home())):
        shutil.rmtree(fdp_com.registry_home())
    elif os.path.exists(DEFAULT_REGISTRY_LOCATION):
        shutil.rmtree(DEFAULT_REGISTRY_LOCATION)


def update_registry_post_setup(repo_dir: str, global_setup: bool = False) -> None:
    """Add user namespace and file types after CLI setup

    Parameters
    ----------
    repo_dir : str
        current FAIR repository location
    global_setup : bool, optional
        whether this is the first (global) setup or local for a new repository
    """
    _is_running = check_server_running(fdp_conf.get_local_uri())
    # Populate file type table
    if not _is_running:
        launch_server(fdp_conf.get_local_uri())
    
    if global_setup:
        fdp_store.populate_file_type(fdp_conf.get_local_uri())

    # Add author and UserAuthor
    _author_url = fdp_store.store_user(repo_dir, fdp_conf.get_local_uri())

    try:
        _admin_url = fdp_req.get(
            fdp_conf.get_local_uri(),
            'users',
            params = {
                'username': 'admin'
            }
        )[0]['url']
    except (KeyError, IndexError):
        raise fdp_exc.RegistryAPICallError(
            "Failed to retrieve 'admin' user from registry database"
        )

    fdp_req.post_else_get(
        fdp_conf.get_local_uri(),
        'user_author',
        data = {
            'user': _admin_url,
            'author': _author_url
        }
    )

    # Only stop the server if it was not running initially
    if not _is_running:
        stop_server()
