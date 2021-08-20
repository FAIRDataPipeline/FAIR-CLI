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
import re
import subprocess
import requests
import glob
import sys
import enum

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.configuration as fdp_conf


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


def check_server_running(port: int = None) -> bool:
    """Check the state of server

    Parameters
    ----------
    port : int
        port local registry is running on

    Returns
    -------
    bool
        whether server is running
    """
    if not port:
        port = fdp_conf.get_local_port()
    _local_remote = f'http://localhost:{port}/api/'

    try:
        _status_code = requests.get(_local_remote).status_code
        assert _status_code == 200
        return True
    except (requests.exceptions.ConnectionError, AssertionError):
        return False


def launch_server(registry_dir: str = None, verbose: bool = False) -> int:
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

    _cmd = [_server_start_script, '-p', fdp_conf.get_local_port()]

    _start = subprocess.Popen(
        _cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )

    if verbose:
        for c in iter(lambda: _start.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _start.wait()

    if not check_server_running():
        raise fdp_exc.RegistryError(
            "Failed to start local registry, no response from server"
        )


def stop_server(
    force: bool = False, verbose: bool = False
) -> None:
    """Stops the FAIR Data Pipeline local server

    Parameters
    ----------
    force : bool, optional
        whether to force server shutdown if it is being used
    """
    _session_port_file = os.path.join(fdp_com.registry_home(), 'session_port.log')

    if not os.path.exists(_session_port_file):
        raise fdp_exc.FileNotFoundError(
            "Failed to retrieve current session port from file "
            f"'{_session_port_file}', file does not exist"
        )
    
    _port = int(open(_session_port_file).read().strip())

    # If there are no session cache files shut down server
    _run_files = glob.glob(os.path.join(fdp_com.session_cache_dir(), "*.run"))
    if not force and _run_files:
        raise fdp_exc.RegistryError(
            "Could not stop registry server, processes still running."
        )

    _server_stop_script = os.path.join(
        fdp_com.registry_home(), "scripts", "stop_fair_registry"
    )

    if not os.path.exists(_server_stop_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_stop_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    _stop = subprocess.Popen(
        _server_stop_script,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )

    if verbose:
        for c in iter(lambda: _stop.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _stop.wait()

    if check_server_running(_port):
        raise fdp_exc.RegistryError("Failed to stop registry server.")

def install_registry() -> None:
    os.system(
    "/bin/bash -c \"$(curl -fsSL https://data.scrc.uk/static/localregistry.sh)\""
    )
