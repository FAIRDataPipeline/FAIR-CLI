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
import requests
import glob
import sys
import enum

import fair.common as fdp_com
import fair.exceptions as fdp_exc


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


def check_server_running(local_remote: str) -> bool:
    """Check the state of server

    Parameters
    ----------
    local_remote : str
        URL of local registry server

    Returns
    -------
    bool
        whether server is running
    """
    try:
        assert requests.get(local_remote).status_code == 200
        return True
    except (requests.exceptions.ConnectionError, AssertionError):
        return False


def launch_server(local_remote: str, verbose: bool = False) -> None:
    """Start the registry server.

    Parameters
    ----------
    local_remote : str
        URL of local remote registry server
    verbose : bool, optional
        show registry start output, by default False
    """

    _server_start_script = os.path.join(
        fdp_com.REGISTRY_HOME, "scripts", "start_fair_registry"
    )

    if not os.path.exists(_server_start_script):
        raise fdp_exc.RegistryError(
            f"Failed to find local registry executable '{_server_start_script}',"
            " is the FAIR data pipeline properly installed on this system?"
        )

    _start = subprocess.Popen(
        _server_start_script,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )

    if verbose:
        for c in iter(lambda: _start.stdout.read(1), b""):
            sys.stdout.buffer.write(c)

    _start.wait()

    if not check_server_running(local_remote):
        raise fdp_exc.RegistryError(
            "Failed to start local registry, no response from server"
        )


def stop_server(
    local_remote: str, force: bool = False, verbose: bool = False
) -> None:
    """Stops the FAIR Data Pipeline local server

    Parameters
    ----------
    local_remote : str
        URL of local remote registry server
    force : bool, optional
        whether to force server shutdown if it is being used
    """
    # If there are no session cache files shut down server
    if glob.glob(os.path.join(fdp_com.session_cache_dir(), "*.run")):
        if not force:
            raise fdp_exc.RegistryError(
                "Could not stop registry server, processes still running."
            )

    _server_stop_script = os.path.join(
        fdp_com.REGISTRY_HOME, "scripts", "stop_fair_registry"
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

    if check_server_running(local_remote):
        raise fdp_exc.RegistryError("Failed to stop registry server.")
