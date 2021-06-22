#!/usr/bin/env python
"""
Contains definitions for the synchronisation tool for communication between
remote and local FAIR Data Pipeline registries.

BSD 2-Clause License

Copyright (c) 2021, Scottish COVID Response Consortium
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

import os
import glob
import sys
import hashlib
import rich
import pathlib
import typing
import datetime
import subprocess
import requests

import toml
import yaml
import click
import socket

from fair.templates import config_template, hist_template

__author__ = "Scottish COVID Response Consortium"
__credits__ = ["Nathan Cummings (UKAEA)", "Kristian Zarebski (UKAEA)"]
__license__ = "BSD-2-Clause"
__status__ = "Development"
__copyright__ = "Copyright 2021, FAIR"


__doc__ = """
Manage synchronisation of data and metadata relating to runs using the SCRC FAIR Data Pipeline system.

Classes:

    FAIR

Misc Variables:

    __author__
    __license__
    __credits__
    __status__
    __copyright__

"""


class FAIR:
    """
    A class which provides the main interface for managing runs and data transfer
    between FAIR Data Pipeline registries.

    Attributes
    ----------
    LOCAL_FOLDER
        the name of the directory created in each repository
    GLOBAL_FOLDER
        the address of the global SCRC folder used to store global configs

    Methods
    ----------
    check_is_repo()
    change_staging_state(file_to_stage: str, stage: bool = True)
    remove_file(file_name: str, cached: bool = False)
    show_history()
    run_bash_command(bash_cmd: str = None)
    show_run_log(run_id: str)
    add_remote(url: str, label: str = "origin")
    remove_remote(label: str)
    modify_remote(label: str, url: str)
    purge()
    list_remotes(verbose: bool = False)
    status()
    set_email(email: str)
    set_user(user_name: str)
    initialise()

    """

    LOCAL_FOLDER = ".fair"
    GLOBAL_FOLDER = os.path.join(pathlib.Path.home(), ".scrc")

    def __init__(self) -> None:
        """Initialise instance of FAIR sync tool"""

        # Creates $HOME/.scrc if it does not exist
        if not os.path.exists(self.GLOBAL_FOLDER):
            os.makedirs(self.GLOBAL_FOLDER)

        # Find the nearest '.fair' folder to determine repository root
        self._here = self._find_fair_root()

        # Create staging file, this is used to keep track of which files
        # and runs are to be pushed to the remote
        self._staging_file = (
            os.path.join(self._here, "staging") if self._here else ""
        )

        # Path of local configuration file
        self._local_config_file = os.path.join(self._here, "config")

        # Path of global configuration file
        self._global_config_file = os.path.join(
            self.GLOBAL_FOLDER, "fairconfig"
        )

        # Local data store containing symlinks to data files stored on the system
        self._soft_data_dir = os.path.join(self._here, ".fair", "data")

        # Initialise all configuration/staging status dictionaries
        self._stage_status: typing.Dict[str, typing.Any] = {}
        self._local_config: typing.Dict[str, typing.Any] = {}
        self._global_config: typing.Dict[str, typing.Any] = {}

    def _launch_server(self) -> None:
        """Start the FAIR Data Pipeline local server"""

        # If the registry server is already running ignore or no setup has
        # yet been performed
        if not self._local_config:
            return

        _server_start_script = os.path.join(
            self.GLOBAL_FOLDER, "scripts", "run_scrc_server"
        )

        if not os.path.exists(_server_start_script):
            click.echo(
                "Error: failed to find local registry executable,"
                " is the FAIR data pipeline properly installed on this system?"
            )
            sys.exit(1)

        _start = subprocess.Popen(
            [_server_start_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )

        _start.wait()

        if (
            not requests.get(
                self._local_config["remotes"]["local"]
            ).status_code
            == 200
        ):
            click.echo(
                "Error: Failed to start local registry, no response from server"
            )
            sys.exit(1)

    def _stop_server(self) -> None:
        """Stops the FAIR Data Pipeline local server"""
        # If the local registry server is not running ignore

        _server_stop_script = os.path.join(
            self.GLOBAL_FOLDER, "scripts", "stop_scrc_server"
        )

        if not os.path.exists(_server_stop_script):
            click.echo(
                "Error: failed to find local registry executable,"
                " is the FAIR data pipeline properly installed on this system?"
            )
            sys.exit(1)

        _stop = subprocess.Popen(
            _server_stop_script,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )

        _stop.wait()

        try:
            requests.get(self._local_config["remotes"]["local"])
            click.echo("Error: Failed to stop local registry")
            sys.exit(1)
        except requests.exceptions.ConnectionError:
            pass

    def check_is_repo(self) -> None:
        """Check that the current location is a FAIR repository"""
        if not self._find_fair_root():
            click.echo(
                "fatal: not a fair repository, run 'fair init' to initialise"
            )
            sys.exit(1)

    def _find_fair_root(self) -> str:
        """Locate the .fair folder within the current hierarchy

        Returns
        -------
        str
            absolute path of the .fair folder
        """
        _current_dir = os.getcwd()

        # Keep upward searching until you find '.fair', stop at the level of
        # the user's home directory
        while _current_dir != pathlib.Path.home():
            _fair_dir = os.path.join(_current_dir, self.LOCAL_FOLDER)
            if os.path.exists(_fair_dir):
                return _fair_dir
            _current_dir = pathlib.Path(_current_dir).parent
        return ""

    def __enter__(self) -> None:
        """Method called when using 'with' statement.

        This ensures all configurations and staging statuses are read at the
        start of every session.

        """
        if os.path.exists(self._staging_file):
            self._stage_status = toml.load(self._staging_file)
        if os.path.exists(self._global_config_file):
            self._global_config = toml.load(self._global_config_file)
        if os.path.exists(self._local_config_file):
            self._local_config = toml.load(self._local_config_file)
        self._launch_server()
        return self

    def change_staging_state(
        self, file_to_stage: str, stage: bool = True
    ) -> None:
        """Change the staging status of a given file

        Parameters
        ----------
        file_to_stage : str
            path of the file to be staged/unstaged
        stage : bool, optional
            whether to stage/unstage file, by default True (staged)
        """
        self.check_is_repo()

        if not os.path.exists(file_to_stage):
            click.echo(f"No such file '{file_to_stage}.")
            sys.exit(1)

        # Create a label with which to store the staging status of the given file
        # using its path with respect to the staging status file
        _label = os.path.relpath(
            file_to_stage, os.path.dirname(self._staging_file)
        )

        self._stage_status[_label] = stage

    def remove_file(self, file_name: str, cached: bool = False) -> None:
        """Remove a file from the file system and tracking

        Parameters
        ----------
        file_name : str
            path of file to be removed
        cached : bool, optional
            remove from tracking but not from system, by default False
        """
        self.check_is_repo()
        _label = os.path.relpath(
            file_name, os.path.dirname(self._staging_file)
        )
        if _label in self._stage_status:
            del self._stage_status[_label]
        else:
            click.echo(
                f"File '{file_name}' is not tracked, so will not be removed"
            )
            return

        if not cached:
            os.remove(file_name)

    def show_history(self, length: int = 10) -> None:
        """Show run history, by time sorting log outputs and displaying their metadata"""

        # Read in all log files from the log storage by reverse sorting them
        # by datetime created
        _time_sorted_logs = sorted(
            glob.glob(os.path.join(self._here, "logs", "*")),
            key=os.path.getmtime,
            reverse=True,
        )

        # Iterate through the logs printing out the run author
        for i, log in enumerate(_time_sorted_logs):
            if i == length:
                return
            _run_id = hashlib.sha1(
                open(log).read().encode("utf-8")
            ).hexdigest()
            with open(log) as f:
                _metadata = f.readlines()[:5]
            _user = _metadata[2].split("=")[1]
            _name = _user.split("<")[0].strip()
            _email = _user.replace(_name, "").strip()
            _meta = {
                "sha": _run_id,
                "user": _name,
                "user_email": _email,
                "datetime": _metadata[1].split("=")[1].strip(),
            }
            rich.print(hist_template.render(**_meta))

    def run_bash_command(self, bash_cmd: str = None) -> None:
        """Execute a bash process as part of a model run

        Parameters
        ----------
        cmd_str : str
            command to execute
        """
        # Record the time the run was executed, create a log and both
        # print output and write it to the log file
        _now = datetime.datetime.now()
        _timestamp = _now.strftime("%Y-%m-%d_%H_%M_%S")
        _logs_dir = os.path.join(self._here, "logs")
        if not os.path.exists(_logs_dir):
            os.mkdir(_logs_dir)
        _log_file = os.path.join(_logs_dir, f"run_{_timestamp}.log")
        _cfg_yml = os.path.join(pathlib.Path(self._here).parent, "config.yaml")

        if not os.path.exists(_cfg_yml):
            click.echo(
                "error: expected file 'config.yaml' at head of repository"
            )
            sys.exit(1)

        with open(_cfg_yml) as f:
            _cfg = yaml.load(f, Loader=yaml.SafeLoader)
            assert _cfg

        if "run_metadata" not in _cfg or "script" not in _cfg["run_metadata"]:
            click.echo(
                "error: failed to find executable method in configuration"
            )
            sys.exit(1)

        if bash_cmd:
            _cfg["run_metadata"]["script"] = bash_cmd

            with open(_cfg_yml, "w") as f:
                yaml.dump(_cfg, f)

        _cmd = _cfg["run_metadata"]["script"]

        if not _cmd:
            click.echo("Nothing to run.")
            sys.exit(0)
        _cmd_list = _cfg["run_metadata"]["script"].split()
        _user = self._global_config["user"]["name"]
        _email = self._global_config["user"]["email"]
        _namespace = self._local_config["namespaces"]["output"]

        with open(_log_file, "a") as f:
            _out_str = _now.strftime("%a %b %d %H:%M:%S %Y %Z")
            f.writelines(
                [
                    "--------------------------------\n",
                    f" Commenced = {_out_str}\n",
                    f" Author    = {_user} <{_email}>\n",
                    f" Namespace = {_namespace}\n",
                    "--------------------------------\n",
                ]
            )
        _process = subprocess.Popen(
            _cmd_list,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            text=True,
            shell=False,
        )

        for line in iter(_process.stdout.readline, ""):
            with open(_log_file, "a") as f:
                f.writelines([line])
            click.echo(line, nl=False)
            sys.stdout.flush()
        _process.wait()
        _end_time = datetime.datetime.now()
        with open(_log_file, "a") as f:
            _duration = _end_time - _now
            f.writelines([f"------- time taken {_duration} -------\n"])

        if _process.returncode != 0:
            sys.exit(_process.returncode)

    def show_run_log(self, run_id: str) -> None:
        """Show the log from a given run"""
        _time_sorted_logs = sorted(
            glob.glob(os.path.join(self._here, "logs", "*")),
            key=os.path.getmtime,
            reverse=True,
        )

        for log_file in _time_sorted_logs:
            _run_id = hashlib.sha1(
                open(log_file).read().encode("utf-8")
            ).hexdigest()

            if _run_id[: len(run_id)] == run_id:
                with open(log_file) as f:
                    click.echo(f.read())
                return
        click.echo(f"Could not find run matching id '{run_id}'")
        sys.exit(1)

    def add_remote(self, remote_url: str, label: str = "origin") -> None:
        """Add a remote to the list of remote URLs"""
        self.check_is_repo()
        if "remotes" not in self._local_config:
            self._local_config["remotes"] = {}
        if label in self._local_config["remotes"]:
            click.echo(f"error: registry remote '{label}' already exists.")
            sys.exit(1)
        self._local_config["remotes"][label] = remote_url

    def remove_remote(self, label: str) -> None:
        """Remove a remote URL from the list of remotes by label"""
        self.check_is_repo()
        if (
            "remotes" not in self._local_config
            or label not in self._local_config
        ):
            self.fail(f"No such entry '{label}' in available remotes")
            sys.exit(1)
        del self._local_config[label]

    def modify_remote(self, label: str, url: str) -> None:
        """Update a remote URL for a given remote"""
        self.check_is_repo()
        if (
            "remotes" not in self._local_config
            or label not in self._local_config
        ):
            click.echo(f"No such entry '{label}' in available remotes")
            sys.exit(1)
        self._local_config[label] = url

    def purge(self) -> None:
        """Remove all local FAIR tracking records and caches"""
        if not os.path.exists(self._staging_file) and not os.path.exists(
            self._local_config_file
        ):
            click.echo("No fair tracking has been initialised")
        else:
            os.remove(self._staging_file)
            os.remove(self._global_config_file)
            os.remove(self._local_config_file)

    def list_remotes(self, verbose: bool = False) -> None:
        """List the available RestAPI URLs"""
        self.check_is_repo()
        if "remotes" not in self._local_config:
            return
        else:
            _remote_print = []
            for remote, url in self._local_config["remotes"].items():
                _out_str = remote
                if verbose:
                    _out_str += f" {url}"
                _remote_print.append(_out_str)
            click.echo("\n".join(_remote_print))

    def status(self) -> None:
        """Get the status of staging"""
        self.check_is_repo()
        _staged = [i for i, j in self._stage_status.items() if j]
        _unstaged = [i for i, j in self._stage_status.items() if not j]

        if _staged:
            click.echo("Changes to be synchronized:")

            for file_name in _staged:
                click.echo(click.style(f"\t\t{file_name}", fg="green"))

        if _unstaged:
            click.echo("Files not staged for synchronization:")
            click.echo(f'\t(use "fair add <file>..." to stage files)')

            for file_name in _unstaged:
                click.echo(click.style(f"\t\t{file_name}", fg="red"))

    def set_email(self, email: str) -> None:
        """Update the email address for the user"""
        self._local_config["user"]["email"] = email

    def set_user(self, name: str) -> None:
        """Update the name of the user"""
        self._local_config["user"]["name"] = name

    def _global_config_query(self) -> None:
        """Ask user question set for creating global FAIR config"""
        _def_local = "http://localhost:8000/api/"

        _remote_url = click.prompt(f"Remote API URL")
        _local_url = click.prompt(f"Local API URL", default=_def_local)

        _def_name = socket.gethostname()
        _user_name = click.prompt("Full name", default=_def_name)
        _user_email = click.prompt("Email")
        _user_orcid = click.prompt("ORCID", default="None")
        _user_orcid = _user_orcid if _user_orcid != "None" else None

        if len(_user_name.strip().split()) == 2:
            _def_ospace = _user_name.strip().lower().split()
            _def_ospace = _def_ospace[0][0] + _def_ospace[1]
        else:
            _def_ospace = _user_name.lower().replace(" ", "")

        _def_ispace = click.prompt("Default input namespace", default="None")
        _def_ispace = _def_ispace if _def_ispace != "None" else None
        _def_ospace = click.prompt(
            "Default output namespace", default=_def_ospace
        )

        self._global_config = {
            "user": {
                "name": _user_name,
                "email": _user_email,
                "orcid": _user_orcid,
            },
            "remotes": {"local": _local_url, "origin": _remote_url},
            "namespaces": {"input": _def_ispace, "output": _def_ospace},
        }

    def _local_config_query(self, first_time_setup: bool = False) -> None:
        """Ask user questions to create local user config"""
        try:
            _def_remote = self._global_config["remotes"]["origin"]
            _def_local = self._global_config["remotes"]["local"]
            _def_ospace = self._global_config["namespaces"]["output"]
        except KeyError:
            click.echo(
                "Error: Failed to read global configuration,"
                " re-running global setup."
            )
            self._global_config_query()
            _def_remote = self._global_config["remotes"]["origin"]
            _def_local = self._global_config["remotes"]["local"]
            _def_ospace = self._global_config["namespaces"]["output"]

        if "input" not in self._global_config["namespaces"]:
            click.echo(
                "Warning: No global input namespace declared,"
                " in order to use the registry you will need to specify one"
                " within this local configuration."
            )
            _def_ispace = "null"

        _desc = click.prompt("Project description")

        if not first_time_setup:
            _def_remote = click.prompt(f"Remote API URL", default=_def_remote)
            _def_local = click.prompt(f"Local API URL", default=_def_local)
            _def_ospace = click.prompt(
                "Default output namespace", default=_def_ospace
            )
            _def_ispace = click.prompt(
                "Default input namespace", default=_def_ispace
            )

        self._local_config["namespaces"] = {
            "output": _def_ospace,
            "input": _def_ispace,
        }

        self._local_config["remotes"] = {
            "origin": _def_remote,
            "local": _def_local,
        }
        self._local_config["description"] = _desc

    def _make_starter_config(self) -> None:
        """Create a starter config.yaml"""
        if "remotes" not in self._local_config:
            click.echo(
                "Cannot generate config.yaml, you need to set the remote URL by running: \n"
                "\n\tfair remote add <url>\n"
            )
            sys.exit(1)
        with open(
            os.path.join(pathlib.Path(self._here).parent, "config.yaml"), "w"
        ) as f:
            f.write(
                config_template.render(
                    instance=self, local_repo=os.path.abspath(self._here)
                )
            )

    def initialise(self) -> None:
        """Initialise an fair repository within the current location"""
        _fair_dir = os.path.abspath(os.path.join(os.getcwd(), ".fair"))

        if os.path.exists(_fair_dir):
            click.echo(f"fatal: fair repository is already initialised.")
            sys.exit(1)

        self._local_config_file = os.path.join(_fair_dir, "config")
        self._staging_file = os.path.join(_fair_dir, "staging")

        click.echo(
            "Initialising FAIR repository, setup will now ask for basic info:\n"
        )

        if not os.path.exists(self._global_config_file):
            self._global_config_query()
            self._local_config_query(first_time_setup=True)
        else:
            self._local_config_query()

        os.mkdir(_fair_dir)

        with open(self._local_config_file, "w") as f:
            toml.dump(self._local_config, f)
        self._make_starter_config()
        click.echo(f"Initialised empty fair repository in {_fair_dir}")

    def __exit__(self, *args) -> None:
        """Upon exiting, dump all configurations to file"""
        if not os.path.exists(self.LOCAL_FOLDER):
            return
        self._stop_server()
        with open(self._staging_file, "w") as f:
            toml.dump(self._stage_status, f)
        with open(self._global_config_file, "w") as f:
            toml.dump(self._global_config, f)
        with open(self._local_config_file, "w") as f:
            toml.dump(self._local_config, f)
