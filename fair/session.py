#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Session
=======

Manage synchronisation of data and metadata relating to runs using the
FAIR Data Pipeline system.

Contents
========

Classes
-------

    FAIR - main class for performing synchronisations and model runs

Misc Variables
--------------

    __author__
    __license__
    __credits__
    __status__
    __copyright__

"""

__date__ = "2021-06-28"

import os
import glob
import uuid
import typing
import pathlib
import logging
import shutil

import click
import rich
import yaml
import socket

from fair.templates import config_template
import fair.common as fdp_com
import fair.run as fdp_run
import fair.server as fdp_serv
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.history as fdp_hist


class FAIR:
    """
    A class which provides the main interface for managing runs and data
    transfer between FAIR Data Pipeline registries.

    Methods are based around a user directory which is specified with locations
    being determined relative to the closest FAIR repository root folder (i.e.
    the closest location in the upper hierarchy containing a '.faircli. folder).

    Methods
    ----------
    check_is_repo()
    change_staging_state(file_to_stage: str, stage: bool = True)
    remove_file(file_name: str, cached: bool = False)
    show_run_log(run_id: str)
    add_remote(url: str, label: str = "origin")
    remove_remote(label: str)
    modify_remote(label: str, url: str)
    purge()
    list_remotes(verbose: bool = False)
    status()
    initialise()
    run()
    """

    def __init__(
        self,
        repo_loc: str,
        user_config: str = None,
        debug: bool = False,
        mode: fdp_serv.SwitchMode = fdp_serv.SwitchMode.NO_SERVER,
    ) -> None:
        """Initialise instance of FAIR sync tool

        All actions are performed relative to the specified folder after the
        local '.faircli' directory for the repository has been located.

        A session is usually cached to ensure that the server is not shut down
        when a process is taking place. The exception is where the user
        explicitly requests for it to be started.

        Parameters
        ----------
        repo_loc : str
            location of FAIR repository to update
        user_config : str, optional
            alternative config.yaml user configuration file
        debug : bool, optional
            run in verbose mode
        mode : fair.server.SwitchMode, optional
            stop/start server mode during session
        """
        self._logger = logging.getLogger("FAIRDataPipeline")
        if debug:
            self._logger.setLevel(logging.DEBUG)
        self._logger.debug("Starting new session.")
        self._session_loc = repo_loc
        self._run_mode = mode
        self._session_id = (
            uuid.uuid4() if mode == fdp_serv.SwitchMode.CLI else None
        )
        self._session_config = (
            user_config
            if user_config
            else fdp_com.local_user_config(self._session_loc)
        )

        # Creates $HOME/.scrc if it does not exist
        if not os.path.exists(fdp_com.REGISTRY_HOME):
            click.echo(
                "Warning: User registry directory was not found, this could "
                "mean the local registry has not been installed."
            )

        if not os.path.exists(fdp_com.global_config_dir()):
            self._logger.debug(
                "Creating directory: %s", fdp_com.global_config_dir()
            )
            os.makedirs(fdp_com.global_config_dir())

        # Initialise all configuration/staging status dictionaries
        self._stage_status: typing.Dict[str, typing.Any] = {}
        self._local_config: typing.Dict[str, typing.Any] = {}
        self._global_config: typing.Dict[str, typing.Any] = {}

        self._load_configurations()

        self._setup_server()

    def _setup_server(self) -> None:
        """Start or stop the server if required"""
        # If a session ID has been specified this means the server is auto
        # started as opposed to being started explcitly by the user
        # this means it will be shut down on completion
        if self._run_mode == fdp_serv.SwitchMode.CLI:
            self.check_is_repo()
            _cache_addr = os.path.join(
                fdp_com.session_cache_dir(), f"{self._session_id}.run"
            )

            # If there are no session cache files start the server
            if not glob.glob(
                os.path.join(fdp_com.session_cache_dir(), "*.run")
            ):
                fdp_serv.launch_server(self._local_config["remotes"]["local"])

            # Create new session cache file
            pathlib.Path(_cache_addr).touch()
        elif self._run_mode == fdp_serv.SwitchMode.USER_START:
            _cache_addr = os.path.join(
                fdp_com.session_cache_dir(), f"user.run"
            )
            if fdp_serv.check_server_running(
                self._local_config["remotes"]["local"]
            ):
                raise fdp_exc.UnexpectedRegistryServerState(
                    "Server already running."
                )
            click.echo("Starting local registry server")
            pathlib.Path(_cache_addr).touch()
            fdp_serv.launch_server(
                self._local_config["remotes"]["local"], verbose=True
            )
        elif self._run_mode in [
            fdp_serv.SwitchMode.USER_STOP,
            fdp_serv.SwitchMode.FORCE_STOP,
        ]:
            _cache_addr = os.path.join(
                fdp_com.session_cache_dir(), f"user.run"
            )
            if not fdp_serv.check_server_running(
                self._local_config["remotes"]["local"]
            ):
                raise fdp_exc.UnexpectedRegistryServerState(
                    "Server is not running."
                )
            if os.path.exists(_cache_addr):
                os.remove(_cache_addr)
            click.echo("Stopping local registry server.")
            fdp_serv.stop_server(
                self._local_config["remotes"]["local"],
                force=self._run_mode == fdp_serv.SwitchMode.FORCE_STOP,
            )

    def run(
        self,
        bash_cmd: str = "",
    ) -> None:
        """Execute a run using the given user configuration file

        Parameters
        ----------
        config_yaml : str, optional
            user configuration file, defaults to FAIR repository config.yaml
        """
        self.check_is_repo()
        if not os.path.exists(self._session_config):
            self.make_starter_config()
        self._logger.debug("Setting up command execution")
        fdp_run.run_command(self._session_loc, self._session_config, bash_cmd)

    def check_is_repo(self) -> None:
        """Check that the current location is a FAIR repository"""
        if not fdp_com.find_fair_root():
            raise fdp_exc.FDPRepositoryError(
                "Not a FAIR repository", hint="Run 'fair init' to initialise."
            )

    def __enter__(self) -> None:
        """Method called when using 'with' statement."""
        return self

    def _load_configurations(self) -> None:
        """This ensures all configurations and staging statuses are read at the
        start of every session.
        """
        if not os.path.exists(fdp_com.session_cache_dir()):
            os.makedirs(fdp_com.session_cache_dir())

        if os.path.exists(fdp_com.staging_cache(self._session_loc)):
            self._stage_status = yaml.safe_load(
                open(fdp_com.staging_cache(self._session_loc))
            )
        if os.path.exists(fdp_com.global_fdpconfig()):
            self._global_config = fdp_conf.read_global_fdpconfig()
        if os.path.exists(fdp_com.local_fdpconfig(self._session_loc)):
            self._local_config = fdp_conf.read_local_fdpconfig(
                self._session_loc
            )

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
            raise fdp_exc.FileNotFoundError(f"No such file '{file_to_stage}.")

        # Create a label with which to store the staging status of the given
        # file using its path with respect to the staging status file
        _label = os.path.relpath(
            file_to_stage,
            os.path.dirname(fdp_com.staging_cache(self._session_loc)),
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
            file_name,
            os.path.dirname(fdp_com.staging_cache(self._session_loc)),
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

    def add_remote(self, remote_url: str, label: str = "origin") -> None:
        """Add a remote to the list of remote URLs"""
        self.check_is_repo()
        if "remotes" not in self._local_config:
            self._local_config["remotes"] = {}
        if label in self._local_config["remotes"]:
            raise fdp_exc.CLIConfigurationError(
                f"Registry remote '{label}' already exists."
            )
        self._local_config["remotes"][label] = remote_url

    def remove_remote(self, label: str) -> None:
        """Remove a remote URL from the list of remotes by label"""
        self.check_is_repo()
        if (
            "remotes" not in self._local_config
            or label not in self._local_config
        ):
            raise fdp_exc.CLIConfigurationError(
                f"No such entry '{label}' in available remotes"
            )
        del self._local_config[label]

    def modify_remote(self, label: str, url: str) -> None:
        """Update a remote URL for a given remote"""
        self.check_is_repo()
        if (
            "remotes" not in self._local_config
            or label not in self._local_config["remotes"]
        ):
            raise fdp_exc.CLIConfigurationError(
                f"No such entry '{label}' in available remotes"
            )
        self._local_config["remotes"][label] = url

    def clear_logs(self) -> None:
        """Delete all local run stdout logs

        This does NOT delete any information from the registry
        """
        _log_files = glob.glob(fdp_hist.history_directory(), "*.log")
        if _log_files:
            for log in _log_files:
                os.remove(log)

    def list_remotes(self, verbose: bool = False) -> None:
        """List the available RestAPI URLs"""
        self.check_is_repo()
        if "remotes" not in self._local_config:
            return
        else:
            _remote_print = []
            for remote, url in self._local_config["remotes"].items():
                _out_str = f"[bold white]{remote}[/bold white]"
                if verbose:
                    _out_str += f"\t[yellow]{url}[/yellow]"
                _remote_print.append(_out_str)
            rich.print("\n".join(_remote_print))

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

        if not _staged and not _unstaged:
            click.echo("Nothing marked for tracking.")

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
            first_time_setup = True
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
            _def_ispace = None
        else:
            _def_ispace = self._global_config["namespaces"]["input"]

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

    def make_starter_config(self) -> None:
        """Create a starter config.yaml"""
        if "remotes" not in self._local_config:
            raise fdp_exc.CLIConfigurationError(
                "Cannot generate user 'config.yaml'",
                hint="You need to set the remote URL"
                " by running: \n\n\tfair remote add <url>\n",
            )
        with open(self._session_config, "w") as f:
            _yaml_str = config_template.render(
                instance=self,
                data_dir=fdp_com.default_data_dir(),
                local_repo=os.path.abspath(fdp_com.find_fair_root()),
            )
            _yaml_dict = yaml.safe_load(_yaml_str)

            # Null keys are not loaded by YAML so add manually
            _yaml_dict["run_metadata"]["script"] = None
            yaml.dump(_yaml_dict, f)

    def initialise(self) -> None:
        """Initialise an fair repository within the current location

        Parameters
        ----------

        repo_loc : str, optional
            location in which to initialise FAIR repository

        """
        _fair_dir = os.path.abspath(
            os.path.join(self._session_loc, fdp_com.FAIR_FOLDER)
        )

        if os.path.exists(_fair_dir):
            fdp_exc.FDPRepositoryError(
                f"FAIR repository is already initialised."
            )

        self._staging_file = os.path.join(_fair_dir, "staging")

        click.echo(
            "Initialising FAIR repository, setup will now ask for basic info:\n"
        )

        if not os.path.exists(fdp_com.global_fdpconfig()):
            self._global_config_query()
            self._local_config_query(first_time_setup=True)
        else:
            self._local_config_query()

        os.mkdir(_fair_dir)

        with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
            yaml.dump(self._local_config, f)
        self.make_starter_config()
        click.echo(f"Initialised empty fair repository in {_fair_dir}")

    def __exit__(self, *args) -> None:
        """Upon exiting, dump all configurations to file"""
        if not os.path.exists(
            os.path.join(self._session_loc, fdp_com.FAIR_FOLDER)
        ):
            return

        if self._session_id:
            # Remove the session cache file
            _cache_addr = os.path.join(
                fdp_com.session_cache_dir(), f"{self._session_id}.run"
            )
            os.remove(_cache_addr)

        if not os.path.exists(
            os.path.join(fdp_com.session_cache_dir(), "user.run")
            and self._run_mode != fdp_serv.SwitchMode.NO_SERVER
        ):
            fdp_serv.stop_server(self._local_config["remotes"]["local"])

        with open(fdp_com.staging_cache(self._session_loc), "w") as f:
            yaml.dump(self._stage_status, f)
        with open(fdp_com.global_fdpconfig(), "w") as f:
            yaml.dump(self._global_config, f)
        with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
            yaml.dump(self._local_config, f)
