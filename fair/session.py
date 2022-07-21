#!/usr/bin/python3
# flake8: noqa
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

        FAIR - main class for performing synchronisations and executing jobs

    Misc Variables
    --------------

        __author__
        __license__
        __credits__
        __status__
        __copyright__

"""

__date__ = "2021-06-28"

import copy
import glob
import logging
import os
import pathlib
import re
import shutil
import typing
import uuid

import click
import git
import pydantic
import rich
import yaml
from rich.console import Console
from rich.table import Table

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.configuration.validation as fdp_clivalid
import fair.exceptions as fdp_exc
import fair.history as fdp_hist
import fair.registry.requests as fdp_req
import fair.registry.server as fdp_serv
import fair.registry.sync as fdp_sync
import fair.staging as fdp_stage
import fair.templates as fdp_tpl
import fair.testing as fdp_test
import fair.user_config as fdp_user


class FAIR:
    """
    A class which provides the main interface for managing runs and data
    transfer between FAIR Data Pipeline registries.

    Methods are based around a user directory which is specified with locations
    being determined relative to the closest FAIR repository root folder (i.e.
    the closest location in the upper hierarchy containing a '.fair' folder).
    """

    _logger = logging.getLogger("FAIRDataPipeline.Session")

    def __init__(
        self,
        repo_loc: str,
        user_config: str = None,
        debug: bool = False,
        server_mode: fdp_serv.SwitchMode = fdp_serv.SwitchMode.NO_SERVER,
        server_port: int = 8000,
        server_address: str = "127.0.0.1",
        allow_dirty: bool = False,
        testing: bool = False,
        local: bool = False,
    ) -> None:
        """Initialise instance of FAIR sync tool

        All actions are performed relative to the specified folder after the
        local '.fair' directory for the repository has been located.

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
        server_mode : fair.registry.server.SwitchMode, optional
            stop/start server mode during session
        server_port : int, optional
            port to run local registry on, default is 8000
        allow_dirty : bool, optional
            allow runs with uncommitted changes, default is False
        testing : bool
            run in testing mode
        local : bool
            bypasses storing the remote in the config yamls
        """
        if debug:
            logging.getLogger("FAIRDataPipeline").setLevel(logging.DEBUG)
        else:
            logging.getLogger("FAIRDataPipeline").setLevel(logging.CRITICAL)
        self._logger.debug("Starting new session.")
        self._testing = testing
        self._local = local
        self._session_loc = repo_loc
        self._allow_dirty = allow_dirty
        self._logger.debug(f"Session location: {self._session_loc}")
        self._run_mode = server_mode
        self._stager: fdp_stage.Stager = fdp_stage.Stager(self._session_loc)
        self._session_id = (
            uuid.uuid4() if server_mode == fdp_serv.SwitchMode.CLI else None
        )
        self._session_config = None

        if user_config and not os.path.exists(user_config):
            raise fdp_exc.FileNotFoundError(
                f"Cannot launch session from configuration file '{user_config}', "
                "file not found."
            )

        self._session_config = fdp_user.JobConfiguration(user_config)

        if (
            server_mode != fdp_serv.SwitchMode.NO_SERVER
            and not os.path.exists(fdp_com.registry_home())
        ):
            raise fdp_exc.RegistryError(
                f"User registry directory '{fdp_com.registry_home()}' was not found, this could "
                "mean the local registry has not been installed."
            )

        if not os.path.exists(fdp_com.global_config_dir()):
            self._logger.debug(
                "Creating directory: %s", fdp_com.global_config_dir()
            )
            os.makedirs(fdp_com.global_config_dir())
            assert os.path.exists(fdp_com.global_config_dir())

        # Initialise all configuration status dictionaries
        self._local_config: typing.Dict[str, typing.Any] = {}
        self._global_config: typing.Dict[str, typing.Any] = {}

        self._logger.debug(
            "Initialising session with:\n"
            "\tlocation       = %s\n"
            "\tsession_config = %s\n"
            "\ttesting        = %s\n"
            "\trun_mode       = %s\n"
            "\tstaging_file   = %s\n"
            "\tsession_id     = %s\n"
            "\tlocal          = %s\n",
            self._session_loc,
            user_config,
            self._testing,
            self._run_mode,
            self._stager._staging_file,
            self._session_id,
            self._local,
        )

        self._load_configurations()

        self._setup_server(server_port, server_address)

    def purge(
        self,
        verbose: bool = True,
        global_cfg: bool = False,
        clear_data: bool = False,
        clear_all: bool = False,
    ) -> None:
        """Remove FAIR-CLI tracking from the given directory

        Parameters
        ==========
        global_cfg : bool, optional
            remove global directories, default is False
        verbose : bool, optional
            run in verbose mode, default is True
        clear_data : bool, optional
            remove the data directory (potentially dangerous), default is False
        clear_all : bool, optional
            remove all FAIR components from the system, overrides others, default is False
        """
        _root_dir = os.path.join(
            fdp_com.find_fair_root(self._session_loc), fdp_com.FAIR_FOLDER
        )
        if os.path.exists(_root_dir):
            if verbose:
                click.echo(f"Removing directory '{_root_dir}'")
            shutil.rmtree(_root_dir)
        if clear_all:
            try:
                if fdp_serv.check_server_running():
                    fdp_serv.stop_server()
            except (fdp_exc.FileNotFoundError, fdp_exc.CLIConfigurationError):
                click.echo(
                    "Warning: Unable to check if server is running, "
                    "you may need to manually terminate the Django process"
                )
            if verbose and os.path.exists(fdp_com.USER_FAIR_DIR):
                click.echo(f"Removing directory '{fdp_com.USER_FAIR_DIR}'")
            shutil.rmtree(fdp_com.USER_FAIR_DIR)
            return
        if clear_data:
            try:
                if verbose and os.path.exists(fdp_com.default_data_dir()):
                    click.echo(
                        f"Removing directory '{fdp_com.default_data_dir()}'"
                    )
                if os.path.exists(fdp_com.default_data_dir()):
                    shutil.rmtree(fdp_com.default_data_dir())
            except FileNotFoundError as e:
                raise fdp_exc.FileNotFoundError(
                    "Cannot remove local data store, a global CLI configuration "
                    "is required to identify its location"
                ) from e

        if global_cfg:
            if verbose:
                click.echo(
                    f"Removing directory '{fdp_com.global_config_dir()}'"
                )
            _global_dirs = fdp_com.global_config_dir()
            if os.path.exists(_global_dirs):
                shutil.rmtree(_global_dirs)

    def _setup_server(self, port: int, address: str) -> None:
        """Start or stop the server if required"""
        self._logger.debug(
            f"Running server setup for run mode {self._run_mode}"
        )
        if self._run_mode == fdp_serv.SwitchMode.CLI:
            self._setup_server_cli_mode(port, address)
        elif self._run_mode == fdp_serv.SwitchMode.USER_START:
            self._setup_server_user_start(port, address)
        elif self._run_mode in [
            fdp_serv.SwitchMode.USER_STOP,
            fdp_serv.SwitchMode.FORCE_STOP,
        ]:
            self._stop_server()

    def _stop_server(self) -> None:
        _cache_addr = os.path.join(fdp_com.session_cache_dir(), "user.run")
        if not fdp_serv.check_server_running():
            raise fdp_exc.UnexpectedRegistryServerState(
                "Server is not running."
            )
        if os.path.exists(_cache_addr):
            os.remove(_cache_addr)
        click.echo("Stopping local registry server.")
        if (
            os.listdir(fdp_com.session_cache_dir())
            and self._run_mode != fdp_serv.SwitchMode.FORCE_STOP
        ):
            raise fdp_exc.UnexpectedRegistryServerState(
                "Cannot stop registry, a process may still be running",
                hint="You can force stop using '--force'",
            )
        fdp_serv.stop_server(
            force=self._run_mode == fdp_serv.SwitchMode.FORCE_STOP,
        )

    def _setup_server_cli_mode(self, port: int, address: str) -> None:
        self.check_is_repo()
        _cache_addr = os.path.join(
            fdp_com.session_cache_dir(), f"{self._session_id}.run"
        )

        self._logger.debug("Checking for existing sessions")
        # If there are no session cache files start the server
        if not glob.glob(os.path.join(fdp_com.session_cache_dir(), "*.run")):
            self._logger.debug("No sessions found, launching server")
            fdp_serv.launch_server(port=port, address=address)

        self._logger.debug(f"Creating new session #{self._session_id}")

        if not os.path.exists(fdp_com.session_cache_dir()):
            raise fdp_exc.InternalError(
                "Failed to create session cache file, "
                f"expected cache directory '{fdp_com.session_cache_dir()}' to exist"
            )

        # Create new session cache file
        pathlib.Path(_cache_addr).touch()

    def _setup_server_user_start(self, port: int, address: str) -> None:
        if not os.path.exists(fdp_com.session_cache_dir()):
            os.makedirs(fdp_com.session_cache_dir())

        _cache_addr = os.path.join(fdp_com.session_cache_dir(), "user.run")

        if self._global_config and "registries" not in self._global_config:
            raise fdp_exc.CLIConfigurationError(
                "Cannot find server address in current configuration",
                hint="Is the current location a FAIR repository?",
            )

        if fdp_serv.check_server_running():
            raise fdp_exc.UnexpectedRegistryServerState(
                "Server already running."
            )
        click.echo("Starting local registry server")
        pathlib.Path(_cache_addr).touch()
        fdp_serv.launch_server(port=port, verbose=True, address=address)

    def _pre_job_setup(self, remote: str = None) -> None:
        self._logger.debug("Running pre-job setup")
        self.check_is_repo()
        self._session_config.update_from_fair(
            fdp_com.find_fair_root(self._session_loc), remote
        )

    def _post_job_breakdown(self, add_run: bool = False) -> None:
        if add_run:
            self._logger.debug(
                f"Tracking job hash {self._session_config.hash}"
            )

        self._logger.debug("Updating staging post-run")

        self._stager.update_data_product_staging()

        if add_run:
            # Automatically add the run to tracking but unstaged
            self._stager.add_to_staging(self._session_config.hash, "job")

        self._session_config.close_log()

    def push(self, remote: str = "origin"):
        self._pre_job_setup(remote)
        self._session_config.prepare(
            fdp_com.CMD_MODE.PUSH, allow_dirty=self._allow_dirty, local=local
        )
        _staged_data_products = self._stager.get_item_list(
            True, "data_product"
        )

        if not _staged_data_products:
            click.echo("Nothing to push.")

        fdp_sync.sync_data_products(
            origin_uri=fdp_conf.get_local_uri(),
            dest_uri=fdp_conf.get_remote_uri(self._session_loc, remote),
            dest_token=fdp_conf.get_remote_token(
                self._session_loc, remote, local=self._local
            ),
            origin_token=fdp_req.local_token(),
            remote_label=remote,
            data_products=_staged_data_products,
        )

        self._session_config.write_log_lines(
            [f"Pushing data products to remote '{remote}':"]
            + [f"\t- {data_product}" for data_product in _staged_data_products]
        )

        self._post_job_breakdown()

        # When push successful unstage data products again
        for data_product in _staged_data_products:
            self._stager.change_stage_status(
                data_product, "data_product", False
            )

    def pull(self, remote: str = "origin"):
        if not self._local:

            self._logger.debug("Performing pull on remote '%s'", remote)

            _remote_addr = fdp_conf.get_remote_uri(self._session_loc, remote)

            if not fdp_serv.check_server_running(_remote_addr):
                raise fdp_exc.UnexpectedRegistryServerState(
                    f"Cannot perform pull from registry '{remote}' as the"
                    f" server does not exist. Expected response from '{_remote_addr}'.",
                    hint="Is your FAIR repository configured correctly?",
                )
            self._logger.debug("Retrieving namespaces from remote")

            fdp_sync.pull_all_namespaces(
                fdp_conf.get_local_uri(),
                fdp_conf.get_remote_uri(self._session_loc, remote),
                fdp_req.local_token(),
                fdp_conf.get_remote_token(
                    self._session_loc, remote, self._local
                ),
            )

            self._logger.debug("Performing pre-job setup")

        self._pre_job_setup(remote)

        self._session_config.prepare(
            fdp_com.CMD_MODE.PULL,
            allow_dirty=self._allow_dirty,
            local=self._local,
            remote_uri=fdp_conf.get_remote_uri(self._session_loc, remote),
            remote_token=fdp_conf.get_remote_token(
                self._session_loc, remote, local=self._local
            ),
        )

        _readables = self._session_config.get_readables()

        self._session_config.write()

        self._logger.debug("Preparing to retrieve %s items", len(_readables))

        self._logger.debug("Pulling data products locally")

        # Only push data products if there are any to do so, this covers the
        # case whereby no remote has been setup and we just want to register
        # items on the local registry
        if _readables:
            fdp_sync.sync_data_products(
                origin_uri=fdp_conf.get_remote_uri(self._session_loc, remote),
                dest_uri=fdp_conf.get_local_uri(),
                dest_token=fdp_req.local_token(),
                origin_token=fdp_conf.get_remote_token(
                    self._session_loc, remote, local=self._local
                ),
                remote_label=remote,
                data_products=_readables,
                local_data_store=self._session_config.default_data_store,
            )

            self._session_config.write_log_lines(
                [f"Pulled data products from remote '{remote}':"]
                + [f"\t- {data_product}" for data_product in _readables]
            )
        else:
            click.echo(f"No items to retrieve from remote '{remote}'.")

        self._logger.debug("Performing post-job breakdown")

        self._post_job_breakdown()
        # else:
        #     click.echo("working with no remote")
        #     self._logger.debug(f"local is {self._local}")

    def run(
        self,
        bash_cmd: str = "",
        passive: bool = False,
        allow_dirty: bool = False,
        local: bool = False,
    ) -> str:
        """Execute a run using the given user configuration file"""
        self._pre_job_setup()

        self._session_config.prepare(
            fdp_com.CMD_MODE.PASS if passive else fdp_com.CMD_MODE.RUN,
            allow_dirty=self._allow_dirty,
            local=local,
        )

        self._logger.debug("Setting up command execution")
        if bash_cmd:
            self._session_config.set_command(bash_cmd)

        self._session_config.setup_job_script()

        if allow_dirty:
            self._logger.debug("Allowing uncommitted changes during run.")

        # Only apply constraint for clean repository when executing a run
        if passive:
            allow_dirty = True
        if not local:
            self.check_git_repo_state(allow_dirty=allow_dirty)

        self._session_config.write()

        self._session_config.execute()

        self._post_job_breakdown(add_run=True)

        return self._session_config.hash

    def check_is_repo(self, location: str = None) -> None:
        """Check that the current location is a FAIR repository"""
        if not location:
            location = self._session_loc
        if not fdp_com.find_fair_root(location):
            raise fdp_exc.FDPRepositoryError(
                f"'{location}' is not a FAIR repository",
                hint="Run 'fair init' to initialise.",
            )

    def check_git_repo_state(
        self, remote_label: str = "origin", allow_dirty: bool = False
    ) -> bool:
        """Checks the git repository is clean and that local matches remote"""
        _repo_root = fdp_com.find_git_root(self._session_loc)
        _repo = git.Repo(_repo_root)
        _rem_commit = None
        _loc_commit = None
        _current_branch = None

        # Firstly get the current branch
        try:
            _current_branch = _repo.active_branch.name
            # Get the latest commit on the current branch locally
            _loc_commit = _repo.refs[_current_branch].commit.hexsha
        except (TypeError, IndexError) as e:
            if allow_dirty:
                click.echo(f"Warning: {' '.join(e.args)}")
            else:
                raise fdp_exc.FDPRepositoryError(" ".join(e.args)) from e

        # Get the latest commit on this branch on remote

        try:
            if _current_branch:
                _rem_commit = (
                    _repo.remotes[remote_label]
                    .refs[_current_branch]
                    .commit.hexsha
                )
        except git.InvalidGitRepositoryError as exc:
            raise fdp_exc.FDPRepositoryError(
                f"Location '{self._session_loc}' is not a valid git repository"
            ) from exc

        except ValueError as exc:
            raise fdp_exc.FDPRepositoryError(
                f"Failed to retrieve latest commit for local repository '{self._session_loc}'",
                hint="Have any changes been committed in the project repository?",
            ) from exc

        except IndexError as exc:
            _msg = f"Failed to find branch '{_current_branch}' on remote repository"
            if allow_dirty:
                click.echo(f"Warning: {_msg}")
            else:
                raise fdp_exc.FDPRepositoryError(_msg) from exc

        # Commit match
        _com_match = _loc_commit == _rem_commit

        if not _com_match:
            if allow_dirty:
                click.echo(
                    "Warning: local git repository is ahead/behind remote"
                )
            else:
                raise fdp_exc.FDPRepositoryError(
                    "Cannot run job, local git repository not level with "
                    f"remote '{remote_label}'"
                )
        if _repo.is_dirty():
            if allow_dirty:
                click.echo("Warning: running with uncommitted changes")
            else:
                raise fdp_exc.FDPRepositoryError(
                    "Cannot run job, git repository contains uncommitted changes"
                )

        return _repo.is_dirty() and _com_match

    def __enter__(self) -> None:
        """Method called when using 'with' statement."""
        return self

    def _load_configurations(self) -> None:
        """This ensures all configurations are read at the
        start of every session.
        """
        self._logger.debug("Loading CLI configurations.")

        if os.path.exists(fdp_com.global_fdpconfig()):
            self._global_config = fdp_conf.read_global_fdpconfig()
        if os.path.exists(fdp_com.local_fdpconfig(self._session_loc)):
            self._local_config = fdp_conf.read_local_fdpconfig(
                self._session_loc
            )

    def reset_staging(self) -> None:
        """Reset all staged items"""
        self._stager.reset_staged()

    def change_staging_state(
        self,
        identifier: str,
        type_to_stage: str = "data_product",
        stage: bool = True,
    ) -> None:
        """Change the staging status of a given run

        Parameters
        ----------
        job_to_stage : str
            uuid of run to add to staging
        stage : bool, optional
            whether to stage/unstage run, by default True (staged)
        """
        self.check_is_repo()
        if type_to_stage == "data_product":
            self._stager.change_stage_status(identifier, type_to_stage, stage)
        else:
            self._stager.change_job_stage_status(identifier, stage)

    def remove_job(self, job_id: str, cached: bool = False) -> None:
        """Remove a job from tracking

        Parameters
        ----------
        file_name : str
            path of file to be removed
        cached : bool, optional
            remove from tracking but not from system, by default False
        """
        self.check_is_repo()

        self._stager.remove_staging_entry(job_id)

    def add_remote(
        self, remote_url: str, token_file: str, label: str = "origin"
    ) -> None:
        """Add a remote to the list of remote URLs"""
        self.check_is_repo()
        if "registries" not in self._local_config:
            self._local_config["registries"] = {}
        if label in self._local_config["registries"]:
            raise fdp_exc.CLIConfigurationError(
                f"Registry remote '{label}' already exists."
            )
        self._local_config["registries"][label] = {
            "uri": remote_url,
            "token": token_file,
        }

    def remove_remote(self, label: str) -> None:
        """Remove a remote URL from the list of remotes by label"""
        self.check_is_repo()
        if (
            "registries" not in self._local_config
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
            "registries" not in self._local_config
            or label not in self._local_config["registries"]
        ):
            raise fdp_exc.CLIConfigurationError(
                f"No such entry '{label}' in available remotes"
            )
        self._local_config["registries"][label]["uri"] = url

    def clear_logs(self) -> None:
        """Delete all local run stdout logs

        This does NOT delete any information from the registry
        """
        if _log_files := glob.glob(
            fdp_hist.history_directory(self._session_loc), "*.log"
        ):
            for log in _log_files:
                os.remove(log)

    def list_remotes(self, verbose: bool = False) -> typing.List[str]:
        """List the available RestAPI URLs"""
        self.check_is_repo()
        if "registries" not in self._local_config:
            return []
        _remote_print = []
        for remote, data in self._local_config["registries"].items():
            _out_str = f"[bold white]{remote}[/bold white]"
            if verbose:
                _out_str += f"\t[yellow]{data['uri']}[/yellow]"
            _remote_print.append(_out_str)
        rich.print("\n".join(_remote_print))
        return _remote_print

    def status_data_products(self) -> None:
        """Get the stageing status of DataProducts"""
        self._logger.debug("Getting DataProducts staging status")
        self.check_is_repo()

        self._stager.update_data_product_staging()
        _staged_data_products = self._stager.get_item_list(
            True, "data_product"
        )
        _unstaged_data_products = self._stager.get_item_list(
            False, "data_product"
        )

        if _staged_data_products:
            self.show_data_products(
                _staged_data_products,
                "Changes to be synchronized",
            )

        if _unstaged_data_products:
            self.show_data_products(
                _unstaged_data_products,
                "Data products not staged for synchronization:",
                style="red",
            )
            click.echo(
                rich.print(
                    '(use "fair add <DataProduct>..." to stage DataProducts)'
                )
            )

        if not _unstaged_data_products and not _staged_data_products:
            click.echo("No DataProducts marked for tracking.")

    def show_data_products(
        self, data_products: typing.List[str], title: str, style="green"
    ) -> None:
        console = Console()
        table = Table(
            title=title,
            title_style="bold",
            title_justify="left",
            box=rich.box.SIMPLE,
        )
        table.add_column("Namespace", style=style, no_wrap=True)
        table.add_column("Name", style=style, no_wrap=True)
        table.add_column("Version", style=style, no_wrap=True)
        for i, data_product in enumerate(data_products):
            namespace, name, version = re.split("[:@]", data_product)
            table.add_row(namespace, name, version)
            if i == 9 and i != len(data_products) - 1:
                table.add_row(
                    f"+ {len(data_products) - i - 1} more...",
                    "",
                    "",
                )
                break
        click.echo(console.print(table))

    def status_jobs(self, verbose: bool = False) -> None:
        """Get the staging status of jobs"""
        self._logger.debug("Getting job staging status")
        self.check_is_repo()

        _staged_jobs = self._stager.get_item_list(True, "job")
        _unstaged_jobs = self._stager.get_item_list(False, "job")

        if _staged_jobs:
            click.echo("Changes to be synchronized:")
            click.echo("\tJobs:")
            for job in _staged_jobs:
                click.echo(click.style(f"\t\t{job}", fg="green"))
                _job_urls = self._stager.get_job_data(
                    fdp_conf.get_local_uri(), job
                )
                if not verbose:
                    continue

                for key, value in _job_urls.items():
                    if not value:
                        continue
                    click.echo(click.style(f"\t\t\t{key}:", fg="green"))
                    if isinstance(value, list):
                        for url in value:
                            click.echo(
                                click.style(f"\t\t\t\t{url}", fg="green")
                            )
                    else:
                        click.echo(click.style(f"\t\t\t\t{value}", fg="green"))

        if _unstaged_jobs:
            click.echo("Changes not staged for synchronization:")
            click.echo('\t(use "fair add <job>..." to stage jobs)')

            click.echo("\tJobs:")

            for job in _unstaged_jobs:
                click.echo(click.style(f"\t\t{job}", fg="red"))
                _job_urls = self._stager.get_job_data(
                    fdp_conf.get_local_uri(), job
                )

                if not verbose:
                    continue

                for key, value in _job_urls.items():
                    if not value:
                        continue
                    click.echo(
                        click.style(
                            f"\t\t\t{key.replace('_', ' ').title()}:", fg="red"
                        )
                    )
                    if isinstance(value, list):
                        for url in value:
                            click.echo(click.style(f"\t\t\t\t{url}", fg="red"))
                    else:
                        click.echo(click.style(f"\t\t\t\t{value}", fg="red"))

        if not _unstaged_jobs and not _staged_jobs:
            click.echo("No jobs marked for tracking.")

    def make_starter_config(self, output_file_name: str = None) -> None:
        """Create a starter config.yaml"""
        if not output_file_name:
            output_file_name = os.path.join(
                self._session_loc, fdp_com.USER_CONFIG_FILE
            )
        if os.path.exists(output_file_name):
            click.echo(
                f"The user configuration file '{os.path.abspath(output_file_name)}'"
                " already exists, skipping creation."
            )
            return
        if "registries" not in self._local_config:
            raise fdp_exc.CLIConfigurationError(
                "Cannot generate user 'config.yaml'",
                hint="You need to set the remote URL"
                " by running: \n\n\tfair remote add <url>\n",
            )

        with open(output_file_name, "w") as f:
            _yaml_str = fdp_tpl.config_template.render(
                instance=self,
                data_dir=fdp_com.default_data_dir(),
                local_repo=os.path.abspath(
                    fdp_com.find_fair_root(self._session_loc)
                ),
            )
            _yaml_dict = yaml.safe_load(_yaml_str)

            yaml.dump(_yaml_dict, f, sort_keys=False)

    def _export_cli_configuration(self, output_file: str) -> None:
        _cli_config = fdp_conf.read_global_fdpconfig()
        _loc_config = fdp_conf.read_local_fdpconfig(self._session_loc)
        _cli_config["git"] = _loc_config["git"]
        _cli_config["registries"].update(_loc_config["registries"])
        _cli_config["user"].update(_loc_config["user"])
        with open(output_file, "w") as f:
            yaml.dump(_cli_config, f)

    # noqa: C901
    def initialise(
        self,
        using: typing.Dict = None,
        registry: str = None,
        export_as: str = None,
        local: bool = False,
    ) -> None:
        """Initialise an fair repository within the current location

        Parameters
        ----------
        using : str
            load from an existing global CLI configuration file
        """
        _fair_dir = os.path.abspath(
            os.path.join(self._session_loc, fdp_com.FAIR_FOLDER)
        )

        _first_time = not os.path.exists(fdp_com.global_fdpconfig())

        if self._testing:
            using = fdp_test.create_configurations(
                registry,
                fdp_com.find_git_root(os.getcwd()),
                os.getcwd(),
                os.path.join(os.getcwd(), "data_store"),
            )

        if os.path.exists(_fair_dir):
            if export_as:
                self._export_cli_configuration(export_as)
                return
            click.echo("FAIR repository is already initialised.")
            return

        if _existing := fdp_com.find_fair_root(self._session_loc):
            click.echo(
                "A FAIR repository was initialised for this location at"
                f" '{_existing}'"
            )
            _confirm = click.confirm("Do you want to continue?", default=False)
            if not _confirm:
                click.echo("Aborted intialisation.")
                return

        if not using:
            click.echo(
                "Initialising FAIR repository, setup will now ask for basic info:\n"
            )

        if not os.path.exists(_fair_dir):
            os.mkdir(_fair_dir)
            os.makedirs(fdp_com.session_cache_dir(), exist_ok=True)
            if using:
                self._validate_and_load_cli_config(using)
            self._stager.initialise()

        if not os.path.exists(fdp_com.global_fdpconfig()):
            try:
                self._global_config = fdp_conf.global_config_query(
                    registry, local
                )
            except (fdp_exc.CLIConfigurationError, click.Abort) as e:
                self._clean_reset(_fair_dir, e)
            try:
                self._local_config = fdp_conf.local_config_query(
                    self._global_config,
                    first_time_setup=_first_time,
                    local=local,
                )
            except (fdp_exc.CLIConfigurationError, click.Abort) as e:
                self._clean_reset(_fair_dir, e, True)
        elif not using:
            try:
                self._local_config = fdp_conf.local_config_query(
                    self._global_config, local=local
                )
            except (fdp_exc.CLIConfigurationError, click.Abort) as e:
                self._clean_reset(_fair_dir, e, True)
        if not using:
            with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
                yaml.dump(self._local_config, f)
            with open(fdp_com.global_fdpconfig(), "w") as f:
                yaml.dump(self._global_config, f)
        else:
            self._global_config = fdp_conf.read_global_fdpconfig()
            self._local_config = fdp_conf.read_local_fdpconfig(
                self._session_loc
            )

        if export_as:
            self._export_cli_configuration(export_as)

        fdp_serv.update_registry_post_setup(self._session_loc, _first_time)
        try:
            fdp_clivalid.LocalCLIConfig(**self._local_config)
        except pydantic.ValidationError as e:
            self._logger.debug(f"Local CLI validator returned: {e.json()}")
            self._clean_reset(_fair_dir, local_only=True)
            raise fdp_exc.InternalError(
                "Initialisation failed, validation of local CLI config file did not pass"
            ) from e

        try:
            fdp_clivalid.GlobalCLIConfig(**self._global_config)
        except pydantic.ValidationError as e:
            self._logger.debug(f"Global CLI validator returned: {e.json()}")
            self._clean_reset(_fair_dir, local_only=False)
            raise fdp_exc.InternalError(
                "Initialisation failed, validation of global CLI config file did not pass"
            ) from e

        os.makedirs(
            fdp_hist.history_directory(self._session_loc), exist_ok=True
        )

        click.echo(f"Initialised empty fair repository in {_fair_dir}")

    def _clean_reset(
        self, _fair_dir, e: Exception = None, local_only: bool = False
    ):
        if not local_only:
            shutil.rmtree(fdp_com.session_cache_dir(), ignore_errors=True)
            shutil.rmtree(fdp_com.global_config_dir(), ignore_errors=True)
        shutil.rmtree(_fair_dir)
        if e:
            raise e

    def close_session(self) -> None:
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

        if os.path.exists(fdp_com.global_config_dir()):
            with open(fdp_com.global_fdpconfig(), "w") as f:
                yaml.dump(self._global_config, f)
        if os.path.exists(os.path.dirname(fdp_com.local_fdpconfig())):
            with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
                yaml.dump(self._local_config, f)

    def _validate_and_load_cli_config(self, cli_config: typing.Dict):
        _exp_keys = ["registries", "namespaces", "user", "git"]

        for key in _exp_keys:
            if key not in cli_config:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key '{key}' in CLI configuration file"
                )

        for exp_reg in ["local", "origin"]:
            if exp_reg not in cli_config["registries"]:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'registries:{exp_reg}' in CLI configuration file"
                )

        for name, reg in cli_config["registries"].items():
            if "data_store" not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key '{key}' for remote '{name}' "
                    "in CLI configuration"
                )
            if name != "local" and "uri" not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'uri' for remote '{name}' "
                    "in CLI configuration"
                )
            if name != "local" and "token" not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'token' for remote '{name}' "
                    "in CLI configuration"
                )
            if name == "local" and "directory" not in reg:
                raise fdp_exc.CLIConfigurationError(
                    "Expected key 'directory' for local registry in CLI configuration"
                )

        _user_keys = ["email", "family_name", "given_names", "orcid", "uuid"]

        for key in _user_keys:
            if key not in cli_config["user"]:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'user:{key}' in CLI configuration file"
                )

        if not cli_config["user"]["orcid"] and not cli_config["user"]["uuid"]:
            raise fdp_exc.CLIConfigurationError(
                "At least one of 'user:orcid' and 'user:uuid' must be provided "
                " in CLI configuration"
            )

        for key in ["local_repo", "remote"]:
            if key not in cli_config["git"]:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'git:{key}' in CLI configuration"
                )

        _glob_cfg = copy.deepcopy(cli_config)
        _loc_cfg = copy.deepcopy(cli_config)
        del _glob_cfg["git"]
        if "description" in _glob_cfg:
            del _glob_cfg["description"]
        del _loc_cfg["registries"]["local"]

        with open(fdp_com.global_fdpconfig(), "w") as f:
            yaml.dump(_glob_cfg, f)

        with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
            yaml.dump(_loc_cfg, f)

    def __exit__(self, *args) -> None:
        self.close_session()
