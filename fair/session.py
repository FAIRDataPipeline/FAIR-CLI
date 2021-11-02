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

import os
import glob
import uuid
import typing
import pathlib
import logging
import shutil
import copy
import click
import rich
import git
import yaml

import fair.templates as fdp_tpl
import fair.common as fdp_com
import fair.run as fdp_run
import fair.registry.server as fdp_serv
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.history as fdp_hist
import fair.staging as fdp_stage
import fair.testing as fdp_test
import fair.registry.sync as fdp_sync


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
        testing: bool = False
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
        testing : bool
            run in testing mode
        """
        if debug:
            logging.getLogger('FAIRDataPipeline').setLevel(logging.DEBUG)
        self._logger.debug("Starting new session.")
        self._testing = testing
        self._session_loc = repo_loc
        self._logger.debug(f"Session location: {self._session_loc}")
        self._run_mode = server_mode
        self._stager: fdp_stage.Stager = fdp_stage.Stager(self._session_loc)
        self._session_id = (
            uuid.uuid4() if server_mode == fdp_serv.SwitchMode.CLI else None
        )
        self._session_config = user_config or fdp_com.local_user_config(
            self._session_loc
        )

        if server_mode != fdp_serv.SwitchMode.NO_SERVER and not os.path.exists(
            fdp_com.registry_home()
        ):
            raise fdp_exc.RegistryError(
                "User registry directory was not found, this could "
                "mean the local registry has not been installed."
            )

        if not os.path.exists(fdp_com.global_config_dir()):
            self._logger.debug(
                "Creating directory: %s", fdp_com.global_config_dir()
            )
            os.makedirs(fdp_com.global_config_dir())

        # Initialise all configuration status dictionaries
        self._local_config: typing.Dict[str, typing.Any] = {}
        self._global_config: typing.Dict[str, typing.Any] = {}

        self._logger.debug(
            "Initialising session with:\n"
            "\tsession_config = %s\n"
            "\ttesting        = %s\n"
            "\trun_mode       = %s\n"
            "\tstaging_file   = %s\n"
            "\tsession_id     = %s\n",
            self._session_config,
            self._testing,
            self._run_mode,
            self._stager._staging_file,
            self._session_id
        )

        self._load_configurations()

        self._setup_server()

    def push(self, remote: str = 'origin'):
        self._logger.debug(
            f"Pushing items in '{self._session_config}:write' to remote"
            f" registry '{remote}'"
        )
        fdp_sync.push_from_config(
            fdp_conf.get_local_uri(),
            fdp_conf.get_remote_uri(self._session_loc, remote),
            fdp_conf.get_remote_token(self._session_loc, remote),
            self._session_config
        )

    def purge(
        self,
        verbose: bool = True,
        local_cfg: bool = False,
        global_cfg: bool = False,
        clear_data: bool = False
        ) -> None:
        """Remove FAIR-CLI tracking from the given directory

        Parameters
        ==========
        local_cfg : bool, optional
            remove local directories, default is False
        global_cfg : bool, optional
            remove global directories, default is False
        verbose : bool, optional
            run in verbose mode, default is True
        clear_data : bool, optional
            remove the data directory (potentially dangerous), default is False
        """
        _root_dir = os.path.join(fdp_com.find_fair_root(self._session_loc), fdp_com.FAIR_FOLDER)
        if local_cfg and os.path.exists(_root_dir):
            if verbose:
                click.echo(f"Removing directory '{_root_dir}'")
            shutil.rmtree(_root_dir)
        if clear_data:
            try:
                if verbose:
                    click.echo(
                        f"Removing directory '{fdp_com.default_data_dir()}'"
                    )
                if os.path.exists(fdp_com.default_data_dir()):
                    shutil.rmtree(fdp_com.default_data_dir())
            except FileNotFoundError:
                raise fdp_exc.FileNotFoundError(
                    "Cannot remove local data store, a global CLI configuration "
                    "is required to identify its location"
                )
        if global_cfg:
            if verbose:
                click.echo(f"Removing directory '{fdp_com.global_config_dir()}'")
            _global_dirs = fdp_com.global_config_dir()
            if os.path.exists(_global_dirs):
                shutil.rmtree(_global_dirs)

    def _setup_server(self) -> None:
        """Start or stop the server if required"""

        # If a session ID has been specified this means the server is auto
        # started as opposed to being started explcitly by the user
        # this means it will be shut down on completion
        self._logger.debug(f"Running server setup for run mode {self._run_mode}")
        if self._run_mode == fdp_serv.SwitchMode.CLI:
            self._setup_server_cli_mode()
        elif self._run_mode == fdp_serv.SwitchMode.USER_START:
            self._setup_server_user_start()
        elif self._run_mode in [
            fdp_serv.SwitchMode.USER_STOP,
            fdp_serv.SwitchMode.FORCE_STOP,
        ]:
            _cache_addr = os.path.join(fdp_com.session_cache_dir(), 'user.run')
            if not fdp_serv.check_server_running():
                raise fdp_exc.UnexpectedRegistryServerState(
                    "Server is not running."
                )
            if os.path.exists(_cache_addr):
                os.remove(_cache_addr)
            click.echo("Stopping local registry server.")
            fdp_serv.stop_server(
                force=self._run_mode == fdp_serv.SwitchMode.FORCE_STOP,
            )

    def _setup_server_cli_mode(self):
        self.check_is_repo()
        _cache_addr = os.path.join(
            fdp_com.session_cache_dir(), f"{self._session_id}.run"
        )

        self._logger.debug("Checking for existing sessions")
        # If there are no session cache files start the server
        if not glob.glob(
            os.path.join(fdp_com.session_cache_dir(), "*.run")
        ):
            self._logger.debug("No sessions found, launching server")
            fdp_serv.launch_server()

        self._logger.debug(f"Creating new session #{self._session_id}")

        if not os.path.exists(fdp_com.session_cache_dir()):
            raise fdp_exc.InternalError(
                "Failed to create session cache file, "
                f"expected cache directory '{fdp_com.session_cache_dir()}' to exist"
            )

        # Create new session cache file
        pathlib.Path(_cache_addr).touch()

    def _setup_server_user_start(self):
        if not os.path.exists(fdp_com.session_cache_dir()):
            os.makedirs(fdp_com.session_cache_dir())

        _cache_addr = os.path.join(fdp_com.session_cache_dir(), 'user.run')

        if "registries" not in self._global_config:
            raise fdp_exc.CLIConfigurationError(
                "Cannot find server address in current configuration",
                hint="Is the current location a FAIR repository?"
            )

        if fdp_serv.check_server_running():
            raise fdp_exc.UnexpectedRegistryServerState(
                "Server already running."
            )
        click.echo("Starting local registry server")
        pathlib.Path(_cache_addr).touch()
        fdp_serv.launch_server(fdp_conf.get_local_uri(), verbose=True)

    def run_job(
        self,
        bash_cmd: str = "",
        mode: fdp_run.CMD_MODE = fdp_run.CMD_MODE.RUN
    ) -> str:
        """Execute a run using the given user configuration file"""
        self.check_is_repo()
        if not os.path.exists(self._session_config):
            self.make_starter_config()
        
        self._logger.debug("Setting up command execution")

        _hash = fdp_run.run_command(
            repo_dir=self._session_loc,
            config_yaml=self._session_config,
            bash_cmd=bash_cmd,
            mode=mode
        )

        self._logger.debug(f"Tracking job hash {_hash}")

        # Automatically add the run to tracking but unstaged
        self._stager.change_job_stage_status(_hash, False)

        return _hash

    def check_is_repo(self, location: str = None) -> None:
        """Check that the current location is a FAIR repository"""
        if not location:
            location = self._session_loc
        if not fdp_com.find_fair_root(location):
            raise fdp_exc.FDPRepositoryError(
                f"'{location}' is not a FAIR repository", hint="Run 'fair init' to initialise."
            )

    def check_git_repo_state(self, git_repo: str, remote_label: str = 'origin') -> bool:
        """Checks the git repository is clean and that local matches remote"""
        _repo_root = fdp_com.find_git_root(git_repo)
        _repo = git.Repo(_repo_root)

        # Firstly get the current branch
        _current_branch = _repo.active_branch.name

        # Get the latest commit on the current branch locally
        _loc_commit = _repo.refs[_current_branch].commit.hexsha

        # Get the latest commit on this branch on remote
        _rem_commit = _repo.remotes[remote_label].refs[_current_branch].commit.hexsha

        # Commit match
        _com_match = _loc_commit == _rem_commit

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

    def change_staging_state(
        self, job_to_stage: str, stage: bool = True
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
        self._stager.change_job_stage_status(job_to_stage, stage)

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
        self,
        remote_url: str,
        token_file: str,
        label: str = "origin"
        ) -> None:
        """Add a remote to the list of remote URLs"""
        self.check_is_repo()
        if "registries" not in self._local_config:
            self._local_config['registries'] = {}
        if label in self._local_config['registries']:
            raise fdp_exc.CLIConfigurationError(
                f"Registry remote '{label}' already exists."
            )
        self._local_config['registries'][label] = {
            'uri': remote_url,
            'token': token_file
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
            or label not in self._local_config['registries']
        ):
            raise fdp_exc.CLIConfigurationError(
                f"No such entry '{label}' in available remotes"
            )
        self._local_config['registries'][label]['uri'] = url

    def clear_logs(self) -> None:
        """Delete all local run stdout logs

        This does NOT delete any information from the registry
        """
        _log_files = glob.glob(
            fdp_hist.history_directory(self._session_loc), "*.log"
        )
        if _log_files:
            for log in _log_files:
                os.remove(log)

    def list_remotes(self, verbose: bool = False) -> typing.List[str]:
        """List the available RestAPI URLs"""
        self.check_is_repo()
        if "registries" not in self._local_config:
            return []
        _remote_print = []
        for remote, data in self._local_config['registries'].items():
            _out_str = f"[bold white]{remote}[/bold white]"
            if verbose:
                _out_str += f"\t[yellow]{data['uri']}[/yellow]"
            _remote_print.append(_out_str)
        rich.print("\n".join(_remote_print))
        return _remote_print

    def status(self, verbose: bool = False) -> None:
        """Get the status of staging"""
        self._logger.debug("Updating staging status")
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
                        click.echo(
                            click.style(f"\t\t\t\t{value}", fg="green")
                        )

        if _unstaged_jobs:
            click.echo("Changes not staged for synchronization:")
            click.echo('\t(use "fair add <job>..." to stage jobs)')

            click.echo("\tJobs:")

            for job in _unstaged_jobs:
                click.echo(click.style(f"\t\t{job}", fg="red"))
                _job_urls = self._stager.get_job_data(
                    fdp_conf.get_local_uri(),
                    job
                )

                if not verbose:
                    continue

                for key, value in _job_urls.items():
                    if not value:
                        continue
                    click.echo(
                        click.style(
                            f"\t\t\t{key.replace('_', ' ').title()}:",
                            fg="red"
                        )
                    )
                    if isinstance(value, list):
                        for url in value:
                            click.echo(
                                click.style(f"\t\t\t\t{url}", fg="red")
                            )
                    else:
                        click.echo(
                            click.style(f"\t\t\t\t{value}", fg="red")
                        )

        if not _unstaged_jobs and not _staged_jobs:
            click.echo("Nothing marked for tracking.")

    def make_starter_config(self, output_file_name: str = None) -> None:
        """Create a starter config.yaml"""
        if not output_file_name:
            output_file_name = os.path.join(self._session_loc, fdp_com.USER_CONFIG_FILE)
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
                local_repo=os.path.abspath(fdp_com.find_fair_root(self._session_loc)),
            )
            _yaml_dict = yaml.safe_load(_yaml_str)

            yaml.dump(_yaml_dict, f, sort_keys = False)

    def _export_cli_configuration(self, output_file: str) -> None:
        _cli_config = fdp_conf.read_global_fdpconfig()
        _loc_config = fdp_conf.read_local_fdpconfig(self._session_loc)
        _cli_config['git'] = _loc_config['git']
        _cli_config['registries'].update(_loc_config['registries'])
        _cli_config['user'].update(_loc_config['user'])
        with open(output_file, 'w') as f:
            yaml.dump(_cli_config, f)

    def initialise(
        self,
        using: typing.Dict = None,
        registry: str = None,
        export_as: str = None
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
                os.getcwd()
            )

        if os.path.exists(_fair_dir):
            if export_as:
                self._export_cli_configuration(export_as)
                return
            click.echo('FAIR repository is already initialised.')
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
                    registry
                )
                self._local_config = fdp_conf.local_config_query(
                    self._global_config, first_time_setup=_first_time
                )
            except (fdp_exc.CLIConfigurationError, click.Abort) as e:
                self._clean_reset(_fair_dir, e)
        elif not using:
            try:
                self._local_config = fdp_conf.local_config_query(
                    self._global_config
                )
            except (fdp_exc.CLIConfigurationError, click.Abort) as e:
                self._clean_reset(_fair_dir, e)
        if not using:
            with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
                yaml.dump(self._local_config, f)
            with open(fdp_com.global_fdpconfig(), "w") as f:
                yaml.dump(self._global_config, f)
        else:
            self._global_config = fdp_conf.read_global_fdpconfig()
            self._local_config = fdp_conf.read_local_fdpconfig(self._session_loc)

        if export_as:
            self._export_cli_configuration(export_as)

        fdp_serv.update_registry_post_setup(self._session_loc, _first_time)

        click.echo(f"Initialised empty fair repository in {_fair_dir}")

    def _clean_reset(self, _fair_dir, e):
        shutil.rmtree(fdp_com.session_cache_dir(), ignore_errors=True)
        shutil.rmtree(fdp_com.global_config_dir(), ignore_errors=True)
        shutil.rmtree(_fair_dir)
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

        if (not os.path.exists(os.path.join(fdp_com.session_cache_dir(), "user.run"))
            and self._run_mode != fdp_serv.SwitchMode.NO_SERVER
        ):
            fdp_serv.stop_server()

        with open(fdp_com.global_fdpconfig(), "w") as f:
            yaml.dump(self._global_config, f)
        with open(fdp_com.local_fdpconfig(self._session_loc), "w") as f:
            yaml.dump(self._local_config, f)

    def _validate_and_load_cli_config(self, cli_config: typing.Dict):
        _exp_keys = [
            'registries',
            'namespaces',
            'user',
            'git'
        ]

        for key in _exp_keys:
            if key not in cli_config:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key '{key}' in CLI configuration file"
                )

        for exp_reg in ['local', 'origin']:
            if exp_reg not in cli_config['registries']:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'registries:{exp_reg}' in CLI configuration file"
                )

        for name, reg in cli_config['registries'].items():
            if 'data_store' not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key '{key}' for remote '{name}' "
                    "in CLI configuration"
                )
            if name != 'local' and 'uri' not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'uri' for remote '{name}' "
                    "in CLI configuration"
                )
            if name != 'local' and 'token' not in reg:
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'token' for remote '{name}' "
                    "in CLI configuration"
                )
            if name == 'local' and 'directory' not in reg:
                raise fdp_exc.CLIConfigurationError(
                    "Expected key 'directory' for local registry in CLI configuration"
                )


        _user_keys = ['email', 'family_name', 'given_names', 'orcid', 'uuid']

        for key in _user_keys:
            if key not in cli_config['user']:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'user:{key}' in CLI configuration file"
                )

        if not cli_config['user']['orcid'] and not cli_config['user']['uuid']:
            raise fdp_exc.CLIConfigurationError(
                "At least one of 'user:orcid' and 'user:uuid' must be provided "
                " in CLI configuration"
            )

        for key in ['local_repo', 'remote']:
            if key not in cli_config['git']:
                self.purge(verbose=False)
                raise fdp_exc.CLIConfigurationError(
                    f"Expected key 'git:{key}' in CLI configuration"
                )

        _glob_cfg = copy.deepcopy(cli_config)
        _loc_cfg = copy.deepcopy(cli_config)
        del _glob_cfg['git']
        if 'description' in _glob_cfg:
            del _glob_cfg['description']
        del _loc_cfg['registries']['local']

        with open(fdp_com.global_fdpconfig(), 'w') as f:
            yaml.dump(_glob_cfg, f)

        with open(fdp_com.local_fdpconfig(self._session_loc), 'w') as f:
            yaml.dump(_loc_cfg, f)

    def __exit__(self, *args) -> None:
        self.close_session()
