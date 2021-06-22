import os
import sys
import typing
import subprocess
import requests

import click
import yaml
import socket

from fair.templates import config_template
import fair.common as fdp_com
import fair.run as fdp_run
import fair.configuration as fdp_conf

__doc__ = """
Manage synchronisation of data and metadata relating to runs using the
FAIR Data Pipeline system.

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
    A class which provides the main interface for managing runs and data
    transfer between FAIR Data Pipeline registries.

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

    def __init__(self) -> None:
        """Initialise instance of FAIR sync tool"""

        # Creates $HOME/.scrc if it does not exist
        if not os.path.exists(fdp_com.REGISTRY_HOME):
            click.echo(
                "Warning: User registry directory was not found, this could "
                "mean the local registry has not been installed."
            )

        if not os.path.exists(fdp_com.GLOBAL_CONFIG_DIR):
            os.makedirs(fdp_com.GLOBAL_CONFIG_DIR)

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
            fdp_com.REGISTRY_HOME, "scripts", "run_scrc_server"
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
                "Error: Failed to start local registry,"
                " no response from server"
            )
            sys.exit(1)

    def run(
        self,
        config_yaml: str = fdp_com.local_user_config(),
        bash_cmd: str = "",
    ) -> None:
        """Execute a run using the given user configuration file

        Parameters
        ----------
        config_yaml : str, optional
            user configuration file, defaults to FAIR repository config.yaml
        """
        self.check_is_repo()
        fdp_run.run_bash_command(config_yaml, bash_cmd)

    def _stop_server(self) -> None:
        """Stops the FAIR Data Pipeline local server"""
        # If the local registry server is not running ignore

        _server_stop_script = os.path.join(
            fdp_com.REGISTRY_HOME, "scripts", "stop_scrc_server"
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
        if not fdp_com.find_fair_root():
            click.echo(
                "fatal: not a fair repository, run 'fair init' to initialise"
            )
            sys.exit(1)

    def __enter__(self) -> None:
        """Method called when using 'with' statement.

        This ensures all configurations and staging statuses are read at the
        start of every session.

        """
        if os.path.exists(fdp_com.staging_cache()):
            self._stage_status = yaml.load(
                open(fdp_com.staging_cache()), Loader=yaml.BaseLoader
            )
        if os.path.exists(fdp_com.GLOBAL_FAIR_CONFIG):
            self._global_config = fdp_conf.read_global_fdpconfig()
        if os.path.exists(fdp_com.local_fdpconfig()):
            self._local_config = fdp_conf.read_local_fdpconfig()
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

        # Create a label with which to store the staging status of the given
        # file using its path with respect to the staging status file
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
            fdp_com.local_fdpconfig()
        ):
            click.echo("No fair tracking has been initialised")
        else:
            os.remove(fdp_com.staging_cache())
            os.remove(fdp_com.GLOBAL_FAIR_CONFIG)
            os.remove(fdp_com.local_fdpconfig())

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

    def _make_starter_config(self) -> None:
        """Create a starter config.yaml"""
        if "remotes" not in self._local_config:
            click.echo(
                "Cannot generate config.yaml, you need to set the remote URL"
                " by running: \n\n\tfair remote add <url>\n"
            )
            sys.exit(1)
        with open(
            os.path.join(fdp_com.find_fair_root(), "config.yaml"), "w"
        ) as f:
            _yaml_str = config_template.render(
                instance=self,
                data_dir=fdp_com.data_dir(),
                local_repo=os.path.abspath(fdp_com.find_fair_root()),
            )
            _yaml_dict = yaml.load(_yaml_str, Loader=yaml.BaseLoader)
            _yaml_dict["fail_on_hash_mismatch"] = True
            _yaml_dict["run_metadata"]["script"] = None
            yaml.dump(_yaml_dict, f)

    def initialise(self) -> None:
        """Initialise an fair repository within the current location"""
        _fair_dir = os.path.abspath(os.path.join(os.getcwd(), ".fair"))

        if os.path.exists(_fair_dir):
            click.echo(f"fatal: fair repository is already initialised.")
            sys.exit(1)

        self._staging_file = os.path.join(_fair_dir, "staging")

        click.echo(
            "Initialising FAIR repository, setup will now ask for basic info:\n"
        )

        if not os.path.exists(fdp_com.GLOBAL_FAIR_CONFIG):
            self._global_config_query()
            self._local_config_query(first_time_setup=True)
        else:
            self._local_config_query()

        os.mkdir(_fair_dir)

        with open(fdp_com.local_fdpconfig(), "w") as f:
            yaml.dump(self._local_config, f)
        self._make_starter_config()
        click.echo(f"Initialised empty fair repository in {_fair_dir}")

    def __exit__(self, *args) -> None:
        """Upon exiting, dump all configurations to file"""
        if not os.path.exists(fdp_com.FAIR_FOLDER):
            return
        self._stop_server()
        with open(fdp_com.staging_cache(), "w") as f:
            yaml.dump(self._stage_status, f)
        with open(fdp_com.GLOBAL_FAIR_CONFIG, "w") as f:
            yaml.dump(self._global_config, f)
        with open(fdp_com.local_fdpconfig(), "w") as f:
            yaml.dump(self._local_config, f)
