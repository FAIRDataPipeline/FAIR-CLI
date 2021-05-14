from os.path import abspath
import os
import sys
from pathlib import Path
from typing import Dict

import rich
import toml
import click


class DANTE:
    LOCAL_FOLDER = ".dante"
    GLOBAL_FOLDER = os.path.join(Path.home(), ".scrc")

    def __init__(self) -> None:
        if not os.path.exists(self.GLOBAL_FOLDER):
            os.makedirs(self.GLOBAL_FOLDER)
        self._here = self._find_dante_root()
        self._staging_file = os.path.join(self._here, "staging") if self._here else ""
        self._local_config_file = (
            os.path.join(self._here, "config") if self._here else ""
        )
        self._stage_status: Dict = {}
        self._local_config: Dict = {}

    def check_is_repo(self) -> None:
        if not self._find_dante_root():
            click.echo("fatal: not a dante repository, run 'dante init' to initialise")
            sys.exit(1)

    def _find_dante_root(self) -> str:
        """Locate the .dante folder within the current hierarchy

        Returns
        -------
        str
            absolute path of the .dante folder
        """
        _current_dir = os.getcwd()

        # Keep upward searching until you find '.dante', stop at the level of
        # the user's home directory
        while _current_dir != Path.home():
            _dante_dir = os.path.join(_current_dir, self.LOCAL_FOLDER)
            if os.path.exists(_dante_dir):
                return os.path.abspath(_dante_dir)
            _current_dir = Path(_current_dir).parent
        return ""

    def __enter__(self) -> None:
        if os.path.exists(self._staging_file):
            self._stage_status = toml.load(self._staging_file)
        if os.path.exists(self._local_config_file):
            self._local_config = toml.load(self._local_config_file)
        if not self._stage_status:
            self._stage_status = {}
        if not self._local_config:
            self._local_config = {}
        return self

    def change_staging_state(self, file_to_stage: str, stage=True) -> None:
        self.check_is_repo()
        if not os.path.exists(file_to_stage):
            click.echo(f"No such file '{file_to_stage}.")
            sys.exit(1)
        _label = os.path.relpath(file_to_stage, os.path.dirname(self._staging_file))
        self._stage_status[_label] = stage

    def set_configuration(self):
        self.check_is_repo()

    def remove_file(self, file_name: str, cached: bool = False) -> None:
        self.check_is_repo()
        _label = os.path.relpath(file_name, os.path.dirname(self._staging_file))
        if _label in self._stage_status:
            del self._stage_status[_label]
        else:
            click.echo(f"File '{file_name}' is not tracked, so will not be removed")

        if not cached:
            os.remove(file_name)

    def add_remote(self, remote_url: str, label: str = "origin") -> None:
        self.check_is_repo()
        if "remotes" not in self._local_config:
            self._local_config["remotes"] = {}
        if label in self._local_config["remotes"]:
            click.echo(f"error: registry remote '{label}' already exists.")
            sys.exit(1)
        self._local_config["remotes"][label] = remote_url

    def remove_remote(self, label: str) -> None:
        self.check_is_repo()
        if "remotes" not in self._local_config or label not in self._local_config:
            self.fail(f"No such entry '{label}' in available remotes")
            sys.exit(1)
        del self._local_config[label]

    def modify_remote(self, label: str, url: str) -> None:
        self.check_is_repo()
        if "remotes" not in self._local_config or label not in self._local_config:
            click.echo(f"No such entry '{label}' in available remotes")
            sys.exit(1)
        self._local_config[label] = url

    def purge(self) -> None:
        if not os.path.exists(self._staging_file) and not os.path.exists(
            self._local_config_file
        ):
            click.echo("No dante tracking has been initialised")
        else:
            os.remove(self._staging_file)
            os.remove(self._local_config_file)

    def list_remotes(self, verbose: bool = False) -> None:
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
        self.check_is_repo()
        _staged = [i for i, j in self._stage_status.items() if j]
        _unstaged = [i for i, j in self._stage_status.items() if not j]

        if _staged:
            click.echo("Changes to be synchronized:")

            for file_name in _staged:
                rich.print(f"[green]\t\t{file_name}[/]")

        if _unstaged:
            click.echo("Files not staged for synchronization:")
            click.echo(f'\t(use "dante add <file>..." to stage files)')

            for file_name in _unstaged:
                rich.print(f"[red]\t\t{file_name}[/]")

    def initialise(self) -> None:
        """Initialise an dante repository within the current location"""
        _dante_dir = os.path.abspath(os.path.join(os.getcwd(), ".dante"))
        try:
            os.mkdir(_dante_dir)
        except FileExistsError:
            click.echo(f"fatal: dante repository is already initialised.")
            sys.exit(1)
        click.echo(f"Initialised empty dante repository in {_dante_dir}")
        self._local_config_file = os.path.join(_dante_dir, "config")
        self._staging_file = os.path.join(_dante_dir, "staging")

    def __exit__(self, *args) -> None:
        if not os.path.exists(self.LOCAL_FOLDER):
            return
        with open(self._staging_file, "w") as f:
            toml.dump(self._stage_status, f)
        with open(self._local_config_file, "w") as f:
            toml.dump(self._local_config, f)
