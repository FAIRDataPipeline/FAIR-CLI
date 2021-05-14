from os.path import abspath
import os
import sys
from pathlib import Path
from typing import Dict
import rich
import toml
import click


class DANTE:
    LOCAL_FOLDER = '.dante'
    GLOBAL_FOLDER = os.path.join(Path.home(), '.scrc')

    def __init__(self) -> None:
        if not os.path.exists(self.GLOBAL_FOLDER):
            os.makedirs(self.GLOBAL_FOLDER)
        self._staging_file = os.path.join(self.GLOBAL_FOLDER, '.dante_staging')
        self._local_config_file = os.path.join(self.LOCAL_FOLDER, 'config')
        self._stage_status: Dict = {}
        self._local_config: Dict = {}

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
        raise FileNotFoundError(
            "fatal: not an dante repository, run 'dante init' to initialise"
        )

    def __enter__(self) -> None:
        if os.path.exists(self._staging_file):
            self._stage_status = toml.load(
                self._staging_file
            )
        if os.path.exists(self._local_config_file):
            self._local_config = toml.load(
                self._local_config_file
            )
        if not self._stage_status:
            self._stage_status = {}
        if not self._local_config:
            self._local_config = {}
        return self

    def change_staging_state(self, file_to_stage: str, stage=True) -> None:
        if not os.path.exists(file_to_stage):
            click.echo(
                f"No such file '{file_to_stage}."
            )
            sys.exit(1)
        _label = os.path.relpath(
            file_to_stage,
            os.path.dirname(self._staging_file)
        )
        self._stage_status[_label] = stage

    def set_configuration():
        pass

    def remove_file(self, file_name: str, cached: bool = False) -> None:
        _label = os.path.relpath(
            file_name,
            os.path.dirname(self._staging_file)
        )
        if _label in self._stage_status:
            del self._stage_status[_label]
        else:
            click.echo(f"File '{file_name}' is not tracked, so will not be removed")

        if not cached:
            os.remove(file_name)

    def add_remote(self, label: str, remote_url: str) -> None:
        self._local_config['remotes'][label] = remote_url

    def status(self) -> None:
        _staged = [i for i, j in self._stage_status.items() if j]
        _unstaged = [i for i, j in self._stage_status.items() if not j]

        if _staged:
            click.echo('Changes to be synchronized:')

            for file_name in _staged:
                rich.print(f'[green]\t\t{file_name}[/]')

        if _unstaged:
            click.echo('Files not staged for synchronization:')
            click.echo(f'\t(use "dante add <file>..." to stage files)')

            for file_name in _unstaged:
                rich.print(f'[red]\t\t{file_name}[/]')

    def initialise(self) -> None:
        """Initialise an dante repository within the current location"""
        _dante_dir = os.path.abspath(os.path.join(os.getcwd(), '.dante'))
        os.mkdir(_dante_dir)
        click.echo(
            f'Initialised empty dante repository in {_dante_dir}'
        )

    def __exit__(self, *args) -> None:
        with open(self._staging_file, 'w') as f:
            yaml.dump(self._stage_status, f)
