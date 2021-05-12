import yaml
import os
import sys
from pathlib import Path
from typing import Dict
import rich


class Staging:
    def __init__(self,
                 cache_dir: str = os.path.join(Path.home(), '.scrc')
                 ) -> None:
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        self._cache_file = os.path.join(cache_dir, '.fdp_staging')
        self._cache: Dict = {}

    def __enter__(self) -> None:
        self._cache = yaml.load(open(self._cache_file), Loader=yaml.SafeLoader)
        if not self._cache:
            self._cache = {}
        return self

    def change_staging_state(self, file_to_stage: str, stage=True) -> None:
        if not os.path.exists(file_to_stage):
            print(
                f"No such file '{file_to_stage}."
            )
            sys.exit(1)
        _label = os.path.relpath(
            file_to_stage,
            os.path.dirname(self._cache_file)
        )
        self._cache[_label] = stage

    def remove_file(self, file_name: str, cached: bool = False) -> None:
        _label = os.path.relpath(
            file_name,
            os.path.dirname(self._cache_file)
        )
        if _label in self._cache:
            del self._cache[_label]
        else:
            print(f"File '{file_name}' is not tracked, so will not be removed")

        if not cached:
            os.remove(file_name)

    def status(self) -> None:
        _staged = [i for i, j in self._cache.items() if j]
        _unstaged = [i for i, j in self._cache.items() if not j]

        if _staged:
            print('Changes to be synchronized:')

            for file_name in _staged:
                rich.print(f'[green]\t\t{file_name}[/]')

        if _unstaged:
            print('Files not staged for synchronization:')
            print(f'\t(use "fairdp add <file>..." to stage files)')

            for file_name in _unstaged:
                rich.print(f'[red]\t\t{file_name}[/]')

    def __exit__(self, *args) -> None:
        with open(self._cache_file, 'w') as f:
            yaml.dump(self._cache, f)
