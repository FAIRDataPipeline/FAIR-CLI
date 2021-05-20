import os
import glob
import sys
import hashlib
import rich
from pathlib import Path
from typing import Dict
from datetime import date, datetime

import toml
import yaml
import click
import socket
import subprocess

from dante.templates import config_template, hist_template


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
        self._soft_data_dir = os.path.join(self._here, ".dante", "data")
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
                return _dante_dir
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

    def show_history(self) -> None:
        _all_log_files = glob.glob(os.path.join(self._here, "logs", "*"))
        for log in _all_log_files:
            _run_id = hashlib.sha1(open(log).read().encode("utf-8")).hexdigest()
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
        _now = datetime.now()
        _timestamp = _now.strftime("%Y-%m-%d_%H_%M_%S")
        _logs_dir = os.path.join(self._here, "logs")
        if not os.path.exists(_logs_dir):
            os.mkdir(_logs_dir)
        _log_file = os.path.join(_logs_dir, f"run_{_timestamp}.log")
        _cfg_yml = os.path.join(Path(self._here).parent, "config.yaml")

        if not os.path.exists(_cfg_yml):
            click.echo("error: expected file 'config.yaml' at head of repository")
            sys.exit(1)

        with open(_cfg_yml) as f:
            _cfg = yaml.load(f, Loader=yaml.SafeLoader)
            assert _cfg

        if "run_metadata" not in _cfg or "script" not in _cfg["run_metadata"]:
            click.echo("error: failed to find executable method in configuration")
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
        _user = self._local_config["user"]["name"]
        _email = self._local_config["user"]["email"]
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
        _end_time = datetime.now()
        with open(_log_file, "a") as f:
            _duration = _end_time - _now
            f.writelines([f"------- time taken {_duration} -------\n"])

        if _process.returncode != 0:
            sys.exit(_process.returncode)

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
                click.echo(click.style(f"\t\t{file_name}", fg="green"))

        if _unstaged:
            click.echo("Files not staged for synchronization:")
            click.echo(f'\t(use "dante add <file>..." to stage files)')

            for file_name in _unstaged:
                click.echo(click.style(f"\t\t{file_name}", fg="red"))

    def set_email(self, email: str) -> None:
        self._local_config["user"]["email"] = email

    def set_user(self, name: str) -> None:
        self._local_config["user"]["name"] = name

    def _config_query(self) -> None:
        click.echo(
            "Initialising DANTE repository, setup will now ask for basic info:\n"
        )
        _def_name = socket.gethostname()
        _def_local = "http://localhost:8000/api/"
        _desc = click.prompt("Project description")
        _user_name = click.prompt(f"User name", default=_def_name)
        _user_email = click.prompt("User email")
        _remote_url = click.prompt(f"Remote API URL")
        _local_url = click.prompt(f"Local API URL", default=_def_local)
        if len(_user_name.strip().split()) == 2:
            _def_ospace = _user_name.strip().lower().split()
            _def_ospace = _def_ospace[0][0] + _def_ospace[1]
        else:
            _def_ospace = _user_name.lower().replace(" ", "")

        self._local_config["namespaces"] = {"output": _def_ospace, "input": "null"}
        self._local_config["user"] = {"name": _user_name, "email": _user_email}

        self._local_config["remotes"] = {"origin": _remote_url, "local": _local_url}
        self._local_config["description"] = _desc

    def _make_starter_config(self) -> None:
        """Create a starter config.yaml"""
        if "remotes" not in self._local_config:
            click.echo(
                "Cannot generate config.yaml, you need to set the remote URL by running: \n"
                "\n\tdante remote add <url>\n"
            )
            sys.exit(1)
        print(self._here)
        with open(os.path.join(Path(self._here).parent, "config.yaml"), "w") as f:
            f.write(
                config_template.render(
                    instance=self, local_repo=os.path.abspath(self._here)
                )
            )

    def initialise(self) -> None:
        """Initialise an dante repository within the current location"""
        _dante_dir = os.path.abspath(os.path.join(os.getcwd(), ".dante"))

        if os.path.exists(_dante_dir):
            click.echo(f"fatal: dante repository is already initialised.")
            sys.exit(1)

        self._local_config_file = os.path.join(_dante_dir, "config")
        self._staging_file = os.path.join(_dante_dir, "staging")
        self._config_query()

        os.mkdir(_dante_dir)

        with open(self._local_config_file, "w") as f:
            toml.dump(self._local_config, f)
        self._make_starter_config()
        click.echo(f"Initialised empty dante repository in {_dante_dir}")

    def __exit__(self, *args) -> None:
        if not os.path.exists(self.LOCAL_FOLDER):
            return
        with open(self._staging_file, "w") as f:
            toml.dump(self._stage_status, f)
        with open(self._local_config_file, "w") as f:
            toml.dump(self._local_config, f)
