#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Execute run
===========

Creates required files for a model run and executes the run itself using the
retrieved metadata


Contents
========

Functions
-------

    create_working_config  -  creation of working config.yaml

"""

__date__ = "2021-06-24"

import os
import sys
import glob
import platform
import re
from collections.abc import Mapping
from typing import Dict, Any
from datetime import datetime

import git
import yaml
import click
import subprocess

import fair.configuration as fdp_conf
import fair.common as fdp_com
import fair.utilities as fdp_util
import fair.history as fdp_hist


SHELLS: Dict[str, str] = {
    "pwsh": "pwsh -command \". '{0}'\"",
    "python": "python",
    "python3": "python3",
    "R": "R -f",
    "julia": "julia",
    "bash": "bash -eo pipefail {0}",
    "sh": "sh -e {0}",
    "powershell": "powershell -command \". '{0}'\".",
}


def run_command(
    run_dir: str,
    config_yaml: str = os.path.join(fdp_com.find_fair_root(), "config.yaml"),
    bash_cmd: str = "",
) -> None:
    """Execute a process as part of a model run

    Executes a command from the given run config file, if a command is
    given this is run instead and overwrites that in the run config file.

    Parameters
    ----------
    run_dir : str
        directory of repository to run from
    config_yaml : str, optional
        run from a given config.yaml file
    bash_cmd : str, optional
        override execution command with a bash command
    """
    # Record the time the run was executed, create a log and both
    # print output and write it to the log file
    _now = datetime.now()
    _timestamp = _now.strftime("%Y-%m-%d_%H_%M_%S")
    _logs_dir = fdp_hist.history_directory()
    if not os.path.exists(_logs_dir):
        os.mkdir(_logs_dir)
    _log_file = os.path.join(_logs_dir, f"run_{_timestamp}.log")

    # Check that the specified user config file for a run actually exists
    if not os.path.exists(config_yaml):
        click.echo("Error: expected file 'config.yaml' at head of repository")
        sys.exit(1)

    with open(config_yaml) as f:
        _cfg = yaml.load(f)

    if not _cfg:
        click.echo(
            "Error: Failed to load specified 'config.yaml' file, "
            "contents empty."
        )
        sys.exit(1)

    if "run_metadata" not in _cfg or (
        "script" not in _cfg["run_metadata"]
        and "script_path" not in _cfg["run_metadata"]
    ):
        click.echo(
            "Error: failed to find executable method in specified "
            "'config.yaml', expected either key 'script' or 'script_path'"
            " with valid values."
        )
        sys.exit(1)

    # Create a new timestamped directory for the run
    # use the key 'data_store' from the 'config.yaml' if
    # specified else revert to the
    if "data_store" in _cfg["run_metadata"]:
        _run_dir = _cfg["run_metadata"]["data_store"]
    else:
        _run_dir = fdp_com.default_coderun_dir()

    _run_dir = os.path.join(_run_dir, _timestamp)
    os.makedirs(_run_dir)

    # Set location of working config.yaml to the run directory
    _work_cfg_yml = os.path.join(_run_dir, "config.yaml")

    # Create working config
    create_working_config(_run_dir, config_yaml, _work_cfg_yml, _now)

    # If a bash command is specified, save it to the configuration
    # and use this during the run
    if bash_cmd:
        _cfg["run_metadata"]["script"] = bash_cmd

        _work_cfg = yaml.load(open(_work_cfg_yml))
        _work_cfg["run_metadata"]["script"] = bash_cmd

        with open(config_yaml, "w") as f:
            yaml.dump(_cfg, f)
        with open(_work_cfg_yml, "w") as f:
            yaml.dump(_work_cfg, f)

    # Create a run script if 'script' is specified instead of 'script_path'
    # else use the script
    _cmd_setup = setup_run_script(_work_cfg_yml, _run_dir)
    _shell = _cmd_setup["shell"]

    if _shell not in SHELLS:
        click.echo(f"Error: Unrecognised shell '{_shell}' specified.")
        sys.exit(1)

    _cmd_list = _shell.format(_cmd_setup["script"]).split()

    _run_meta = _cfg["run_metadata"]

    if (
        "script" not in _run_meta.keys()
        and "script_path" not in _run_meta.keys()
    ):
        click.echo("Nothing to run.")
        sys.exit(0)

    # Fetch the CLI configurations for logging information
    _glob_conf = fdp_conf.read_global_fdpconfig()
    _loc_conf = fdp_conf.read_local_fdpconfig(run_dir)
    _user = _glob_conf["user"]["name"]
    _email = _glob_conf["user"]["email"]
    _namespace = _loc_conf["namespaces"]["output"]

    # Generate a local run log for the CLI, this is NOT
    # related to metadata sent to the registry
    # this log is viewable via the `fair view <run-cli-sha>`
    with open(_log_file, "a") as f:
        _out_str = _now.strftime("%a %b %d %H:%M:%S %Y %Z")
        f.writelines(
            [
                "--------------------------------\n",
                f" Commenced = {_out_str}\n",
                f" Author    = {_user} <{_email}>\n",
                f" Namespace = {_namespace}\n",
                f" Command   = {' '.join(_cmd_list)}\n",
                "--------------------------------\n",
            ]
        )

    # Run the submission script/model run
    _process = subprocess.Popen(
        _cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
        text=True,
        shell=False,
        env=_cmd_setup["env"],
    )

    # Write any stdout to the CLI run log
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

    # Exit the session if the run failed
    if _process.returncode != 0:
        click.echo(f"Error: run failed with exit code '{_process.returncode}'")
        sys.exit(_process.returncode)


def create_working_config(
    run_dir: str, config_yaml: str, output_file: str, time: datetime
) -> None:
    """Generate a working configuration file used during runs

    Parameters
    ----------
    run_dir : str
        session run directory
    config_yaml : str
        user run configuration file
    output_file : str
        location to write generated config
    time : datetime.datetime
        time stamp of run initiation time
    """

    # Substitutes are defined as functions for which particular cases
    # can be given as arguments, e.g. for DATE the format depends on if
    # the key is a version key or not
    _substitutes: Mapping = {
        "DATE": lambda x: time.strftime(
            "%Y{0}%m{0}%d".format("" if "version" in x else "-"),
        ),
        "DATETIME": lambda x: time.strftime("%Y-%m-%s %H:%M:S"),
        "USER": fdp_conf.get_current_user,
        "REPO_DIR": lambda x: fdp_com.find_fair_root(
            os.path.dirname(config_yaml)
        ),
        "CONFIG_DIR": lambda x: run_dir,
        "SOURCE_CONFIG": lambda x: config_yaml,
        "GIT_BRANCH": lambda x: git.Repo(
            os.path.dirname(config_yaml)
        ).active_branch.name,
        "GIT_REMOTE_ORIGIN": lambda x: git.Repo(os.path.dirname(config_yaml))
        .remotes["origin"]
        .url,
        "GIT_TAG": lambda x: git.Repo(os.path.dirname(config_yaml))
        .tags[-1]
        .name,
    }

    _conf_yaml = yaml.load(open(config_yaml))

    # Remove 'register' from working configuration
    if "register" in _conf_yaml:
        del _conf_yaml["register"]

    # Flatten the nested YAML dictionary so it is easier to iterate
    _flat_conf = fdp_util.flatten_dict(_conf_yaml)

    # Construct Regex objects to find variables in the config
    _regex_star = re.compile(r":\s*(.+\*+)")
    _regex_var_candidate = re.compile(
        r"\$\{\{\s*cli\..+\s*\}\}", re.IGNORECASE
    )
    _regex_var = re.compile(r"\$\{\{\s*cli\.(.+)\s*\}\}")
    _regex_env_candidate = re.compile(r"\$\{?[0-9\_A-Z]+\}?", re.IGNORECASE)
    _regex_env = re.compile(r"\$\{?([0-9\_A-Z]+)\}?", re.IGNORECASE)

    for key, value in _flat_conf.items():
        if not isinstance(value, str):
            continue

        # Search for '${{ fair.variable }}' then also make an exclusive search
        # to extract 'variable'
        _var_search = _regex_var_candidate.findall(value)
        _var_label = _regex_var.findall(value)

        # The number of results for '${{fair.var}}' should match those
        # for the exclusive search for 'var'
        if not len(_var_search) == len(_var_label):
            click.echo("Error: FAIR variable matching failed")
            sys.exit(1)

        # Search for '${ENV_VAR}' then also make an exclusive search to extract
        # 'ENV_VAR' from that result
        _env_search = _regex_env_candidate.findall(value)
        _env_label = _regex_env.findall(value)

        # The number of results for '${ENV_VAR}' should match those
        # for the exclusive search for 'ENV_VAR'
        if not len(_env_search) == len(_env_label):
            click.echo("Error: environment variable matching failed")
            sys.exit(1)

        for entry, var in zip(_var_search, _var_label):
            if var.upper().strip() in _substitutes:
                _new_var = _substitutes[var.upper().strip()](key)
                _flat_conf[key] = value.replace(entry, _new_var)
            else:
                click.echo(
                    f"Error: Variable '{var}' is not a recognised FAIR config "
                    "variable, config.yaml substitution failed."
                )
                sys.exit(1)

        # Print warnings for environment variables which have been stated in
        # the config.yaml but are not actually present in the shell
        for entry, var in zip(_env_search, _env_label):
            _env = os.environ[var]
            if not _env:
                click.echo(
                    f"Warning: Environment variable '{var}' in config.yaml"
                    " is not defined in current shell."
                )
                continue

        # If '*' or '**' in value, expand into a list and
        # save to the same key
        if _regex_star.findall(value):
            _flat_conf[key] = glob.glob(value)

    _conf_yaml = fdp_util.expand_dict(_flat_conf)

    with open(output_file, "w") as out_f:
        yaml.dump(_conf_yaml, out_f)


def setup_run_script(config_yaml: str, output_dir: str) -> Dict[str, Any]:
    """Setup a run script from the given configuration.

    Checks the user configuration file for the required 'script' or 'script_path'
    keys and determines the process to be executed. Also sets up an environment
    usable when executing the submission script.

    Parameters
    ----------
    config_yaml : str
        user run configuration file
    output_dir : str
        location to store submission/run script

    Returns
    -------
    Dict[str, Any]
        a dictionary containing information on the command to execute,
        which shell to run it in and the environment to use
    """
    _conf_yaml = yaml.load(open(config_yaml))
    _cmd = None
    _run_env = os.environ.copy()

    # Create environment variable which users can refer to in their
    # submission scripts
    _run_env["FDP_LOCAL_REPO"] = _conf_yaml["run_metadata"]["local_repo"]

    # Check if a specific shell has been defined for the script
    _shell = None
    _out_file = None

    if "shell" in _conf_yaml["run_metadata"]:
        _shell = _conf_yaml["run_metadata"]["shell"]
    else:
        if platform.system() == "Windows":
            _shell = "pwsh"
        else:
            _shell = "bash"

    # TODO: Currently when "script" is specified the script is
    # written to a file with no suffix as this cannot be determined
    # by the shell choice or contents
    if "script" in _conf_yaml["run_metadata"]:
        _cmd = _conf_yaml["run_metadata"]["script"]
        _out_file = os.path.join(output_dir, "run_script")
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)

    elif "script_path" in _conf_yaml["run_metadata"]:
        _path = _conf_yaml["run_metadata"]["script_path"]
        if not os.path.exists(_path):
            click.echo(
                f"Error: Failed to execute run, script '{_path}' was not found, or"
                " failed to be created."
            )
            sys.exit(1)
        _cmd = open(_path).read()
        _out_file = os.path.join(output_dir, os.path.basename(_path))
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)

    if not _cmd or not _out_file:
        click.echo(
            "Error: Configuration file must contain either a valid "
            "'script' or 'script_path' entry under 'run_metadata'"
        )
        sys.exit(1)

    return {"shell": _shell, "script": _out_file, "env": _run_env}
