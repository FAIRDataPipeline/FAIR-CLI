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
import os
import sys
import glob
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


def run_bash_command(
    config_yaml: str = os.path.join(fdp_com.find_fair_root(), "config.yaml"),
    bash_cmd: str = "",
) -> None:
    """Execute a bash process as part of a model run

    Executes a bash command from the given run config file, if a command is
    given this is run instead and overwrites that in the run config file.

    Parameters
    ----------
    config_yaml : str, optional
        run from a given config.yaml file
    bash_cmd : str, optional
        command to execute
    """
    # Record the time the run was executed, create a log and both
    # print output and write it to the log file
    _now = datetime.now()
    _timestamp = _now.strftime("%Y-%m-%d_%H_%M_%S")

    if not os.path.exists(config_yaml):
        click.echo("error: expected file 'config.yaml' at head of repository")
        sys.exit(1)

    with open(config_yaml) as f:
        _cfg = yaml.load(f, Loader=yaml.SafeLoader)
        assert _cfg

    if "run_metadata" not in _cfg or (
        "script" not in _cfg["run_metadata"]
        and "script_path" not in _cfg["run_metadata"]
    ):
        click.echo("Error: failed to find executable method in configuration")
        sys.exit(1)

    _run_dir = os.path.join(fdp_com.coderun_dir(), _timestamp)
    os.makedirs(_run_dir)

    _work_cfg_yml = os.path.join(_run_dir, "config.yaml")

    create_working_config(config_yaml, _work_cfg_yml, _now)

    if bash_cmd:
        _cfg["run_metadata"]["script"] = bash_cmd

        _work_cfg = yaml.load(_work_cfg_yml, Loader=yaml.SafeLoader)
        _work_cfg["run_metadata"]["script"] = bash_cmd

        with open(config_yaml, "w") as f:
            yaml.dump(_cfg, f)
        with open(_work_cfg_yml, "w") as f:
            yaml.dump(_work_cfg, f)

    _cmd_setup = setup_run_script(_work_cfg_yml, _run_dir)
    _cmd_list = [_cmd_setup["shell"], _cmd_setup["script"]]

    _run_meta = _cfg["run_metadata"]

    if (
        "script" not in _run_meta.keys()
        and "script_path" not in _run_meta.keys()
    ):
        click.echo("Nothing to run.")
        sys.exit(0)

    _glob_conf = fdp_conf.read_global_fdpconfig()
    _loc_conf = fdp_conf.read_local_fdpconfig()

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

    _process.wait()

    if _process.returncode != 0:
        sys.exit(_process.returncode)


def create_working_config(
    config_yaml: str, output_name: str, time: datetime
) -> None:

    _run_dir = os.path.join(
        fdp_com.coderun_dir(), time.strftime("%Y-%m-%d_%H_%M_%S")
    )

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
        "CONFIG_DIR": lambda x: _run_dir,
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

    _conf_yaml = yaml.load(open(config_yaml), Loader=yaml.SafeLoader)

    # Remove 'register' from working configuration
    if "register" in _conf_yaml:
        del _conf_yaml["register"]

    _flat_conf = fdp_util.flatten_dict(_conf_yaml)
    _regex_star = re.compile(r":\s*(.+\*+)")
    _regex_var_candidate = re.compile(r"\$\{\{\s*fair\..+\s*\}\}")
    _regex_var = re.compile(r"\$\{\{\s*fair\.(.+)\s*\}\}")
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

    with open(output_name, "w") as out_f:
        yaml.dump(_conf_yaml, out_f)


def setup_run_script(config_yaml: str, output_dir: str) -> Dict[str, Any]:
    _conf_yaml = yaml.load(open(config_yaml), Loader=yaml.SafeLoader)
    _cmd = None
    _run_env = os.environ.copy()

    # Remove the local repository path as it may contain the user's
    # file system in the address
    if "local_repo" not in _conf_yaml["run_metadata"]:
        click.echo("Error: No entry for key 'run_metadata:local_repo'")
        sys.exit(1)

    _run_env["FDP_LOCAL_REPO"] = _conf_yaml["run_metadata"]["local_repo"]

    del _conf_yaml["run_metadata"]["local_repo"]

    # Check if a specific shell has been defined for the script
    _shell = None
    _out_file = None

    if "shell" in _conf_yaml["run_metadata"]:
        _shell = _conf_yaml["run_metadata"]["shell"]
    else:
        _shell = "bash"

    # TODO: Currently when "script" is specified the script is
    # written to a file with no suffix as this cannot be determined
    # by the shell choice or contents
    if "script" in _conf_yaml["run_metadata"]:
        _cmd = _conf_yaml["run_metadata"]["script"]
        _out_file = os.path.join(output_dir, "run_script")
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
        with open(_out_file, "w") as f:
            f.write(_cmd)

    if not _cmd or not _out_file:
        click.echo(
            "Error: Configuration file must contain either a valid "
            "'script' or 'script_path' entry under 'run_metadata'"
        )
        sys.exit(1)

    return {"shell": _shell, "script": _out_file, "env": _run_env}
