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
from collections import Mapping
from datetime import datetime

import git
import yaml
import click
import subprocess

import fair.configuration as fdp_conf
import fair.common as fdp_com
import fair.history as fdp_hist
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
    _logs_dir = fdp_hist.history_directory()
    if not os.path.exists(_logs_dir):
        os.mkdir(_logs_dir)
    _log_file = os.path.join(_logs_dir, f"run_{_timestamp}.log")

    if not os.path.exists(config_yaml):
        click.echo("error: expected file 'config.yaml' at head of repository")
        sys.exit(1)

    with open(config_yaml) as f:
        _cfg = yaml.load(f, Loader=yaml.SafeLoader)
        assert _cfg

    if "run_metadata" not in _cfg or "script" not in _cfg["run_metadata"]:
        click.echo("error: failed to find executable method in configuration")
        sys.exit(1)

    create_working_config(config_yaml)

    if bash_cmd:
        _cfg["run_metadata"]["script"] = bash_cmd

        with open(config_yaml, "w") as f:
            yaml.dump(_cfg, f)

    _cmd = _cfg["run_metadata"]["script"]

    if not _cmd:
        click.echo("Nothing to run.")
        sys.exit(0)
    _cmd_list = _cfg["run_metadata"]["script"].split()
    _glob_conf = fdp_conf.read_global_fdpconfig()
    _loc_conf = fdp_conf.read_local_fdpconfig()
    _user = _glob_conf["user"]["name"]
    _email = _glob_conf["user"]["email"]
    _namespace = _loc_conf["namespaces"]["output"]

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
    _end_time = datetime.datetime.now()
    with open(_log_file, "a") as f:
        _duration = _end_time - _now
        f.writelines([f"------- time taken {_duration} -------\n"])

    if _process.returncode != 0:
        sys.exit(_process.returncode)


def create_working_config(config_yaml: str, output_name: str) -> None:

    # Substitutes are defined as functions which are called at creation
    # time to ensure they reflect environment properties during a run
    _substitutes: Mapping = Mapping(
        {
            "DATE": lambda x: datetime.strftime(
                datetime.now(),
                "%Y{0}%m{0}%d".format("" if "version" in x else "-"),
            ),
            "DATETIME": lambda x: datetime.now,
            "USER": fdp_conf.get_current_user,
            "REPO_DIR": lambda x: fdp_com.find_fair_root(
                os.path.dirname(config_yaml)
            ),
            "CONFIG_DIR": lambda x: os.path.dirname(config_yaml),
            "CONFIG_PATH": lambda x: config_yaml,
            "GIT_BRANCH": lambda x: git.Repo(
                os.path.dirname(config_yaml)
            ).active_branch.name,
            "GIT_REMOTE_ORIGIN": lambda x: git.Repo(
                os.path.dirname(config_yaml)
            )
            .remotes["origin"]
            .url,
            "GIT_TAG": lambda x: git.Repo(os.path.dirname(config_yaml))
            .tags[-1]
            .name,
        }
    )

    _conf_yaml = yaml.load(open(config_yaml), loader=yaml.BaseLoader)

    # Remove 'register' from working configuration
    if "register" in _conf_yaml:
        del _conf_yaml["register"]

    _flat_conf = fdp_util.flatten_dict(_conf_yaml)
    _regex_star = re.compile(r":\s*(.+\*+)")

    for key, value in _flat_conf.items():
        for subst, func in _substitutes.items():
            if "{" + subst + "}" in value:
                _flat_conf[key] = value.replace(subst, func(value))
            if _regex_star.findall(value):
                _flat_conf[key] = glob.glob(value)

    _conf_yaml = fdp_util.expand_dictionary(_flat_conf)

    with open(config_yaml, "w") as out_f:
        yaml.dump(_conf_yaml, out_f)


def setup_run_script(config_yaml: str, output_name: str) -> None:
    _conf_yaml = yaml.load(open(config_yaml), loader=yaml.BaseLoader)
    _cmd = None

    if "script" in _conf_yaml["run_metadata"]:
        _cmd = _conf_yaml["script"]
    elif "script_path" in _conf_yaml["run_metadata"]:
        _cmd = open(_conf_yaml["run_metadata"]["script_path"]).read()

    if not _cmd:
        click.echo(
            "Error: Configuration file must contain either a valid "
            "'script' or 'script_path' entry under 'run_metadata'"
        )
        sys.exit(1)

    with open(output_name, "w") as out_f:
        out_f.write(_cmd)
