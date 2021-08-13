#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Execute Job
===========

Creates required files for job execution and runs the job itself using the
retrieved metadata


Contents
========

Functions
-------

    create_working_config  -  creation of working config.yaml

"""

__date__ = "2021-06-30"

import os
import sys
import platform
import typing
import glob
import hashlib
from datetime import datetime

import yaml
import click
import subprocess

import fair.configuration as fdp_conf
import fair.common as fdp_com
import fair.history as fdp_hist
import fair.exceptions as fdp_exc
import fair.registry.storage as fdp_reg_store
import fair.parsing as fdp_parse


# Dictionary of recognised shell labels.
SHELLS: typing.Dict[str, str] = {
    "pwsh": "pwsh -command \". '{0}'\"",
    "python2": "python2 {0}",
    "python3": "python3 {0}",
    "python": "python {0}",
    "R": "R -f {0}",
    "julia": "julia {0}",
    "bash": "bash -eo pipefail {0}",
    "java": "java {0}",
    "sh": "sh -e {0}",
    "powershell": "powershell -command \". '{0}'\".",
}


def run_command(
    local_uri: str,
    repo_dir: str,
    config_yaml: str = os.path.join(fdp_com.find_fair_root(), "config.yaml"),
    bash_cmd: str = "",
) -> str:
    """Execute a process as part of job

    Executes a command from the given job config file, if a command is
    given this is job instead and overwrites that in the job config file.

    Parameters
    ----------
    local_uri : str
        local registry endpoint
    repo_dir : str
        directory of repository to run from
    config_yaml : str, optional
        run from a given config.yaml file
    bash_cmd : str, optional
        override execution command with a bash command

    Returns
    -------
        str
            job hash
    """
    # Record the time the job was commenced, create a log and both
    # print output and write it to the log file
    _now = datetime.now()
    _timestamp = _now.strftime("%Y-%m-%d_%H_%M_%S_%f")
    _logs_dir = fdp_hist.history_directory(repo_dir)
    if not os.path.exists(_logs_dir):
        os.mkdir(_logs_dir)
    _log_file = os.path.join(_logs_dir, f"job_{_timestamp}.log")

    # Check that the specified user config file for a job actually exists
    if not os.path.exists(config_yaml):
        raise fdp_exc.FileNotFoundError(
            "Failed to read user configuration, "
            f"file '{config_yaml}' does not exist."
        )

    with open(config_yaml) as f:
        _cfg = yaml.safe_load(f)

    if not _cfg:
        raise fdp_exc.UserConfigError(
            f"Failed to load file '{config_yaml}', contents empty."
        )

    if 'run_metadata' not in _cfg:
        raise fdp_exc.UserConfigError(
            f"Failed to find 'run_metadata' in config '{config_yaml}'"
        )

    # If a bash command is specified, override the config.yaml with it
    if bash_cmd:
        _cfg['run_metadata']['script'] = bash_cmd
        yaml.dump(_cfg, open(config_yaml, 'w'))

    if (
        "script" not in _cfg["run_metadata"]
        and "script_path" not in _cfg["run_metadata"]
    ):
        raise fdp_exc.UserConfigError(
            "Failed to find executable method in specified "
            "'config.yaml', expected either key 'script' or 'script_path'"
            " with valid values."
        )

    # Create a new timestamped directory for the job
    # use the key 'write_data_store' from the 'config.yaml' if
    # specified else revert to the
    if "write_data_store" in _cfg["run_metadata"]:
        _job_dir = os.path.join(
            _cfg["run_metadata"]["write_data_store"], fdp_com.JOBS_DIR
        )
    else:
        _job_dir = fdp_com.default_jobs_dir()

    _job_dir = os.path.join(_job_dir, _timestamp)
    os.makedirs(_job_dir, exist_ok=True)

    # Set location of working config.yaml to the job directory
    _work_cfg_yml = os.path.join(_job_dir, "config.yaml")

    # Create working config
    create_working_config(
        local_uri, _job_dir, config_yaml, _work_cfg_yml, _now
    )

    if not os.path.exists(_work_cfg_yml):
        raise fdp_exc.InternalError(
            "Failed to create working config.yaml in job folder"
        )

    # Setup the registry storage location root
    fdp_reg_store.store_working_config(
        repo_dir,
        fdp_conf.read_local_fdpconfig(repo_dir)["remotes"]["local"],
        _work_cfg_yml,
    )

    # Create a run script if 'script' is specified instead of 'script_path'
    # else use the script
    _cmd_setup = setup_job_script(_work_cfg_yml, _job_dir)
    _shell = _cmd_setup["shell"]

    fdp_reg_store.store_working_script(
        repo_dir,
        fdp_conf.read_local_fdpconfig(repo_dir)["remotes"]["local"],
        _cmd_setup["script"],
        _work_cfg_yml
    )

    if _shell not in SHELLS:
        raise fdp_exc.UserConfigError(
            f"Unrecognised shell '{_shell}' specified."
        )

    _cmd_list = SHELLS[_shell].format(_cmd_setup["script"]).split()

    _run_meta = _cfg["run_metadata"]

    if (
        "script" not in _run_meta.keys()
        and "script_path" not in _run_meta.keys()
    ):
        click.echo("Nothing to run.")
        sys.exit(0)

    # Fetch the CLI configurations for logging information
    _glob_conf = fdp_conf.read_global_fdpconfig()

    if not _glob_conf:
        raise fdp_exc.CLIConfigurationError(
            "Global configuration is empty",
            hint="You may need to re-setup FAIR-CLI on this system"
        )

    _loc_conf = fdp_conf.read_local_fdpconfig(repo_dir)
    _user = fdp_conf.get_current_user_name(repo_dir)
    _email = _glob_conf["user"]["email"]
    _namespace = _loc_conf["namespaces"]["output"]

    # Generate a local job log for the CLI, this is NOT
    # related to metadata sent to the registry
    # this log is viewable via the `fair view <run-cli-sha>`
    with open(_log_file, "a") as f:
        _out_str = _now.strftime("%a %b %d %H:%M:%S %Y %Z")
        f.writelines(
            [
                "--------------------------------\n",
                f" Commenced = {_out_str}\n",
                f" Author    = {' '.join(_user)} <{_email}>\n",
                f" Namespace = {_namespace}\n",
                f" Command   = {' '.join(_cmd_list)}\n",
                "--------------------------------\n",
            ]
        )

    # Run the submission script
    _process = subprocess.Popen(
        _cmd_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        bufsize=1,
        text=True,
        shell=False,
        env=_cmd_setup["env"],
    )

    # Write any stdout to the job log
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

    # Exit the session if the job failed
    if _process.returncode != 0:
        raise fdp_exc.CommandExecutionError(
            f"Run failed with exit code '{_process.returncode}'",
            exit_code=_process.returncode
        )

    _hash = get_job_hash(_job_dir)

    return _hash

def create_working_config(
    local_uri: str,
    job_dir: str,
    config_yaml: str,
    output_file: str,
    time: datetime
) -> None:
    """Generate a working configuration file used during jobs

    Parameters
    ----------
    local_uri : str
        local registry endpoint
    job_dir : str
        session job directory
    config_yaml : str
        job configuration file
    output_file : str
        location to write generated config
    time : datetime.datetime
        time stamp of job initiation time
    """
    
    _conf_yaml = fdp_parse.subst_cli_vars(
        local_uri, job_dir, time, config_yaml
    )

    # Remove 'register' from working configuration
    if "register" in _conf_yaml:
        del _conf_yaml["register"]    

    if 'read' in _conf_yaml:
        _conf_yaml['read'] = fdp_parse.glob_read_write(job_dir, _conf_yaml['read'])

    if 'write' in _conf_yaml:
        _conf_yaml['write'] = fdp_parse.glob_read_write(job_dir, _conf_yaml['write'])
    
    with open(output_file, "w") as out_f:
        yaml.dump(_conf_yaml, out_f)


def get_job_hash(job_dir: str) -> str:
    """Retrieve the hash for a given job

    NOTE: A job can consist of multiple code runs if the API implementation
    called initiates multiple executions. "Job" here refers to a call of 
    'fair run'.

    Parameters
    ----------
    job_dir : str
        jobs directory

    Returns
    -------
    str
        hash of job
    """
    if not os.path.exists(job_dir):
        raise fdp_exc.FileNotFoundError(
            "Failed to find hash for job, "
            f"directory '{job_dir}' does not exist."
        )
    _directory = os.path.abspath(job_dir)
    return hashlib.sha1(_directory.encode("utf-8")).hexdigest()


def get_job_dir(job_hash: str) -> str:
    """Get job directory from a hash

    Parameters
    ----------
    job_hash : str
        hash for a given job

    Returns
    -------
    str
        associated job directory
    """
    _jobs = glob.glob(os.path.join(fdp_com.default_jobs_dir(), '*'))

    for job in _jobs:
        _hash = hashlib.sha1(os.path.abspath(job).encode("utf-8")).hexdigest()
        if _hash == job_hash:
            return job
    
    return ""


def setup_job_script(
    config_yaml: str,
    output_dir: str
    ) -> typing.Dict[str, typing.Any]:
    """Setup a job script from the given configuration.

    Checks the user configuration file for the required 'script' or 'script_path'
    keys and determines the process to be executed. Also sets up an environment
    usable when executing the submission script.

    Parameters
    ----------
    config_yaml : str
        job configuration file
    output_dir : str
        location to store submission/job script

    Returns
    -------
    Dict[str, Any]
        a dictionary containing information on the command to execute,
        which shell to run it in and the environment to use
    """
    _conf_yaml = yaml.safe_load(open(config_yaml))
    _cmd = None
    _run_env = os.environ.copy()

    # Create environment variable which users can refer to in their
    # submission scripts
    _run_env["FDP_LOCAL_REPO"] = _conf_yaml["run_metadata"]["local_repo"]
    _run_env["FDP_CONFIG_DIR"] = os.path.dirname(config_yaml)

    # Check if a specific shell has been defined for the script
    _shell = None
    _out_file = None

    if "shell" in _conf_yaml["run_metadata"]:
        _shell = _conf_yaml["run_metadata"]["shell"]
    else:
        if platform.system() == "Windows":
            _shell = "pwsh"
        else:
            _shell = "sh"

    # TODO: Currently when "script" is specified the script is
    # written to a file with no suffix as this cannot be determined
    # by the shell choice or contents
    if "script" in _conf_yaml["run_metadata"]:
        _cmd = _conf_yaml["run_metadata"]["script"]
        _out_file = os.path.join(output_dir, "run_script")
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)
        del _conf_yaml["run_metadata"]["script"]

    elif "script_path" in _conf_yaml["run_metadata"]:
        _path = _conf_yaml["run_metadata"]["script_path"]
        if not os.path.exists(_path):
            raise fdp_exc.CommandExecutionError(
                f"Failed to execute run, script '{_path}' was not found, or"
                " failed to be created.",
                exit_code=1
            )
        _cmd = open(_path).read()
        _out_file = os.path.join(output_dir, os.path.basename(_path))
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)
    
    if not _cmd or not _out_file:
        raise fdp_exc.UserConfigError(
            "Configuration file must contain either a valid "
            "'script' or 'script_path' entry under 'run_metadata'"
        )

    _conf_yaml["run_metadata"]["script_path"] = _out_file

    with open(config_yaml, 'w') as f:
        yaml.dump(_conf_yaml, f)

    return {"shell": _shell, "script": _out_file, "env": _run_env}
