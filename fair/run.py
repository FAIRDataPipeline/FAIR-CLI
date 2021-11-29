#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Execute Job
===========

Creates required files for job execution and runs the job itself using the
retrieved metadata

"""

__date__ = "2021-06-30"

import datetime
import glob
import hashlib
import logging
import os
import platform
import subprocess
import sys
import typing

import click

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.history as fdp_hist
import fair.user_config as fdp_user
from fair.common import CMD_MODE

logger = logging.getLogger("FAIRDataPipeline.Run")

# Dictionary of recognised shell labels.
SHELLS: typing.Dict[str, str] = {
    "pwsh": {"exec": "pwsh -command \". '{0}'\"", "extension": "ps1"},
    "batch": {"exec": "{0}", "extension": "bat"},
    "powershell": {"exec": "powershell -command \". '{0}'\"", "extension": "ps1"},
    "python2": {"exec": "python2 {0}", "extension": "py"},
    "python3": {"exec": "python3 {0}", "extension": "py"},
    "python": {"exec": "python {0}", "extension": "py"},
    "R": {"exec": "R -f {0}", "extension": "R"},
    "julia": {"exec": "julia {0}", "extension": "jl"},
    "bash": {
        "exec": "bash -eo pipefail {0}",
        "extension": "sh",
    },
    "java": {"exec": "java {0}", "extension": "java"},
    "sh": {"exec": "sh -e {0}", "extension": "sh"},
}


def run_command(
    repo_dir: str,
    config_yaml: str = None,
    mode: CMD_MODE = CMD_MODE.RUN,
    bash_cmd: str = "",
    allow_dirty: bool = False
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
    allow_dirty : bool, optional
            allow runs with uncommitted changes, default is False
    """

    if not config_yaml:
        config_yaml = os.path.join(fdp_com.find_fair_root(), fdp_com.USER_CONFIG_FILE)

    logger.debug("Using user configuration file: %s", config_yaml)
    click.echo(f"Updating registry from {config_yaml}", err=True)

    # Record the time the job was commenced, create a log and both
    # print output and write it to the log file
    _now = datetime.datetime.now()
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

    logger.debug(f"Creating user configuration job object from {config_yaml}")

    _job_cfg = fdp_user.JobConfiguration(config_yaml)

    _job_cfg.update_from_fair(repo_dir)

    if bash_cmd:
        _job_cfg.set_command(bash_cmd)

    _job_dir = os.path.join(fdp_com.default_jobs_dir(), _timestamp)
    logger.debug("Using job directory: %s", _job_dir)
    os.makedirs(_job_dir, exist_ok=True)

    _job_cfg.prepare(_job_dir, _timestamp, mode, allow_dirty=allow_dirty)

    _run_executable = (
        "script" in _job_cfg["run_metadata"]
        or "script_path" in _job_cfg["run_metadata"]
    )
    _run_executable = _run_executable and mode in [CMD_MODE.RUN, CMD_MODE.PASS]

    if mode == CMD_MODE.PASS:
        logger.debug("Run called in passive mode, no command will be executed")

    # Set location of working config.yaml to the job directory
    _work_cfg_yml = os.path.join(_job_dir, fdp_com.USER_CONFIG_FILE)

    # Fetch the CLI configurations for logging information
    _user = fdp_conf.get_current_user_name(repo_dir)
    _email = fdp_conf.get_current_user_email(repo_dir)

    if mode in [CMD_MODE.PULL]:
        # If not a fair run then the log file will have less metadata
        # all commands should produce a log so that the 'fair log' history
        # can be displayed
        with open(_log_file, "a") as f:
            _out_str = _now.strftime("%a %b %d %H:%M:%S %Y %Z")
            f.writelines(
                [
                    "--------------------------------\n",
                    f" Commenced = {_out_str}\n",
                    f" Author    = {' '.join(_user)} <{_email}>\n",
                    " Command   = fair pull\n",
                    "--------------------------------\n",
                ]
            )

    _job_cfg.write(_work_cfg_yml)

    logger.debug("Creating working configuration storage location")

    if _run_executable:

        # Create a run script if 'script' is specified instead of 'script_path'
        # else use the script
        _cmd_setup = setup_job_script(
            _job_cfg.content, _job_cfg.env["FDP_CONFIG_DIR"], _job_dir
        )

        _job_cfg.set_script(_cmd_setup["script"])
        _job_cfg.write(_work_cfg_yml)

        if _job_cfg.shell not in SHELLS:
            raise fdp_exc.UserConfigError(
                f"Unrecognised shell '{_job_cfg.shell}' specified."
            )

        _exec = SHELLS[_job_cfg.shell]["exec"]
        _cmd_list = _exec.format(_cmd_setup["script"]).split()

        if not _job_cfg.command:
            click.echo("Nothing to run.")
            sys.exit(0)

        # Generate a local job log for the CLI, this is NOT
        # related to metadata sent to the registry
        # this log is viewable via the `fair view <run-cli-sha>`
        with open(_log_file, "a") as f:
            _out_str = _now.strftime("%a %b %d %H:%M:%S %Y %Z")
            _user = _user[0] if not _user[1] else " ".join(_user)
            f.writelines(
                [
                    "--------------------------------\n",
                    f" Commenced = {_out_str}\n",
                    f" Author    = {_user} <{_email}>\n",
                    f" Namespace = {_job_cfg.default_output_namespace}\n",
                    f" Command   = {' '.join(_cmd_list)}\n",
                    "--------------------------------\n",
                ]
            )

        if mode == CMD_MODE.RUN:
            execute_run(_cmd_list, _job_cfg, _log_file, _now)
        else:  # CMD_MODE.PASS
            _end_time = datetime.datetime.now()
            with open(_log_file, "a") as f:
                _duration = _end_time - _now
                f.writelines(
                    [
                        "Operating in ci mode without running script\n",
                        f"------- time taken {_duration} -------\n",
                    ]
                )
    else:
        _end_time = datetime.datetime.now()
        with open(_log_file, "a") as f:
            _duration = _end_time - _now
            f.writelines([f"------- time taken {_duration} -------\n"])

    return get_job_hash(_job_dir)


def execute_run(
    command: typing.List[str],
    job_config: fdp_user.JobConfiguration,
    log_file: str,
    timestamp: datetime.datetime,
) -> None:
    """Execute a run initialised by a CLI run of mode RUN

    Parameters
    ----------
    command : typing.List[str]
        command to execute
    job_config : str
        job configuration
    log_file : str
        log file to write stdout
    timestamp : datetime.datetime
        job execution start time

    Raises
    ------
    fdp_exc.CommandExecutionError
        [description]
    """
    logger.debug("Executing command: %s", " ".join(command))

    # Run the submission script
    _process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1,
        text=True,
        shell=False,
        env=job_config.environment,
        cwd=job_config.local_repository,
    )

    # Write any stdout to the job log
    for line in iter(_process.stdout.readline, ""):
        with open(log_file, "a") as f:
            f.writelines([line])
        click.echo(line, nl=False)
        sys.stdout.flush()
    _process.wait()
    _end_time = datetime.datetime.now()
    with open(log_file, "a") as f:
        _duration = _end_time - timestamp
        f.writelines([f"------- time taken {_duration} -------\n"])

    # Exit the session if the job failed
    if _process.returncode != 0:
        raise fdp_exc.CommandExecutionError(
            f"Run failed with exit code '{_process.returncode}'",
            exit_code=_process.returncode,
        )


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
            "Failed to find hash for job, " f"directory '{job_dir}' does not exist."
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
    _jobs = glob.glob(os.path.join(fdp_com.default_jobs_dir(), "*"))

    for job in _jobs:
        _hash = hashlib.sha1(os.path.abspath(job).encode("utf-8")).hexdigest()
        if _hash == job_hash:
            return job

    return ""


def setup_job_script(
    user_config: typing.Dict, config_dir: str, output_dir: str
) -> typing.Dict[str, typing.Any]:
    """Setup a job script from the given configuration.

    Checks the user configuration file for the required 'script' or 'script_path'
    keys and determines the process to be executed. Also sets up an environment
    usable when executing the submission script.

    Parameters
    ----------
    local_repo : str
        local FAIR repository
    script : str
        script to write to file
    config_dir : str
        final location of output config.yaml
    output_dir : str
        location to store submission/job script

    Returns
    -------
    Dict[str, Any]
        a dictionary containing information on the command to execute,
        which shell to run it in and the environment to use
    """
    logger.debug("Setting up job script for execution")
    _cmd = None

    if config_dir[-1] != os.path.sep:
        config_dir += os.path.sep

    # Check if a specific shell has been defined for the script
    _shell = None
    _out_file = None

    if "shell" in user_config["run_metadata"]:
        _shell = user_config["run_metadata"]["shell"]
    else:
        _shell = "batch" if platform.system() == "Windows" else "sh"

    logger.debug("Will use shell: %s", _shell)

    if "script" in user_config["run_metadata"]:
        _cmd = user_config["run_metadata"]["script"]

        if "extension" not in SHELLS[_shell]:
            raise fdp_exc.InternalError(
                f"Failed to retrieve an extension for shell '{_shell}'"
            )
        _ext = SHELLS[_shell]["extension"]
        _out_file = os.path.join(output_dir, f"script.{_ext}")
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)

    elif "script_path" in user_config["run_metadata"]:
        _path = user_config["run_metadata"]["script_path"]
        if not os.path.exists(_path):
            raise fdp_exc.CommandExecutionError(
                f"Failed to execute run, script '{_path}' was not found, or"
                " failed to be created.",
                exit_code=1,
            )
        _cmd = open(_path).read()
        _out_file = os.path.join(output_dir, os.path.basename(_path))
        if _cmd:
            with open(_out_file, "w") as f:
                f.write(_cmd)

    logger.debug("Script command: %s", _cmd)
    logger.debug("Script written to: %s", _out_file)

    if not _cmd or not _out_file:
        raise fdp_exc.UserConfigError(
            "Configuration file must contain either a valid "
            "'script' or 'script_path' entry under 'run_metadata'"
        )

    return {"shell": _shell, "script": _out_file}
