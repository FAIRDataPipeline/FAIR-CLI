#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Run History
===========

Methods relating to the summary of runs called within the given FAIR-CLI
repository. Allowing user to view any stdout from these.

Contents
========

Functions
---------

    history_directory - returns the current repository logs directory
"""
import os
import glob

import click
import rich

import fair.common as fdp_com
import fair.run as fdp_run
import fair.exceptions as fdp_exc
from fair.templates import hist_template


def history_directory(repo_loc: str) -> str:
    """Retrieve the directory containing run logs for the specified repository

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository path

    Returns
    -------
    str
        location of the run logs directory
    """
    return os.path.join(
        fdp_com.find_fair_root(repo_loc), fdp_com.FAIR_FOLDER, "logs"
    )


def show_run_log(repo_loc: str, run_id: str) -> str:
    """Show the log from a given run

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository path
    run_id : str
        SHA identifier for the code run

    Returns
    -------
    str
        log file location for the given run
    """
    _time_sorted_logs = sorted(
        glob.glob(os.path.join(history_directory(repo_loc), "*")),
        key=os.path.getmtime,
        reverse=True,
    )

    for log_file in _time_sorted_logs:
        # Use the timestamp directory name for the hash
        _run_id = fdp_run.get_cli_run_hash(os.path.dirname(log_file))

        if _run_id[: len(run_id)] == run_id:
            with open(log_file) as f:
                click.echo(f.read())
            _code_runs_list = os.path.join(
                fdp_com.CODERUN_DIR,
                os.path.splitext(log_file)[0].replace("run_", ""),
                'coderuns.txt'
            )

            # Check if a code runs file exists for the given run and also
            # print the list of code_run uuids created in the registry
            if os.path.exists(_code_runs_list):
                click.echo("Related Code Runs: ")
                click.echo('\n\t- '.join(open(_code_runs_list).readlines()))

            return log_file

    raise fdp_exc.FileNotFoundError(
        f"Could not find run matching id '{run_id}'"
    )


def show_history(repo_loc: str, length: int = 10) -> None:
    """Show run history by time sorting log outputs, display metadata

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository path
    length : int, optional
        max number of entries to display, by default 10
    """

    # Read in all log files from the log storage by reverse sorting them
    # by datetime created
    _time_sorted_logs = sorted(
        glob.glob(os.path.join(history_directory(repo_loc), "*")),
        key=os.path.getmtime,
    )

    # Iterate through the logs printing out the run author
    for i, log in enumerate(_time_sorted_logs):
        _run_id = fdp_run.get_cli_run_hash(os.path.dirname(log))
        with open(log) as f:
            _metadata = f.readlines()[:5]
        if not _metadata:
            continue
        _metadata = [i for i in _metadata if i.strip()]
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

        if i == length:
            return
