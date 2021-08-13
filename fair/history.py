#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Run History
===========

Methods relating to the summary of jobs commenced within the given FAIR-CLI
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
    """Retrieve the directory containing job logs for the specified repository

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository path

    Returns
    -------
    str
        location of the job logs directory
    """
    return os.path.join(
        fdp_com.find_fair_root(repo_loc), fdp_com.FAIR_FOLDER, "logs"
    )


def show_job_log(repo_loc: str, job_id: str) -> str:
    """Show the log from a given job

    Parameters
    ----------
    repo_loc : str
        FAIR-CLI repository path
    job_id : str
        SHA identifier for the job

    Returns
    -------
    str
        log file location for the given job
    """
    _time_sorted_logs = sorted(
        glob.glob(os.path.join(history_directory(repo_loc), "*")),
        key=os.path.getmtime,
        reverse=True,
    )

    for log_file in _time_sorted_logs:
        # Use the timestamp directory name for the hash
        _job_id = fdp_run.get_job_hash(os.path.dirname(log_file))

        if _job_id[: len(job_id)] == job_id:
            with open(log_file) as f:
                click.echo(f.read())
            _jobs_list = os.path.join(
                fdp_com.JOBS_DIR,
                os.path.splitext(log_file)[0].replace("job_", ""),
                'coderuns.txt'
            )

            # Check if a code runs file exists for the given job and also
            # print the list of code_run uuids created in the registry
            if os.path.exists(_jobs_list):
                click.echo("Related Code Runs: ")
                click.echo('\n\t- '.join(open(_jobs_list).readlines()))

            return log_file

    raise fdp_exc.FileNotFoundError(
        f"Could not find job matching id '{job_id}'"
    )


def show_history(repo_loc: str, length: int = 10) -> None:
    """Show job history by time sorting log outputs, display metadata

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

    # Iterate through the logs printing out the job author
    for i, log in enumerate(_time_sorted_logs):
        _job_id = fdp_run.get_job_hash(os.path.dirname(log))
        with open(log) as f:
            _metadata = f.readlines()[:5]
        if not _metadata:
            continue
        _metadata = [i for i in _metadata if i.strip()]
        _user = _metadata[2].split("=")[1]
        _name = _user.split("<")[0].strip()
        _email = _user.replace(_name, "").strip()
        _meta = {
            "sha": _job_id,
            "user": _name,
            "user_email": _email,
            "datetime": _metadata[1].split("=")[1].strip(),
        }
        rich.print(hist_template.render(**_meta))

        if i == length:
            return
