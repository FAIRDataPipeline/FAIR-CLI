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
import collections
import glob
import os
import typing

import click
import rich
import yaml

from pathlib import Path

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.run as fdp_run
import fair.templates as fdp_tpl


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
    return os.path.join(fdp_com.find_fair_root(repo_loc), fdp_com.FAIR_FOLDER, "logs")


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
    _job_dir = Path(f"{fdp_com.default_jobs_dir()}")

    _sorted_time_dirs = sorted(glob.glob(os.path.join(_job_dir, "*")), reverse=True)

    _log_files = [
        os.path.join(history_directory(repo_loc), f"job_{os.path.basename(i)}.log")
        for i in _sorted_time_dirs
    ]

    for job_dir, _log_file in zip(_sorted_time_dirs, _log_files):
        # Use the timestamp directory name for the hash
        _job_id = fdp_run.get_job_hash(job_dir)

        # A job with no log here was run from another repository using the
        # same data store
        if _job_id[: len(job_id)] == job_id and os.path.exists(_log_file):
            with open(_log_file, encoding="utf-8") as f:
                click.echo(f.read())
            _jobs_list = os.path.join(job_dir, "coderuns.txt")

            # Check if a code runs file exists for the given job and also
            # print the list of code_run uuids created in the registry
            if os.path.exists(_jobs_list):
                click.echo("Related Code Runs: ")
                click.echo(
                    "\t- "
                    + "\n\t- ".join(open(_jobs_list, encoding="utf-8").readlines())
                )

            return _log_file

    raise fdp_exc.FileNotFoundError(
        f"Could not find job matching id '{job_id}'",
        hint="Run 'fair log' to list the jobs run from this repository.",
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

    _job_dir = Path(f"{fdp_com.default_jobs_dir()}")

    _sorted_time_dirs = sorted(glob.glob(os.path.join(_job_dir, "*")), reverse=True)

    _log_files = [
        os.path.join(history_directory(repo_loc), f"job_{os.path.basename(i)}.log")
        for i in _sorted_time_dirs
    ]

    # The jobs directory is shared by every repository using the data store,
    # and each keeps the logs of its own jobs
    _listed = []
    _unlisted = []
    for job_dir, _log_file in zip(_sorted_time_dirs, _log_files):
        if os.path.exists(_log_file):
            _listed.append((job_dir, _log_file))
        else:
            _unlisted.append(job_dir)

    if not _listed:
        click.echo("No jobs have been run from this repository")

    # Iterate through the logs printing out the job author
    _shown = 0
    for job_dir, _log_file in _listed:
        if _shown == length:
            break
        _job_id = fdp_run.get_job_hash(job_dir)
        with open(_log_file, encoding="utf-8") as f:
            _log_lines = f.readlines()
            _metadata = []
            for line in _log_lines:
                if "------- time taken " in line:
                    break
                if " = " in line:
                    _metadata.append(line)
        if not _metadata:
            continue
        _metadata = [i for i in _metadata if i.strip()]
        _user_lines = [i for i in _metadata if "Author" in i]
        if not _user_lines:
            raise fdp_exc.InternalError(
                "Failed to retrieve author information from log " f"for job '{_job_id}'"
            )
        _date_lines = [i for i in _metadata if "Commenced" in i]
        if not _date_lines:
            raise fdp_exc.InternalError(
                "Failed to retrieve date information from log " f"for job '{_job_id}'"
            )
        _date = _date_lines[0].split("=")[1].strip()
        _user = _user_lines[0].split("=")[1].strip()
        _name = _user.split("<")[0].strip()
        _email = _user.replace(_name, "").strip()
        _meta = {
            "sha": _job_id,
            "user": _name,
            "user_email": _email,
            "datetime": _date,
        }
        # Unhighlighted, as rich colours the numbers of a date in shades that
        # some terminal backgrounds hide
        rich.get_console().print(fdp_tpl.hist_template.render(**_meta), highlight=False)
        _shown += 1

    _report_unlisted(repo_loc, _job_dir, _unlisted)


def _job_repository(job_dir: str) -> typing.Optional[str]:
    """Return the repository a job's working config names, or None if it names none"""
    try:
        with open(
            os.path.join(job_dir, fdp_com.USER_CONFIG_FILE), encoding="utf-8"
        ) as f:
            return yaml.safe_load(f)["run_metadata"]["local_repo"]
    except (OSError, yaml.YAMLError, KeyError, TypeError):
        return None


def _jobs(count: int) -> str:
    """Return a count of jobs in words"""
    return f"{count} job{'' if count == 1 else 's'}"


def _report_unlisted(repo_loc: str, jobs_dir: str, unlisted: typing.List[str]) -> None:
    """Say how many jobs 'fair log' did not list, and which repository ran them"""
    if not unlisted:
        return
    _here = os.path.realpath(fdp_com.find_fair_root(repo_loc))
    _repos = collections.Counter(_job_repository(j) for j in unlisted)
    click.echo(
        f"Not listed: {_jobs(len(unlisted))} in '{jobs_dir}' with no log in "
        "this repository"
    )
    for _repo, _count in sorted(_repos.items(), key=lambda r: (-r[1], str(r[0]))):
        if _repo is None:
            click.echo(f"    {_jobs(_count)} whose working config names no repository")
        elif os.path.normcase(os.path.realpath(_repo)) == os.path.normcase(_here):
            click.echo(f"    {_jobs(_count)} run from this repository, its log gone")
        else:
            click.echo(
                f"    {_jobs(_count)} run from '{_repo}', where 'fair log' lists them"
            )
