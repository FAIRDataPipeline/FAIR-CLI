#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""

Execute Job
===========

Creates required files for job execution and runs the job itself using the
retrieved metadata

"""

__date__ = "2021-06-30"

import glob
import hashlib
import logging
import os
import typing

import fair.common as fdp_com
import fair.exceptions as fdp_exc

logger = logging.getLogger("FAIRDataPipeline.Run")

# Dictionary of recognised shell labels.
SHELLS: typing.Dict[str, str] = {
    "pwsh": {"exec": "pwsh -command \". '{0}'\"", "extension": "ps1"},
    "batch": {"exec": "{0}", "extension": "bat"},
    "powershell": {
        "exec": "powershell -command \". '{0}'\"",
        "extension": "ps1",
    },
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
