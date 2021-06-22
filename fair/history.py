import os
import glob
import sys

import hashlib
import click
import rich

import fair.common as fdp_com
from fair.templates import hist_template


def history_directory() -> str:
    return os.path.join(fdp_com.find_fair_root(), fdp_com.FAIR_FOLDER, "logs")


def show_run_log(run_id: str) -> None:
    """Show the log from a given run"""
    _time_sorted_logs = sorted(
        glob.glob(os.path.join(history_directory(), "*")),
        key=os.path.getmtime,
        reverse=True,
    )

    for log_file in _time_sorted_logs:
        _run_id = hashlib.sha1(
            open(log_file).read().encode("utf-8")
        ).hexdigest()

        if _run_id[: len(run_id)] == run_id:
            with open(log_file) as f:
                click.echo(f.read())
            return
    click.echo(f"Could not find run matching id '{run_id}'")
    sys.exit(1)


def show_history(self, length: int = 10) -> None:
    """Show run history by time sorting log outputs, display metadata"""

    # Read in all log files from the log storage by reverse sorting them
    # by datetime created
    _time_sorted_logs = sorted(
        glob.glob(os.path.join(history_directory(), "*")),
        key=os.path.getmtime,
        reverse=True,
    )

    # Iterate through the logs printing out the run author
    for i, log in enumerate(_time_sorted_logs):
        if i == length:
            return
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
