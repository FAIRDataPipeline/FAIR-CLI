#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Logging
=======

Creation of job logs when running 'pull', 'run', 'push' etc. This is not to be
confused with use of the 'logging' module which handles debug printouts etc.

"""

__date__ = "2021-12-07"

import datetime
import os
import typing

import fair.configuration as fdp_conf


class JobLogger:
    def __init__(
        self,
        time_stamp: datetime.datetime,
        output_dir: str,
        command: str,
        project_dir: str = os.getcwd(),
    ) -> None:
        self._log: typing.List[str] = []
        self._directory = output_dir
        self._command = command
        self._project = project_dir
        self._now = time_stamp
        self._file_stream = None

    def __enter__(self) -> None:
        """Create the log entry for a command

        Parameters
        ----------
        command : str
            command executed
        """
        # Record the time the job was commenced, create a log and both
        # print output and write it to the log file
        _timestamp = self._now.strftime("%Y-%m-%d_%H_%M_%S_%f")
        _out_file = os.path.join(self._directory, f"job_{_timestamp}.log")
        self._file_stream = open(_out_file, "w")
        _out_str = self._now.strftime("%a %b %d %H:%M:%S %Y %Z")
        _user = fdp_conf.get_current_user_name(self._project)
        _email = fdp_conf.get_current_user_email(self._project)
        self._log += [
            "--------------------------------",
            f" Commenced = {_out_str}",
            f" Author    = {' '.join(_user)} <{_email}>",
            f" Command   = {self._command}",
            "--------------------------------",
        ]

        return self

    def append(self, log_line: str) -> None:
        """Append a line to the log file

        Parameters
        ----------
        log_line : str
            line to append
        """
        self._log.append(log_line)

    def write(self) -> None:
        """Write log to file"""
        _end_time = datetime.datetime.now()
        _duration = _end_time - self._now
        self.append(f"------- time taken {_duration} -------")
        self._file_stream.write("\n".join(self._log))

    def __exit__(self, type, value, tb) -> None:
        self.write()
        self._file_stream.close()
