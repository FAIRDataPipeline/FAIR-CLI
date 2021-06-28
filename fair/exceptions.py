#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
FAIR-CLI Exceptions
===================

Custom exceptions for the command line interface. These are captured during
command line usage, but provide a means examining scenarios during testing.


Contents
========

Exceptions
----------

    FAIRCLIException
    RegistryErrorException
    CLIConfigurationError
    UnexpectedRegistryServerState
    FDPRepositoryError
    FileNotFoundError
    InternalError

"""

__date__ = "2021-06-28"

import click


class FAIRCLIException(Exception):
    """Base exception class for all FAIR-CLI exceptions"""

    def __init__(
        self,
        msg: str,
        hint: str = "",
        level: str = "Error",
        exit_code: int = 1,
    ):
        """Initialises a FAIR-CLI exception type.

        A level can be specified which is a prefix whenever the exception is
        captured and printed (when used within the CLI itself)

        Parameters
        ----------
        msg : str
            message to display
        hint : str
            possible solution to issue raised
        level : str, optional
            level descriptor (message prefix), by default "Error"
        """
        self.msg = msg
        self.level = level
        self.hint = hint
        self.exit_code = exit_code
        super().__init__(msg)

    def err_print(self) -> None:
        click.echo(
            f"{self.level+': ' if self.level else ''}"
            f"{self.msg}" + "\n" + self.hint
            if self.hint
            else ""
        )


class RegistryError(FAIRCLIException):
    """Errors relating to registry setup and usage"""

    def __init__(self, msg):
        super().__init__(msg)


class CLIConfigurationError(FAIRCLIException):
    """Errors relating to CLI configuration"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint=hint)


class UserConfigError(FAIRCLIException):
    """Errors relating to the user 'config.yaml' file"""

    def __init__(self, msg):
        super().__init__(msg)


class UnexpectedRegistryServerState(FAIRCLIException):
    """Errors relating to server start/stop when already up/down)"""

    def __init__(self, msg):
        super().__init__(msg, "")


class FDPRepositoryError(FAIRCLIException):
    """Errors relating to FAIR repository"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint=hint)


class FileNotFoundError(FAIRCLIException):
    """Attempt to access a non-existent file"""

    def __init__(self, msg):
        super().__init__(msg)


class InternalError(FAIRCLIException):
    """Errors relating to non-user created issues"""

    def __init__(self, msg):
        super().__init__(msg, level="InternalError")


class CommandExecutionError(FAIRCLIException):
    """Errors related to execution of submission script commands"""

    def __init__(self, msg, exit_code):
        super().__init__(msg, exit_code=exit_code)