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
    KeyPathError
    UnexpectedRegistryServerState
    FDPRepositoryError
    FileNotFoundError
    InternalError
    StagingError

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
        _out_msg = f"{self.level+': ' if self.level else ''}{self.msg}"
        if self.hint:
            _out_msg += f"\n{self.hint}"
        click.echo(_out_msg)


class RegistryError(FAIRCLIException):
    """Errors relating to registry setup and usage"""

    def __init__(self, msg: str, hint:str = ""):
        super().__init__(msg, hint=hint)


class CLIConfigurationError(FAIRCLIException):
    """Errors relating to CLI configuration"""

    def __init__(self, msg, hint="", level='Error'):
        super().__init__(msg, hint=hint, level=level)


class KeyPathError(FAIRCLIException):
    """Errors relating to key path within a nested mapping"""

    def __init__(self, key, parent_label):
        _msg = f"Failed to retrieve item at address '{key}' from mapping '{parent_label}', no such address"
        super().__init__(_msg, level='Error')


class UserConfigError(FAIRCLIException):
    """Errors relating to the user 'config.yaml' file"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint=hint)


class UnexpectedRegistryServerState(FAIRCLIException):
    """Errors relating to server start/stop when already up/down)"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint)


class FDPRepositoryError(FAIRCLIException):
    """Errors relating to FAIR repository"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint=hint)


class FileNotFoundError(FAIRCLIException):
    """Attempt to access a non-existent file"""

    def __init__(self, msg, hint=""):
        super().__init__(msg, hint=hint)


class InternalError(FAIRCLIException):
    """Errors relating to non-user created issues"""

    def __init__(self, msg):
        super().__init__(msg, level="InternalError")


class CommandExecutionError(FAIRCLIException):
    """Errors related to execution of submission script commands"""

    def __init__(self, msg, exit_code):
        super().__init__(msg, exit_code=exit_code)


class RegistryAPICallError(FAIRCLIException):
    """Errors relating to invalid queries to the registry RestAPI"""

    def __init__(self, msg, error_code):
        self.error_code = error_code
        _level = "Warning" if self.error_code in [403] else "Error"
        super().__init__(f'[HTTP {self.error_code}]: {msg}', exit_code=error_code, level=_level)

class NotImplementedError(FAIRCLIException):
    """Errors relating to features that have not yet been implemented"""
    def __init__(self, msg, hint="", level='Error'):
        super().__init__(msg, hint, level=level)


class StagingError(FAIRCLIException):
    """Errors relating to the staging of jobs"""
    def __init__(self, msg):
        super().__init__(msg)


class SynchronisationError(FAIRCLIException):
    """Errors relating to synchronisation between registries"""
    def __init__(self, msg, error_code):
        self.error_code = error_code
        super().__init__(msg, exit_code=error_code)


class ImplementationError(FAIRCLIException):
    """Errors relating to setup via API implementation"""
    def __init__(self, msg):
        super().__init__(msg)
