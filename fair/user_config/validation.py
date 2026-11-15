#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
User Config Validation
======================

validation of the user configuration 'config.yaml' to a defined schema. Uses
pydantic to construct base models which form the components of the configuration.

Contents
========

Classes
-------

    Validation models for configuration components:

    - RunMetadata
    - Use
    - Write
    - Read
    - Author
    - Namespace
    - DataProduct
    - UserConfigModel

Functions
---------

    check_namespace_name - refuse a namespace's name outside the convention
    check_data_product_name - refuse a data product's name outside the convention

"""

__date__ = "2021-11-16"

import datetime
import enum
import pathlib
import re
import typing
import uuid

import pydantic

import fair.exceptions as fdp_exc

# The naming convention: a namespace holds ASCII letters, digits, '_' and '-';
# a data product's name is parts joined by single slashes, each of those and
# '.', which may not start a part (a hidden folder) or end one (stripped on
# Windows)
_NAME_PART = r"[A-Za-z0-9_-](?:[A-Za-z0-9_.-]*[A-Za-z0-9_-])?"
NAMESPACE_NAME = re.compile(r"[A-Za-z0-9_-]+")
DATA_PRODUCT_NAME = re.compile(rf"{_NAME_PART}(?:/{_NAME_PART})*")

# What a data product's name in a configuration may hold besides: a wildcard
# within a part, and the code run's id, which the API puts in place of
# '${{RUN_ID}}'
_STANDS_FOR_NAME = ("${{RUN_ID}}", "*")


class SupportedShells(enum.Enum):
    POWERSHELL = "powershell"
    PWSH = "pwsh"
    BASH = "bash"
    BATCH = "batch"
    PYTHON = "python"
    PYTHON2 = "python2"
    PYTHON3 = "python3"
    R = "R"
    JULIA = "julia"
    JAVA = "java"
    SH = "sh"


class RunMetadata(pydantic.BaseModel):
    local_repo: pathlib.Path = pydantic.Field(
        ...,
        title="local repository",
        description="Absolute path to the location of the local project repository",
    )
    latest_commit: str = pydantic.Field(
        ..., title="latest commit", description="latest git commit"
    )
    remote_repo: str = pydantic.Field(
        ...,
        title="remote repository",
        description="remote repository location",
    )
    description: typing.Optional[str] = pydantic.Field(
        None,
        title="project description",
        description="description of the current project",
    )
    local_data_registry_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="local registry URL",
        description="URL of the local data registry",
    )
    remote_data_registry_url: pydantic.AnyHttpUrl = pydantic.Field(
        ...,
        title="remote registry URL",
        description="URL of the remote data registry",
    )
    default_input_namespace: str = pydantic.Field(
        ...,
        title="default input namespace",
        description="default namespace to use for data imports",
    )
    default_output_namespace: str = pydantic.Field(
        ...,
        title="default output namespace",
        description="default namespace to use for writing of data entries",
    )
    default_read_version: typing.Optional[str] = pydantic.Field(
        None,
        title="default read version",
        description="default version number to use for item reads",
    )
    default_write_version: typing.Optional[str] = pydantic.Field(
        None,
        title="default write version",
        description="default version number to use for item writes",
    )
    write_data_store: pathlib.Path = pydantic.Field(
        ...,
        title="write data store location",
        description="relative path of the file system root",
    )
    script: typing.Optional[str] = pydantic.Field(
        None, title="script", description="command to execute during a run"
    )
    script_path: typing.Optional[pathlib.Path] = pydantic.Field(
        None,
        title="script path",
        description="path of script to execute during a run",
    )
    shell: typing.Optional[str] = pydantic.Field(
        None,
        title="shell",
        description="shell to use for script execution, then any flags for "
        "its program",
    )
    public: typing.Optional[bool] = pydantic.Field(
        True,
        title="public",
        description="whether items are/should be publically accessible",
    )

    # A shell is a supported shell's name, then any flags for its program
    @pydantic.validator("shell")
    def shell_is_supported(
        cls, shell: typing.Optional[str]
    ) -> typing.Optional[str]:
        if shell is None:
            return shell
        _supported = [s.value for s in SupportedShells]
        _words = shell.split()
        if not _words or _words[0] not in _supported:
            raise ValueError(
                f"must start with one of: {', '.join(_supported)}"
            )
        return shell

    class Config:
        extra = "forbid"


class ExternalObject(pydantic.BaseModel):
    external_object: str = pydantic.Field(
        ...,
        title="external object label",
        description="label for the external object",
    )
    identifier: typing.Optional[str] = pydantic.Field(
        None, title="identifier", description="identifier for this object"
    )
    alternate_identifier: typing.Optional[str] = pydantic.Field(
        None,
        title="alternative identifier",
        description="alternative type of identifier",
    )
    alternate_identifier_type: typing.Optional[str] = pydantic.Field(
        None,
        title="alternative identifier type",
        description="type of the alternative identifier",
    )
    namespace_name: str = pydantic.Field(
        ...,
        title="name of namespace",
        alias="namespace",
        description="short label for the namespace",
    )
    namespace_full_name: typing.Optional[str] = pydantic.Field(
        None,
        title="full namespace name",
        description="longer full name of the namespace",
    )
    root: pydantic.AnyHttpUrl = pydantic.Field(
        ..., title="root of the URL", description="root URL for the object"
    )
    path: pathlib.Path = pydantic.Field(
        ...,
        title="path of object",
        description="the relative path from the root to the object",
    )
    title: str = pydantic.Field(
        ..., title="title", description="full title of the object"
    )
    description: typing.Optional[str] = pydantic.Field(
        None,
        title="description of object",
        description="a full description of what the object represents",
    )
    file_type: str = pydantic.Field(
        ...,
        title="file type",
        description="extension of type of file to store object as",
    )
    release_date: datetime.datetime = pydantic.Field(
        ...,
        title="data release date",
        description="date and time of data release",
    )
    release_version: typing.Optional[str] = pydantic.Field(
        None,
        title="data release version",
        description="version of the release the object was taken from",
    )
    version: str = pydantic.Field(
        ..., title="version", description="object version to import as"
    )
    primary: bool = pydantic.Field(
        ...,
        title="primary not supplement",
        description="whether a primary source or supplement object",
    )

    class Config:
        extra = "forbid"


class Use(pydantic.BaseModel):
    data_product: typing.Optional[str] = pydantic.Field(
        None,
        title="data product label",
        description="label for the data product",
    )
    version: typing.Optional[str] = pydantic.Field(
        None, title="version", description="object version to use"
    )
    namespace: typing.Optional[str] = pydantic.Field(
        None,
        title="namespace",
        description="namespace to read/write object using",
    )
    cache: typing.Optional[str] = pydantic.Field(
        None, title="cache", description="local copy of requested file to use"
    )

    class Config:
        extra = "forbid"


class DataProduct(pydantic.BaseModel):
    data_product: str = pydantic.Field(
        ...,
        title="data product label",
        description="label for the data product",
    )
    description: typing.Optional[str] = pydantic.Field(
        None,
        title="description of data product",
        description="a full description of what the data product represents",
    )

    use: typing.Optional[Use]

    class Config:
        extra = "forbid"


class DataProductWrite(DataProduct):
    file_type: str = pydantic.Field(
        ...,
        title="file type",
        description="extension of type of file the data product is",
    )
    public: typing.Optional[bool] = pydantic.Field(
        True,
        title="public",
        description="whether items are/should be publically accessible",
    )

    class Config:
        extra = "forbid"


class Namespace(pydantic.BaseModel):
    namespace: str = pydantic.Field(
        ..., title="namespace label", description="label for the namespace"
    )
    full_name: str = pydantic.Field(
        None,
        title="namespace full name",
        description="longer name for the namespace",
    )
    website: typing.Optional[pydantic.AnyHttpUrl] = pydantic.Field(
        None,
        title="namespace URL",
        description="website URL associated with the namespace",
    )

    class Config:
        extra = "forbid"


class Author(pydantic.BaseModel):
    author: str = pydantic.Field(
        ...,
        title="unique local author identifier",
        description="unique local identifier used to connect"
        "author registration with any objects",
    )
    name: str = pydantic.Field(
        ..., title="author name", description="the name of the author"
    )
    identifier: str = pydantic.Field(
        str(uuid.uuid4()),
        title="identifier for the author",
        description="unique ID within the local registry for the author",
    )

    class Config:
        extra = "forbid"


# Permitted objects which are recognised by the schema and registry
VALID_OBJECTS = {
    "author": Author,
    "data_product": DataProduct,
    "namespace": Namespace,
    "external_object": ExternalObject,
}


class UserConfigModel(pydantic.BaseModel):
    run_metadata: RunMetadata
    read: typing.Optional[typing.List[DataProduct]]
    registration: typing.Optional[
        typing.List[typing.Union[ExternalObject, Author, Namespace]]
    ] = pydantic.Field(None, alias="register")
    write: typing.Optional[typing.List[DataProductWrite]]

    class Config:
        extra = "forbid"


if __name__ in "__main__":
    import argparse

    import yaml

    parser = argparse.ArgumentParser()
    parser.add_argument("in_file")

    _data = yaml.safe_load(open(parser.parse_args().in_file, encoding="utf-8"))
    UserConfigModel(**_data)


def check_namespace_name(name: typing.Any, entry: str) -> None:
    """Refuse a namespace's name that does not keep to the naming convention

    Parameters
    ----------
    name : typing.Any
        the name, as a configuration gives it
    entry : str
        the configuration's entry that gives it, as the refusal names it
    """
    if not isinstance(name, str) or not NAMESPACE_NAME.fullmatch(name):
        raise fdp_exc.UserConfigError(
            f"The namespace '{name}' of {entry} is not a valid name: a "
            "namespace holds ASCII letters, digits, '_' and '-' alone",
            hint="Name it with those alone, such as 'my_namespace'",
        )


def check_data_product_name(name: typing.Any, entry: str) -> None:
    """Refuse a data product's name that does not keep to the naming convention

    A '*' within a part, as a wildcard has, and '${{RUN_ID}}' are allowed.

    Parameters
    ----------
    name : typing.Any
        the name, as a configuration gives it
    entry : str
        the configuration's entry that gives it, as the refusal names it
    """
    _name = name
    if isinstance(_name, str):
        for _token in _STANDS_FOR_NAME:
            _name = _name.replace(_token, "0")
    if not isinstance(_name, str) or not DATA_PRODUCT_NAME.fullmatch(_name):
        raise fdp_exc.UserConfigError(
            f"The data product name '{name}' of {entry} is not a valid name: it "
            "is one or more parts joined by single '/', each of ASCII letters, "
            "digits, '_', '-' and '.', with no '.' at a part's start or end",
            hint="Name it so, such as 'model/results/output_01'",
        )
