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

"""

__date__ = "2021-11-16"

import datetime
import enum
import pathlib
import typing
import uuid

import pydantic


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
    shell: typing.Optional[SupportedShells] = pydantic.Field(
        None, title="shell", description="shell to use for script execution"
    )
    public: typing.Optional[bool] = pydantic.Field(
        True,
        title="public",
        description="whether items are/should be publically accessible",
    )

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

    _data = yaml.safe_load(open(parser.parse_args().in_file))
    UserConfigModel(**_data)
