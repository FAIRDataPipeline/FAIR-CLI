#!/usr/bin/python3
# -*- coding: utf-8 -*-
# sourcery skip: avoid-builtin-shadow
"""
CLI Config Validation
=====================

validation of the CLI configuration files both global and local variants

Contents
========

Classes
-------

"""

__date__ = "2021-12-07"

import pathlib
import typing

import pydantic


class Git(pydantic.BaseModel):
    local_repo: pathlib.Path = pydantic.Field(
        ...,
        title="local repository",
        description="Local project git repository",
    )
    remote: str = pydantic.Field(
        ...,
        title="remote label",
        description="label of git repository remote to use",
    )
    remote_repo: str = pydantic.Field(
        ...,
        title="URL of remote repo",
        description="URL for the given remote repository",
    )

    class Config:
        extra = "forbid"


class Namespaces(pydantic.BaseModel):
    input: str = pydantic.Field(
        ...,
        title="input namespace",
        description="namespace label to use for registry inputs",
    )
    output: str = pydantic.Field(
        ...,
        title="output namespace",
        description="namespace label to use for registry outputs",
    )

    class Config:
        extra = "forbid"


class Registry(pydantic.BaseModel):
    data_store: pathlib.Path = pydantic.Field(
        ...,
        title="data store path",
        description="location of data storage directory for registry",
    )
    token: pathlib.Path = pydantic.Field(
        ...,
        title="token file path",
        description="path of file containing token for the registry",
    )
    uri: pydantic.AnyHttpUrl = pydantic.Field(
        ..., title="registry URL", description="URL of the data registry"
    )
    directory: typing.Optional[pathlib.Path] = pydantic.Field(
        None,
        title="local directory",
        description="local location of registry on system",
    )

    class Config:
        extra = "forbid"


class User(pydantic.BaseModel):
    email: pydantic.EmailStr = pydantic.Field(
        ..., title="user email", description="email address of the user"
    )
    family_name: str = pydantic.Field(
        ..., title="user surname", description="family name of the user"
    )
    given_names: str = pydantic.Field(
        ..., title="user forenames", description="given names of the user"
    )
    orcid: typing.Optional[str] = pydantic.Field(
        None,
        title="ORCID for the user",
        description="The users ORCID if applicable",
    )
    uri: typing.Optional[str] = pydantic.Field(
        None, title="user URI", description="Full URL identifier for the user"
    )
    uuid: typing.Optional[pydantic.UUID4] = pydantic.Field(
        None, title="user UUID", description="The users UUID if applicable"
    )
    name: typing.Optional[str] = pydantic.Field(
        None, title="Full Name", description="Full name for the user"
    )

    class Config:
        extra = "forbid"


class LocalCLIConfig(pydantic.BaseModel):
    git: Git
    namespaces: Namespaces
    registries: typing.Dict[str, Registry]
    user: User

    class Config:
        extra = "forbid"


class GlobalCLIConfig(pydantic.BaseModel):
    namespaces: Namespaces
    registries: typing.Dict[str, Registry]
    user: User

    class Config:
        extra = "forbid"
