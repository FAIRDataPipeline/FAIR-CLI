from pathlib import Path
import os
from click.types import DateTime
import toml
import hashlib

import click

from dante.services import registry_installed, registry_running, download_data
from dante.staging import DANTE

from typing import List


@click.group()
@click.version_option()
def cli():
    """Welcome to DANTE the FAIR data pipeline command-line interface."""
    # if registry_installed() and registry_running():
    #     click.echo("Local registry installed and running")
    # else:
    #     click.echo(
    #         "You do not have a local registry running. Please see "
    #         "https://scottishcovidresponse.github.io/docs/data_pipeline/local_registry/"
    #         "for information on how to install and run a local registry."
    #     )
    #     sys.exit(1)
    pass


@cli.command(name="user-config")
def make_config() -> None:
    """Generate a starter config file"""
    with DANTE() as dante:
        dante.make_starter_config()


@cli.command()
def status() -> None:
    """
    Get the status of staging
    """
    with DANTE() as s:
        s.status()


@cli.command()
def init() -> None:
    """Initialise repository in current location"""
    with DANTE() as dante:
        dante.initialise()


@cli.command()
def purge() -> None:
    _purge = click.prompt(
        "Are you sure you want to reset dante tracking, "
        "this is not reversible [Y/N]? ",
        type=click.BOOL,
    )
    if _purge:
        with DANTE() as dante:
            dante.purge()


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
def reset(file_paths: List[str]) -> None:
    with DANTE() as dante:
        for file_name in file_paths:
            dante.change_staging_state(file_name, False)


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
def add(file_paths: List[str]) -> None:
    """Add a file to staging"""
    with DANTE() as dante:
        for file_name in file_paths:
            dante.change_staging_state(file_name, True)


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
@click.option(
    "--cached/--not-cached",
    default=False,
    help="remove from tracking but do not delete from file system",
)
def rm(file_paths: List[str], cached: bool = False) -> None:
    with DANTE() as dante:
        for file_name in file_paths:
            dante.remove_file(file_name, cached)


@cli.command()
@click.argument("config", type=click.Path(exists=True))
def pull(config: str):
    """parate scripts to add their data/results to the local registry. However for static languages like C++ they will likel i
    download any data required by read: from the remote data store and record metadata in the data registry (whilst
    editing relevant entries, e.g. storage_root)

    pull data associated with all previous versions of these objects from the remote data registry

    download any data listed in register: from the original source and record metadata in the data registry

    local_repo: must always be given in the config.yaml file
        - get the remote repo url from the local repo
        - get the hash of the latest commit on GitHub and store this in the data registry, associated with the submission
          script storage_location (this is where the script should be stored)
        - note that there are exceptions and the user may reference a script located outside of a repository
    """
    click.echo(f"pull command called with config: {config}")
    download_data(config)


@cli.group(invoke_without_command=True)
@click.pass_context
def run(ctx):
    if not ctx.invoked_subcommand:
        with DANTE() as dante:
            dante.run_bash_command()


@run.command()
@click.argument("bash_command")
def bash(bash_command: str):
    """Run a BASH command and set this to be the default run command"""
    with DANTE() as dante:
        dante.run_bash_command(bash_command)


@cli.group(invoke_without_command=True)
@click.option("--verbose/--no-verbose", "-v/")
@click.pass_context
def remote(ctx, verbose: bool = False):
    """List remotes if no additional command is provided"""
    if not ctx.invoked_subcommand:
        with DANTE() as dante:
            dante.list_remotes(verbose)


@remote.command()
@click.argument("options", nargs=-1)
def add(options: List[str]) -> None:
    """Add a remote registry URL with option to give it a label if multiple
    remotes may be used.

    Parameters
    ----------
    options : List[str]
        size 1 or 2 list containing either:
            - label, url
            - url
    """
    _url = options[1] if len(options) > 1 else options[0]
    _label = options[0] if len(options) > 1 else "origin"

    with DANTE() as dante:
        dante.add_remote(_url, _label)


@remote.command()
@click.argument("label")
def remove(label: str) -> None:
    """Removes the specified remote from the remotes list

    Parameters
    ----------
    label : str
        label of remote to remove
    """
    with DANTE() as dante:
        dante.remove_remove(label)


@remote.command()
@click.argument("options", nargs=-1)
def modify(options: List[str]) -> None:
    """Modify a remote address

    Parameters
    ----------
    options : List[str]
        List of 1 or 2 containing name of remote to modify
    """
    _label = options[0] if len(options) > 1 else "origin"
    _url = options[1] if len(options) > 1 else options[0]
    with DANTE() as dante:
        dante.modify_remote(_label, _url)


@cli.command()
def log() -> None:
    with DANTE() as dante:
        dante.show_history()


@cli.command()
@click.argument("api-token")
def push(api_token: str):
    """
    push new files (generated from write: and register:) to the remote data store

    record metadata in the data registry (whilst editing relevant entries, e.g. storage_root)
    """
    click.echo(f"push command called")
    # headers = {
    #     'Authorization': 'token: api_token'
    # }
    #
    # data = {
    #
    # }
    #
    # requests.put('https://data.scrc.uk/api/object/', data, headers=headers)


@cli.group()
def config():
    pass


@config.command(name="user.name")
@click.argument("user_name")
def config_user(user_name: str) -> None:
    """
    TODO: should update a user file in .scrc containing user information
    (API token, associated namespace, local data
    store, login node, and so on).
    """
    with DANTE() as dante:
        dante.set_user(user_name)


@config.command(name="user.email")
@click.argument("user_email")
def config_email(user_email: str) -> None:
    with DANTE() as dante:
        dante.set_email(user_email)
