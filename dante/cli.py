from pathlib import Path
import os
from click.types import DateTime
import toml

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


@cli.command()
@click.argument("config", type=click.Path(exists=True))
def run(config: str):
    """
    Run the submission script referred to in the given config file.

    read (and validate) the config.yaml file

    generate a working config.yaml file
        - globbing (* and ** replaced with all matching objects, all components listed), specific version numbers, and
          any variables (e.g. {CONFIG_PATH}, {VERSION}, {DATETIME}) is replaced with true values
        - register: is removed and external objects / data products are written in read:

    save the working config.yaml file in the local data store (in <local_store>/coderun/<date>-<time>/config.yaml) and
    register metadata in the data registry

    save the submission script to the local data store in <local_store>/coderun/<date>-<time>/script.sh (note that
    config.yaml should contain either script: that should be saved as the submission script, or script_path: that
    points to the file that should be saved as the submission script) and register metadata in the data registry

    save the path to <local_store>/coderun/<date>-<time>/ in the global environment as $dante_config_dir so that it can
    be picked up by the script that is run after this has been completed execute the submission script
    """
    click.echo(f"run command called with config {config}")


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
    user_home = Path.home()
    scrc_user_config = os.path.join(user_home, ".scrc", "config")
    if not os.path.exists(scrc_user_config):
        u_config = {}
    else:
        u_config = toml.load(scrc_user_config)
    if "user" not in u_config:
        u_config["user"] = {}
    u_config["user"]["name"] = user_name
    user_home = Path.home()
    scrc_user_dir = os.path.join(user_home, ".scrc", "users", user_name)
    scrc_user_dir.mkdir(parents=True, exist_ok=True)


@config.command(name="user.email")
@click.argument("user_email")
def config_email(user_email: str) -> None:
    user_home = Path.home()
    scrc_user_config = os.path.join(user_home, ".scrc", "danteconfig")
    if not os.path.exists(scrc_user_config):
        u_config = {}
    else:
        u_config = toml.load(scrc_user_config)
    if "user" not in u_config:
        u_config["email"] = {}
    u_config["user"]["email"] = user_email
