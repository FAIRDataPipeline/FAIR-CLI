#!/usr/bin/env python
"""
Command line interface to the FAIR Data Pipeline synchronisation tool.

BSD 2-Clause License

Copyright (c) 2021, Scottish COVID Response Consortium
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""
import click
import typing

import fair.session as fdp_session
import fair.common as fdp_com
import fair.services as fdp_serv
import fair.history as fdp_hist
import fair.configuration as fdp_conf

__author__ = "Scottish COVID Response Consortium"
__credits__ = ["Nathan Cummings (UKAEA)", "Kristian Zarebski (UKAEA)"]
__license__ = "BSD-2-Clause"
__status__ = "Development"
__copyright__ = "Copyright 2021, FAIR"


@click.group()
@click.version_option()
def cli():
    """Welcome to FAIR, the FAIR data pipeline command-line interface."""
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
    """Get the status of files under staging"""
    with fdp_session.FAIR() as s:
        s.status()


@cli.command()
def init() -> None:
    """Initialise repository in current location"""
    with fdp_session.FAIR() as fair_session:
        fair_session.initialise()


@cli.command()
def purge() -> None:
    """resets the repository deleting all local caches"""
    _purge = click.prompt(
        "Are you sure you want to reset fair tracking, "
        "this is not reversible [Y/N]? ",
        type=click.BOOL,
    )
    if _purge:
        with fdp_session.FAIR() as fair_session:
            fair_session.purge()


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
def reset(file_paths: typing.List[str]) -> None:
    """Removes files/runs from staging"""
    with fdp_session.FAIR() as fair_session:
        for file_name in file_paths:
            fair_session.change_staging_state(file_name, False)


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
def add(file_paths: typing.List[str]) -> None:
    """Add a file to staging"""
    with fdp_session.FAIR() as fair_session:
        for file_name in file_paths:
            fair_session.change_staging_state(file_name, True)


@cli.command()
@click.argument("file_paths", type=click.Path(exists=True), nargs=-1)
@click.option(
    "--cached/--not-cached",
    default=False,
    help="remove from tracking but do not delete from file system",
)
def rm(file_paths: typing.List[str], cached: bool = False) -> None:
    """removes files from system or just tracking"""
    with fdp_session.FAIR() as fair_session:
        for file_name in file_paths:
            fair_session.remove_file(file_name, cached)


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
    fdp_serv.download_data(config)


@cli.group(invoke_without_command=True)
@click.option(
    "--config",
    help="Specify alternate config.yaml",
    default=fdp_com.local_user_config(),
)
@click.pass_context
def run(ctx, config):
    """Initialises a run with the option to specify a bash command"""
    if not ctx.invoked_subcommand:
        with fdp_session.FAIR() as fair_session:
            fair_session.run(config)


@run.command()
@click.argument("bash_command")
@click.option(
    "--config",
    help="Specify alternate config.yaml",
    default=fdp_com.local_user_config(),
)
def bash(bash_command: str):
    """Run a BASH command and set this to be the default run command"""
    with fdp_session.FAIR() as fair_session:
        fair_session.run(config, bash_command)


@cli.group(invoke_without_command=True)
@click.option("--verbose/--no-verbose", "-v/")
@click.pass_context
def remote(ctx, verbose: bool = False):
    """List remotes if no additional command is provided"""
    if not ctx.invoked_subcommand:
        with fdp_session.FAIR() as fair_session:
            fair_session.list_remotes(verbose)


@remote.command()
@click.argument("options", nargs=-1)
def add(options: typing.List[str]) -> None:
    """Add a remote registry URL with option to give it a label if multiple
    remotes may be used.

    Parameters
    ----------
    options : typing.List[str]
        size 1 or 2 list containing either:
            - label, url
            - url
    """
    _url = options[1] if len(options) > 1 else options[0]
    _label = options[0] if len(options) > 1 else "origin"

    with fdp_session.FAIR() as fair_session:
        fair_session.add_remote(_url, _label)


@remote.command()
@click.argument("label")
def remove(label: str) -> None:
    """Removes the specified remote from the remotes list

    Parameters
    ----------
    label : str
        label of remote to remove
    """
    with fdp_session.FAIR() as fair_session:
        fair_session.remove_remove(label)


@remote.command()
@click.argument("options", nargs=-1)
def modify(options: typing.List[str]) -> None:
    """Modify a remote address

    Parameters
    ----------
    options : typing.List[str]
        List of 1 or 2 containing name of remote to modify
    """
    _label = options[0] if len(options) > 1 else "origin"
    _url = options[1] if len(options) > 1 else options[0]
    with fdp_session.FAIR() as fair_session:
        fair_session.modify_remote(_label, _url)


@cli.command()
def log() -> None:
    """Show a full run history"""
    fdp_hist.show_history()


@cli.command()
@click.argument("run_id")
def view(run_id: str) -> None:
    """View log for a given run"""
    fdp_hist.show_run_log(run_id)


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
    """Configure user information"""
    pass


@config.command(name="user.name")
@click.argument("user_name")
def config_user(user_name: str) -> None:
    """
    TODO: should update a user file in .scrc containing user information
    (API token, associated namespace, local data
    store, login node, and so on).
    """
    fdp_conf.set_user(user_name)


@config.command(name="user.email")
@click.argument("user_email")
def config_email(user_email: str) -> None:
    fdp_conf.set_email(user_email)
