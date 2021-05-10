import os
import sys

import click

from fdp.services import registry_running, download_data


@click.group()
def cli():
    """Welcome to the FAIR data pipeline command-line interface."""
    if os.path.exists("~/.scrc") and registry_running():
        click.echo(f"Local registry installed and running")
    else:
        click.echo(f"You do not have a local registry running. Please see "
                   "https://scottishcovidresponse.github.io/docs/data_pipeline/local_registry/"
                   "for information on how to install and run a local registry.")
        sys.exit(1)


@cli.command()
@click.argument("config", type=click.Path(exists=True))
def pull(config: str):
    """
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

    save the path to <local_store>/coderun/<date>-<time>/ in the global environment as $fdp_config_dir so that it can
    be picked up by the script that is run after this has been completed execute the submission script
    """
    click.echo(f"run command called with config {config}")


@cli.command()
@click.argument("api-token")
def push(api_token):
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


@cli.command()
def status():
    """
    Report on overall sync status of registry.
    """
    click.echo(f"status command called")


@cli.command()
def config():
    """
    TODO: should update a user file in .scrc containing user information (API token, associated namespace, local data
          store, login node, and so on).
    """
    click.echo(f"config command called")
