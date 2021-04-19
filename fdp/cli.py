import click


@click.group()
def cli():
    """Welcome to the FAIR data pipeline command-line interface."""
    pass


@cli.command()
@click.argument("config", type=click.Path(exists=True))
def run(config):
    """
    Run the submission script referred to in the given config file.
    """
    click.echo(f"run command called with config {config}")


@cli.command()
@click.argument("config", type=click.Path(exists=True))
def pull(config):
    """
    Pull all of the registry entries relevant to the given config file into the local registry, and download any files
    that are referred to in read: or register: blocks to the local data store.
    """
    click.echo(f"pull command called with config: {config}")


@cli.command()
def commit():
    """
    mark a code run or a specific file (and all of its provenance) to be synced back to the remote registry (once we’ve
    worked out how to refer to code runs in an intuitive way?)
    """
    click.echo(f"commit command called")


@cli.command()
def push():
    """
    Sync back any committed changes to the remote registry and upload any associated data to the remote data store
    (but we still haven’t determined how to specify what to store where).
    """
    click.echo(f"push command called")


@cli.command()
def status():
    """
    Report on overall sync status of registry.
    """
    click.echo(f"status command called")
