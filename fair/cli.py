#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command Line Interface
======================

Main command line interface setup script for creation of commands used to
interact with the synchronisation tool.
"""

__date__ = "2021-06-24"

import os
import pathlib
import sys
import typing

import click
import click.shell_completion
import yaml

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.history as fdp_hist
import fair.registry.server as fdp_svr
import fair.run as fdp_run
import fair.session as fdp_session

__author__ = "Scottish COVID Response Consortium"
__credits__ = [
    "Richard Reeve (University of Glasgow)",
    "Nathan Cummings (UKAEA)",
    "Kristian Zarebski (UKAEA)",
    "Dennis Reddyhoff (University of Sheffield)",
]
__license__ = "BSD-2-Clause"
__status__ = "Development"
__copyright__ = "Copyright 2021, FAIR Data Pipeline"


def complete_yamls(ctx, param, incomplete):
    _file_list: typing.List[str] = [
        str(i) for i in pathlib.Path(os.getcwd()).rglob("*.yaml")
    ]
    _file_list += [str(i) for i in pathlib.Path(os.getcwd()).rglob("*.yml")]
    return [
        click.shell_completion.CompletionItem(k)
        for k in _file_list
        if k.startswith(incomplete)
    ]


def complete_jobs_data_products(ctx, param, incomplete) -> typing.List[str]:
    _staging_file = fdp_com.staging_cache(os.getcwd())
    if not os.path.exists(_staging_file):
        return []
    _staging_data = yaml.safe_load(open(_staging_file))
    _candidates = [d for d in _staging_data["data_product"].keys()]
    return [
        click.shell_completion.CompletionItem(c)
        for c in _candidates 
        if c.startswith(incomplete)
    ]


@click.group()
@click.version_option()
def cli():
    """Welcome to FAIR-CLI, the FAIR data pipeline command-line interface."""
    pass


@cli.command()
@click.option("--verbose/--not-verbose", help="Display URLs", default=False)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def status(verbose, debug) -> None:
    """Get the status of files under staging"""
    try:
        with fdp_session.FAIR(
            os.getcwd(), debug=debug, server_mode=fdp_svr.SwitchMode.CLI
        ) as fair_session:
            fair_session.status_data_products()
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.argument("output", nargs=-1)
def create(debug, output: str) -> None:
    """Generate a new FAIR repository user YAML config file"""
    output = (
        os.path.join(os.getcwd(), fdp_com.USER_CONFIG_FILE) if not output else output[0]
    )
    click.echo(f"Generating new user configuration file" f" '{output}'")
    with fdp_session.FAIR(os.getcwd(), debug=debug) as fair_session:
        fair_session.make_starter_config(output)


@cli.command()
@click.option(
    "--config",
    help="Create a starter user config.yaml file during initialisation",
    default=None,
)
@click.option(
    "--using",
    help="Initialise the CLI system from an existing CLI global configuration file",
    default="",
    shell_complete=complete_yamls,
)
@click.option(
    "--registry",
    help="Specify registry directory",
    default=None,
    show_default=True,
)
@click.option(
    "--ci/--standard",
    help="Run in testing mode for a CI system",
    default=False,
)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option("--export", help="Export the CLI configuration to a file", default="")
def init(
    config: str,
    debug: bool,
    using: str,
    registry: str,
    ci: bool,
    export: str = "",
) -> None:
    """Initialise repository in current location"""
    try:
        with fdp_session.FAIR(
            os.getcwd(), None, debug=debug, testing=ci
        ) as fair_session:
            _use_dict = {}
            if using:
                if not os.path.exists(using):
                    raise fdp_exc.FileNotFoundError(
                        f"Cannot load CLI configuration from file '{using}', "
                        "file does not exist."
                    )
                _use_dict = yaml.safe_load(open(using))
            fair_session.initialise(
                using=_use_dict, registry=registry, export_as=export
            )
            if config:
                fair_session.make_starter_config(config)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.option(
    "glob",
    "--global/--no-global",
    help="Also delete global FAIR-CLI directories",
    default=False,
)
@click.option(
    "--yes/--no",
    help="Deletes the configurations specified without prompt",
    default=False,
)
@click.option(
    "--data/--no-data",
    help="Also delete the local data directory",
    default=False,
)
@click.option(
    "--all/--not-all",
    help="Remove all FAIR interfaces and registry",
    default=False,
)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def purge(glob: bool, debug: bool, yes: bool, data: bool, all: bool) -> None:
    """Resets the repository deleting all local caches"""
    _purge = yes

    if all:
        all = click.confirm(
            "Are you sure you want to remove all FAIR components from this system?\n"
            "WARNING: This will also remove your local registry"
        )
    else:
        _purge = click.confirm(
            "Are you sure you want to reset FAIR tracking, " "this is not reversible?"
        )
        if data:
            data = click.confirm(
                "Are you sure you want to delete the local data directory?\n"
                "WARNING: Do not do this if you have a populated local registry"
            )
        if not _purge:
            return

    try:
        with fdp_session.FAIR(os.getcwd()) as fair_session:
            fair_session.purge(global_cfg=glob, clear_data=data, clear_all=all)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.group()
def registry() -> None:
    """Commands relating to control of the local registry server"""
    pass


@registry.command()
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def uninstall(debug: bool):
    """Uninstall the local registry from the system"""
    _confirm = click.confirm(
        "Are you sure you want to remove the local registry and its components?",
        default=False,
    )
    if not _confirm:
        return
    try:
        fdp_svr.uninstall_registry()
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@registry.command()
@click.option("--force/--no-force", help="Force a reinstall", default=False)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option("--directory", help="Installation location", default=None)
def install(debug: bool, force: bool, directory: str):
    """Install the local registry on the system"""
    try:
        fdp_svr.install_registry(install_dir=directory, force=force)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@registry.command()
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option("--port", help="port on which to run registry", default=8000)
def start(debug: bool, port: int) -> None:
    """Start the local registry server"""
    try:
        fdp_session.FAIR(
            os.getcwd(),
            server_mode=fdp_svr.SwitchMode.USER_START,
            debug=debug,
            server_port=port,
        )
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@registry.command()
@click.option("--force/--no-force", help="Force server stop", default=False)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def stop(force: bool, debug: bool) -> None:
    """Stop the local registry server"""
    _mode = fdp_svr.SwitchMode.FORCE_STOP if force else fdp_svr.SwitchMode.USER_STOP
    try:
        fdp_session.FAIR(os.getcwd(), server_mode=_mode, debug=debug)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def log(debug: bool) -> None:
    """Show a full job history"""
    try:
        fdp_hist.show_history(os.getcwd())
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.argument("job_id")
def view(job_id: str, debug: bool) -> None:
    """View log for a given job"""
    try:
        fdp_hist.show_job_log(os.getcwd(), job_id)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.argument("identifier")
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option("-j", "--job/--no-job", help="Stage entire job", default=False)
def unstage(identifier: str, debug: bool, job: bool) -> None:
    """Remove data products or jobs from staging"""
    try:
        with fdp_session.FAIR(
            os.getcwd(),
            debug=debug,
        ) as fair_session:
            fair_session.change_staging_state(
                identifier,
                "job" if job else "data_product",
                stage=False,
            )
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.argument("identifier", shell_complete=complete_jobs_data_products)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def add(identifier: str, debug: bool) -> None:
    """Add a data product to staging"""
    try:
        with fdp_session.FAIR(
            os.getcwd(),
            debug=debug,
        ) as fair_session:
            fair_session.change_staging_state(
                identifier,
                "data_product",
            )
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.argument("job_ids", nargs=-1)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option(
    "--cached/--not-cached",
    default=False,
    help="remove from tracking but do not delete from file system",
)
def rm(job_ids: typing.List[str], cached: bool = False, debug: bool = False) -> None:
    """Removes jobs from system or just tracking"""
    pass


@cli.command()
@click.argument("config", nargs=-1)
@click.option(
    "--script",
    help="Specify a shell command to execute, this will be inserted into the working config",
    default="",
)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.option(
    "--ci/--no-ci",
    help="Calls run passively without executing any commands for a CI system",
    default=False,
)
@click.option(
    "--dirty/--clean", help="Allow running with uncommitted changes", default=False
)
def run(config: str, script: str, debug: bool, ci: bool, dirty: bool):
    """Initialises a job with the option to specify a bash command"""
    # Allow no config to be specified, if that is the case use default local
    config = config[0] if config else fdp_com.local_user_config(os.getcwd())
    _run_mode = fdp_run.CMD_MODE.RUN if not ci else fdp_run.CMD_MODE.PASS
    try:
        with fdp_session.FAIR(
            os.getcwd(),
            config,
            debug=debug,
            server_mode=fdp_svr.SwitchMode.CLI,
        ) as fair_session:
            _hash = fair_session.run_job(script, mode=_run_mode, allow_dirty=dirty)
            if ci:
                click.echo(fdp_run.get_job_dir(_hash))
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.group(invoke_without_command=True)
@click.option("--verbose/--no-verbose", "-v/")
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
@click.pass_context
def remote(ctx, verbose: bool = False, debug: bool = False):
    """List remotes if no additional command is provided"""
    if not ctx.invoked_subcommand:
        try:
            with fdp_session.FAIR(os.getcwd(), debug=debug) as fair_session:
                fair_session.list_remotes(verbose)
        except fdp_exc.FAIRCLIException as e:
            if debug:
                raise e
            e.err_print()
            if e.level.lower() == "error":
                sys.exit(e.exit_code)


@remote.command()
@click.argument("options", nargs=-1)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def add(options: typing.List[str], debug: bool) -> None:
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

    try:
        with fdp_session.FAIR(os.getcwd(), debug=debug) as fair_session:
            fair_session.add_remote(_url, _label)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@remote.command()
@click.argument("label")
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def remove(label: str, debug: bool) -> None:
    """Removes the specified remote from the remotes list

    Parameters
    ----------
    label : str
        label of remote to remove
    """
    try:
        with fdp_session.FAIR(os.getcwd(), debug=debug) as fair_session:
            fair_session.remove_remove(label)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@remote.command()
@click.argument("label")
@click.argument("url")
@click.pass_context
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def modify(ctx, label: str, url: str, debug: bool) -> None:
    """Modify a remote address"""
    try:
        with fdp_session.FAIR(os.getcwd(), debug=debug) as fair_session:
            fair_session.modify_remote(label, url)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.command()
@click.argument("remote", nargs=-1)
@click.option("--debug/--no-debug", help="Run in debug mode", default=False)
def push(remote: str, debug: bool):
    """Push data between the local and remote registry"""
    remote = "origin" if not remote else remote[0]
    try:
        with fdp_session.FAIR(
            os.getcwd(), debug=debug, server_mode=fdp_svr.SwitchMode.CLI
        ) as fair_session:
            fair_session.push(remote)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


@cli.group()
def config():
    """Configure user information"""
    pass


@config.command(name="user.name")
@click.argument("user_name")
def config_user(user_name: str) -> None:
    fdp_conf.set_user(os.getcwd(), user_name)


@config.command(name="user.email")
@click.argument("user_email")
def config_email(user_email: str) -> None:
    fdp_conf.set_email(os.getcwd(), user_email)


@cli.command()
@click.argument("config", nargs=-1)
@click.option("--debug/--no-debug")
def pull(config: str, debug: bool):
    """Update local registry from remotes and sources"""
    config = config[0] if config != '' else fdp_com.local_user_config(os.getcwd())
    try:
        with fdp_session.FAIR(
            os.getcwd(),
            config,
            server_mode=fdp_svr.SwitchMode.CLI,
            debug=debug,
        ) as fair:
            fair.run_job(mode=fdp_run.CMD_MODE.PULL)
    except fdp_exc.FAIRCLIException as e:
        if debug:
            raise e
        e.err_print()
        if e.level.lower() == "error":
            sys.exit(e.exit_code)


if __name__ in "__main__":
    cli()
