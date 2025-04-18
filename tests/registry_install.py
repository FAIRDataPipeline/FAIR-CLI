#!/usr/bin/env python3
import glob
import os
import pathlib
import shutil
import subprocess
import time
import typing
import platform

import click
import git
import requests

from fair.common import FAIR_FOLDER
from fair.common import remove_readonly
from fair.virtualenv import FAIREnv

FAIR_REGISTRY_REPO = "https://github.com/FAIRDataPipeline/data-registry.git"
TEST_DRAMS_FILE = os.path.join(
    os.path.dirname(__file__), "data", "registry-test-settings.py"
)
TEST_DRAMS = "drams.registry-test-settings"
CONFIG_INI = os.path.join(os.path.dirname(__file__), "data", "config.ini")


def django_environ(environ: typing.Dict = os.environ, remote: bool = False):
    _environ = environ.copy()
    _environ["DJANGO_SUPERUSER_USERNAME"] = "admin"
    _environ["DJANGO_SUPERUSER_PASSWORD"] = "admin"
    if remote:
        _environ["DJANGO_SETTINGS_MODULE"] = TEST_DRAMS
        _environ["FAIR_CONFIG"] = CONFIG_INI
    else:
        _environ["DJANGO_SETTINGS_MODULE"] = "drams.local-settings"
    return _environ


def rebuild_local(
    python: str, install_dir: str = None, silent: bool = False, remote: bool = False
):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry")

    _migration_files = glob.glob(os.path.join(install_dir, "*", "migrations", "*.py*"))

    for mf in _migration_files:
        os.remove(mf)

    _db_file = os.path.join(install_dir, "db.sqlite3")

    if os.path.exists(_db_file):
        os.remove(_db_file)

    _manage = os.path.join(install_dir, "manage.py")

    _sub_cmds = [
        ("makemigrations", "custom_user"),
        ("makemigrations", "data_management"),
        ("migrate",),
        (
            "graph_models",
            "data_management",
            "--arrow-shape",
            "crow",
            "-x",
            '"BaseModel,DataObject,DataObjectVersion"',
            "-E",
            "-o",
            os.path.join(install_dir, "schema.dot"),
        ),
        ("collectstatic", "--noinput"),
        ("createsuperuser", "--noinput"),
        ("set_site_info",),
    ]

    for sub in _sub_cmds:
        subprocess.check_call(
            [python, _manage, *sub],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None,
            env=django_environ(remote=remote),
        )

    if shutil.which("dot"):
        subprocess.check_call(
            [
                shutil.which("dot"),
                os.path.join(install_dir, "schema.dot"),
                "-Tsvg",
                "-o",
                os.path.join(install_dir, "static", "images", "schema.svg"),
            ],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None,
        )


def install_registry(
    repository: str = FAIR_REGISTRY_REPO,
    reference: str = None,
    install_dir: str = None,
    silent: bool = False,
    force: bool = False,
    venv_dir: str = None,
    remote: bool = False,
) -> None:

    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry")

    if force:
        shutil.rmtree(install_dir, onerror=remove_readonly)

    os.makedirs(os.path.dirname(install_dir), exist_ok=True)

    _repo = git.Repo.clone_from(repository, install_dir)

    # If no reference is specified, use the latest tag for the registry
    if not reference:
        _tags = sorted(_repo.tags, key=lambda t: t.commit.committed_datetime)
        reference = _tags[-1].name

    _repo.git.checkout(reference)

    if not venv_dir:
        venv_dir = os.path.join(install_dir, "venv")

        _venv = FAIREnv(with_pip=True, prompt="RegistryTest")

        _venv.create(venv_dir)

    _venv_bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    _venv_python = shutil.which("python", path=os.path.join(venv_dir, _venv_bin_dir))

    if not _venv_python:
        raise FileNotFoundError(f"Failed to find 'python' in location '{venv_dir}")

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "--upgrade", "pip", "wheel"],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    # subprocess.check_call(
    #     [_venv_python, "-m", "pip", "install", "whitenoise"],
    #     shell=False,
    #     stdout=subprocess.DEVNULL if silent else None,
    # )

    _requirements = os.path.join(install_dir, "requirements.txt")

    if not os.path.exists(_requirements):
        raise FileNotFoundError(f"Expected file '{_requirements}'")

    subprocess.check_call(
        [_venv_python, "-m", "pip", "install", "-r", _requirements],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None,
    )

    if remote:
        _TEST_DRAMS_FILE = os.path.join(
            install_dir, "drams", "registry-test-settings.py"
        )
        shutil.copy2(TEST_DRAMS_FILE, _TEST_DRAMS_FILE)
        # with open(_TEST_DRAMS_FILE, "a") as _file:
        #    _file.write("\n")
        #    _file.write(f'CONFIG_LOCATION = "{CONFIG_INI}"')
        # print(f'Using Config: {CONFIG_INI}')
    rebuild_local(_venv_python, install_dir, silent, remote=remote)

    print(f"[REGISTRY] Installed registry version '{reference}'")
    print(f'using: {django_environ(remote = remote)["DJANGO_SETTINGS_MODULE"]}')

    return reference


def refresh(
    install_dir: str = None,
    silent: bool = False,
    venv_dir: str = None,
    remote: bool = False,
):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry")

    _venv_dir = venv_dir or os.path.join(install_dir, "venv")

    if not os.path.exists(_venv_dir):
        raise FileNotFoundError(
            f"Location '{install_dir}' is not a valid registry install"
        )

    _venv_bin_dir = "Scripts" if platform.system() == "Windows" else "bin"
    _venv_python = shutil.which("python", path=os.path.join(_venv_dir, _venv_bin_dir))

    rebuild_local(_venv_python, install_dir, silent, remote)


def launch(
    install_dir: str = None,
    port: int = 8000,
    silent: bool = False,
    venv_dir: str = None,
    remote: bool = False,
):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry")

    _venv_dir = venv_dir or os.path.join(install_dir, "venv")

    if not os.path.exists(_venv_dir):
        raise FileNotFoundError(
            f"Location '{install_dir}' is not a valid registry install"
        )

    _manage = os.path.join(install_dir, "manage.py")

    _venv_bin_dir = "Scripts" if platform.system() == "Windows" else "bin"

    _venv_python = shutil.which("python", path=os.path.join(_venv_dir, _venv_bin_dir))

    with open(os.path.join(install_dir, "session_port.log"), "w") as out_f:
        out_f.write(str(port))

    with open(os.path.join(install_dir, "output.log"), "w") as out_f:
        _process = subprocess.Popen(
            [_venv_python, _manage, "runserver", str(port)],
            stdout=out_f,
            env=django_environ(remote=remote),
            stderr=subprocess.STDOUT,
            shell=False,
        )

    _connection_time = 0

    while _connection_time < 10:
        try:
            _req = requests.get(f"http://127.0.0.1:{port}/api")
            break
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            _connection_time += 1
            continue

    if _connection_time == 10:
        _log_text = open(os.path.join(install_dir, "output.log"))
        raise requests.ConnectionError(f"Log reads:\n{_log_text.read()}")

    if _req.status_code != 200:
        raise requests.ConnectionError("Error starting local registry")

    _token_path = os.path.join(install_dir, "token")
    if os.path.isfile(_token_path):
        os.remove(_token_path)
    with open(os.path.join(install_dir, "token"), "w") as out_f:
        subprocess.check_call(
            [_venv_python, _manage, "get_token"],
            stdout=out_f,
            stderr=subprocess.STDOUT,
            env=django_environ(remote=remote),
            shell=False,
        )

    if not os.path.exists(os.path.join(install_dir, "token")):
        raise FileNotFoundError("Expected token file, but none created.")

    if open(_token_path).read().strip() == "":
        raise FileNotFoundError("Expected token. but file empty.")

    if not silent:
        click.echo(
            "An access token for the REST API is available in the file"
            f"'{os.path.join(install_dir, 'token')}'"
        )
        if not os.path.exists(os.path.join(install_dir, "token")):
            raise AssertionError("Expected token file, but none created")
        if not open(os.path.join(install_dir, "token")).read().strip():
            raise AssertionError("Expected token in token file, but file empty")

    if not shutil.which("dot") and not silent:
        click.echo(
            "WARNING: Graphviz is not installed, so provenance report images are not available"
        )

    return _process


def stop(install_dir: str = None, port: int = 8000, silent: bool = False):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry")

    _manage = os.path.join(install_dir, "manage.py")

    if platform.system() == "Windows":
        _call = os.path.join(install_dir, "scripts", "stop_fair_registry_windows.bat")
    else:
        _call = ["pgrep", "-f", f'"{_manage} runserver"', "|", "xargs", "kill"]

    subprocess.check_call(
        _call,
        env=django_environ(),
        shell=False,
    )

    try:
        requests.get(f"http://127.0.0.1:{port}/api")
        raise AssertionError("Expected registry termination")
    except requests.ConnectionError:
        pass


@click.group()
def fair_reg():
    pass


@fair_reg.command(name="launch")
@click.option(
    "--directory",
    default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry"),
    help="Install location",
)
@click.option("--port", help="Port to run registry on", default=8000)
@click.option("--silent/--normal", help="Run in silent mode", default=False)
@click.option("--remote--local", help="Run in silent mode", default=False)
def reg_launch(directory, port, silent, remote):
    launch(directory, port, silent, remote)


@fair_reg.command(name="stop")
@click.option(
    "--directory",
    default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry"),
    help="Install location",
)
@click.option("--silent/--normal", help="Run in silent mode", default=False)
def reg_stop(directory, silent):
    stop(directory, silent)


@fair_reg.command(name="install")
@click.option(
    "--repository",
    default=FAIR_REGISTRY_REPO,
    help="FAIR Data Registry Repository",
)
@click.option(
    "--head",
    default="main",
    help="Head to use for checkout e.g. branch, tag etc.",
)
@click.option(
    "--directory",
    default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry"),
    help="Install location",
)
@click.option("--silent/--normal", help="Run in debug mode", default=False)
@click.option("--force/--no-force", help="Force re-install", default=False)
@click.option("--remote--local", help="Run in silent mode", default=False)
def reg_install(repository, head, directory, silent, force, remote):
    if force:
        force = click.confirm(
            f"Are you sure you want to remove directory '{directory}'?",
            default=False,
        )
    install_registry(repository, head, directory, silent, force, remote=remote)


@fair_reg.command(name="refresh")
@click.option(
    "--directory",
    default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, "registry"),
    help="Install location",
)
@click.option("--silent/--normal", help="Run in debug mode", default=False)
@click.option("--remote--local", help="Run in silent mode", default=False)
def reg_refresh(directory, silent, remote):
    refresh(directory, silent, remote)


if __name__ in "__main__":
    fair_reg()
