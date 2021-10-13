#!/usr/bin/env python3
import git
import os
import click
import venv
import logging
import shutil
import subprocess
import pathlib
import glob

from fair.common import FAIR_FOLDER

FAIR_REGISTRY_REPO = "https://github.com/FAIRDataPipeline/data-registry.git"


def django_environ():
    _environ = os.environ.copy()
    _environ['DJANGO_SETTINGS_MODULE'] = 'drams.local-settings'
    _environ['DJANGO_SUPERUSER_USERNAME'] = 'admin'
    _environ['DJANGO_SUPERUSER_PASSWORD'] = 'admin'
    return _environ


def rebuild_local(python: str, install_dir: str = None, silent: bool = False):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, 'registry')
    
    _migration_files = glob.glob(os.path.join(install_dir, '*', 'migrations', '*.py*'))

    for mf in _migration_files:
        os.remove(mf)

    _db_file = os.path.join(install_dir, 'db.sqlite3')

    if os.path.exists(_db_file):
        os.remove(_db_file)

    _manage = os.path.join(install_dir, 'manage.py')

    _sub_cmds = [
        ('makemigrations', 'custom_user'),
        ('makemigrations', 'data_management'),
        ('migrate',),
        (
            'graph_models',
            'data_management',
            '--arrow-shape',
            'crow',
            '-x',
            '"BaseModel,DataObject,DataObjectVersion"',
            '-E',
            '-o',
            os.path.join(install_dir, 'schema.dot')
        ),
        ('collectstatic', '--noinput'),
        ('createsuperuser', '--noinput')
    ]

    for sub in _sub_cmds:
        subprocess.check_call(
            [python, _manage, *sub],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None,
            env=django_environ()
        )

    if shutil.which('dot'):
        subprocess.check_call(
            [
                shutil.which('dot'),
                os.path.join(install_dir, 'schema.dot'),
                '-Tsvg',
                '-o',
                os.path.join(install_dir, 'static', 'images', 'schema.svg')
            ],
            shell=False,
            stdout=subprocess.DEVNULL if silent else None
        )


def install_registry(
    repository: str = FAIR_REGISTRY_REPO,
    head: str = 'main',
    install_dir: str = None,
    silent: bool = False,
    force: bool = False) -> None:

    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, 'registry')

    if force:
        shutil.rmtree(install_dir, ignore_errors=True)

    os.makedirs(os.path.dirname(install_dir), exist_ok=True)

    _repo = git.Repo.clone_from(repository, install_dir)

    if head not in _repo.heads:
        raise FileNotFoundError(f"No such HEAD '{head}' in registry repository")
    else:
        _repo.heads[head].checkout()

    _venv_dir = os.path.join(install_dir, 'venv')

    venv.create(_venv_dir, with_pip=True, prompt='TestRegistry',)

    _venv_python = shutil.which('python', path=os.path.join(_venv_dir, 'bin'))

    if not _venv_python:
        raise FileNotFoundError(
            f"Failed to find 'python' in location '{_venv_dir}"
        )

    subprocess.check_call(
        [_venv_python, '-m', 'pip', 'install', '--upgrade', 'pip', 'wheel'],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None
    )

    subprocess.check_call(
        [_venv_python, '-m', 'pip', 'install', 'whitenoise'],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None
    )

    _requirements = os.path.join(install_dir, 'requirements.txt')

    if not os.path.exists(_requirements):
        raise FileNotFoundError(
            f"Expected file '{_requirements}'"
        )

    subprocess.check_call(
        [_venv_python, '-m', 'pip', 'install', '-r', _requirements],
        shell=False,
        stdout=subprocess.DEVNULL if silent else None
    )

    rebuild_local(_venv_python, install_dir, silent)


def refresh(install_dir: str = None, silent: bool = False):
    if not install_dir:
        install_dir = os.path.join(pathlib.Path.home(), FAIR_FOLDER, 'registry')
    
    _venv_dir = os.path.join(install_dir, 'venv')

    if not os.path.exists(_venv_dir):
        raise FileNotFoundError(
            f"Location '{install_dir}' is not a valid registry install"
        )

    _venv_python = shutil.which('python', path=os.path.join(_venv_dir, 'bin'))

    rebuild_local(_venv_python, install_dir, silent)


@click.group()
def fair_reg():
    pass


@fair_reg.command()
@click.option('--repository', default=FAIR_REGISTRY_REPO, help='FAIR Data Registry Repository')
@click.option('--head', default='main', help='Head to use for checkout e.g. branch, tag etc.')
@click.option('--directory', default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, 'registry'), help='Install location')
@click.option('--silent/--normal', help='Run in debug mode', default=False)
@click.option('--force/--no-force', help='Force re-install', default=False)
def install(repository, head, directory, silent, force):
    if force:
        force = click.confirm(f"Are you sure you want to remove directory '{directory}'?", default=False)
    install_registry(repository, head, directory, silent, force)


@fair_reg.command(name='refresh')
@click.option('--directory', default=os.path.join(pathlib.Path.home(), FAIR_FOLDER, 'registry'), help='Install location')
@click.option('--silent/--normal', help='Run in debug mode', default=False)
def reg_refresh(directory, silent):
    refresh(directory, silent)

if __name__ in "__main__":
    fair_reg()
    