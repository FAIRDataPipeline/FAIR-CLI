"""
Virtual Environment for Registries
----------------------------------

The local registry is a Django application, so its virtual environment needs a
real Python interpreter. Installed with pip, FAIR-CLI uses the Python it runs
under. A FAIR-CLI binary (PyInstaller) has no usable interpreter of its own:
``sys.executable`` is the binary itself. It therefore looks for one, in order:

1. the ``FAIR_PYTHON`` environment variable
2. ``python3``, ``python`` (and ``py -3`` on Windows) on the ``PATH``
3. ``uv``, which fetches a Python if the system has none

"""

__date__ = "2022-01-05"

import logging
import os
import platform
import shutil
import subprocess
import sys
import typing

import fair.exceptions as fdp_exc

# The registry (Django 5.2) supports no older Python
MIN_PYTHON = (3, 10)

_logger = logging.getLogger("FAIRDataPipeline.VirtualEnv")


def _is_suitable(python: typing.List[str]) -> bool:
    """Whether the interpreter runs and is at least MIN_PYTHON"""
    try:
        return (
            subprocess.run(
                [
                    *python,
                    "-c",
                    f"import sys; sys.exit(sys.version_info < {MIN_PYTHON})",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=30,
            ).returncode
            == 0
        )
    except (OSError, subprocess.TimeoutExpired):
        return False


def find_python() -> typing.Optional[typing.List[str]]:
    """Command for the Python interpreter to build the registry environment with

    Returns
    -------
    typing.Optional[typing.List[str]]
        the interpreter command, or None if no suitable one was found
    """
    if not getattr(sys, "frozen", False):
        return [sys.executable]

    _min = ".".join(map(str, MIN_PYTHON))

    if os.environ.get("FAIR_PYTHON"):
        _python = [os.environ["FAIR_PYTHON"]]
        # A Python chosen explicitly is not silently replaced by another
        if not _is_suitable(_python):
            raise fdp_exc.RegistryError(
                f"FAIR_PYTHON '{_python[0]}' is not a Python >= {_min}",
                hint=f"Set FAIR_PYTHON to a Python >= {_min} interpreter, or unset it",
            )
        return _python

    _candidates = [["python3"], ["python"]]
    if platform.system() == "Windows":
        _candidates.append(["py", "-3"])

    for _candidate in _candidates:
        _exe = shutil.which(_candidate[0])
        if _exe and _is_suitable([_exe, *_candidate[1:]]):
            return [_exe, *_candidate[1:]]

    return None


def venv_command(
    venv_dir: str, prompt: typing.Optional[str] = None
) -> typing.List[str]:
    """Command that creates a virtual environment, with pip, for the local registry

    Found before any installation work, so a missing Python fails early.

    Parameters
    ----------
    venv_dir : str
        directory to create the environment in
    prompt : str, optional
        prompt prefix shown when the environment is activated

    Returns
    -------
    typing.List[str]
        the command to run
    """
    _prompt = ["--prompt", prompt] if prompt else []
    _python = find_python()

    if _python:
        return [*_python, "-m", "venv", *_prompt, venv_dir]

    _min = ".".join(map(str, MIN_PYTHON))
    _uv = shutil.which("uv")

    if not _uv:
        raise fdp_exc.RegistryError(
            f"The local registry needs Python >= {_min}, and none was found",
            hint=f"Install Python >= {_min}, set FAIR_PYTHON to one, or install uv "
            "(https://docs.astral.sh/uv/) to have one fetched",
        )

    _logger.debug("No suitable Python found, using uv")
    # --seed installs pip, used to install the registry's requirements
    return [_uv, "venv", "--seed", "--python", f">={_min}", *_prompt, venv_dir]


def create_venv(venv_dir: str, prompt: typing.Optional[str] = None) -> None:
    """Create a virtual environment, with pip, for the local registry

    Parameters
    ----------
    venv_dir : str
        directory to create the environment in
    prompt : str, optional
        prompt prefix shown when the environment is activated
    """
    _cmd = venv_command(venv_dir, prompt)
    _logger.debug(f"Creating virtual environment in '{venv_dir}': {_cmd}")
    subprocess.check_call(_cmd)
