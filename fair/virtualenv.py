"""
Virtual Environment for Registries
----------------------------------

It was discovered that if building a FAIR-CLI binary "venv" would assume it
had been called by "python" and not "fair" which would break the setting up
of a virtual environment when installing a registry. As such a modified version
of the class which sets the Python executable manually is required.

"""

__date__ = "2022-01-05"

import logging
import os
import shutil
from types import SimpleNamespace
from venv import EnvBuilder


class PythonExecutableIdentificationError(Exception):
    def __init__(self):
        super().__init__("Failed to identify python executable")


class FAIREnv(EnvBuilder):
    _logger = logging.getLogger("FAIRDataPipeline.VirtualEnv")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def ensure_directories(self, env_dir) -> SimpleNamespace:
        self._logger.debug(f"Creating virtual environment in '{env_dir}'")
        _context: SimpleNamespace = super().ensure_directories(env_dir)
        _python_exe = shutil.which("python3")
        if not _python_exe:
            self._logger.warning("python3.exe not found trying python.exe")
            _python_exe = shutil.which("python")
            if not _python_exe:
                raise PythonExecutableIdentificationError

        self._logger.debug(f"Using python '{_python_exe}' for setup")

        _context.executable = _python_exe
        _dirname, _exename = os.path.split(os.path.abspath(_python_exe))
        _context.python_dir = _dirname
        _context.python_exe = _exename
        return _context
