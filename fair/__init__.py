import toml
from pathlib import Path
import os

__version__ = toml.load(
    os.path.join(Path(os.path.join(os.path.dirname(__file__))).parent, "pyproject.toml")
)["tool"]["poetry"]["version"]
