import os
import pathlib
import typing

import click.testing
import pytest
import pytest_mock
import yaml

import fair.registry.server as fdp_serv
from fair.cli import cli
from fair.common import FAIR_FOLDER
from fair.registry.requests import get, url_get
from tests.conftest import RegistryTest

REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(
    os.path.dirname(__file__), "data", "test_pull_config.yaml"
)