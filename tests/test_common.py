import pytest
import os

import fair.common as fdp_com
from . import switch_dir


@pytest.mark.common
def test_fair_root(set_constant_paths, initialise_test_repo):
    assert fdp_com.find_fair_root(initialise_test_repo) == initialise_test_repo


@pytest.mark.common
def test_staging_cache(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.staging_cache() == os.path.join(
            initialise_test_repo, fdp_com.FAIR_FOLDER, "staging"
        )


@pytest.mark.common
def test_data_dir(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.data_dir() == os.path.join(set_constant_paths, "data")


@pytest.mark.common
def test_local_fdpconfig(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.local_fdpconfig() == os.path.join(
            initialise_test_repo, fdp_com.FAIR_FOLDER, fdp_com.FAIR_CLI_CONFIG
        )


@pytest.mark.common
def test_local_user_config(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.local_user_config() == os.path.join(
            initialise_test_repo, "config.yaml"
        )


@pytest.mark.common
def test_code_dir(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.coderun_dir() == os.path.join(
            set_constant_paths, "data", "coderun"
        )


@pytest.mark.common
def test_global_config_dir(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.global_config_dir() == os.path.join(
            set_constant_paths, "cli"
        )


@pytest.mark.common
def test_global_config(set_constant_paths, initialise_test_repo):
    with switch_dir(initialise_test_repo):
        assert fdp_com.global_fdpconfig() == os.path.join(
            set_constant_paths, "cli", fdp_com.FAIR_CLI_CONFIG
        )
