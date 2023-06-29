import os

import git
import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc


@pytest.mark.faircli_common
def test_find_git_root(tmp_path):
    tempd = tmp_path.__str__()
    with pytest.raises(fdp_exc.UserConfigError):
        fdp_com.find_git_root(tempd)
    git.Repo.init(tempd)
    _proj_dir = os.path.join(tempd, "project")
    os.makedirs(_proj_dir)
    assert os.path.realpath(fdp_com.find_git_root(tempd)) == os.path.realpath(tempd)
    assert os.path.realpath(fdp_com.find_git_root(_proj_dir)) == os.path.realpath(tempd)


@pytest.mark.faircli_common
def test_find_fair_root(tmp_path):
    tempd = tmp_path.__str__()
    assert not fdp_com.find_fair_root(tempd)
    os.makedirs(os.path.join(tempd, fdp_com.FAIR_FOLDER))
    _proj_dir = os.path.join(tempd, "project")
    os.makedirs(_proj_dir)
    assert fdp_com.find_fair_root(tempd) == tempd
    assert fdp_com.find_fair_root(_proj_dir) == tempd


@pytest.mark.faircli_common
def test_staging_cache(tmp_path):
    tempd = tmp_path.__str__()
    _fair_dir = os.path.join(tempd, fdp_com.FAIR_FOLDER)
    os.makedirs(_fair_dir)
    assert fdp_com.staging_cache(tempd) == os.path.join(
        _fair_dir, "staging"
    )


@pytest.mark.faircli_common
def test_default_data(mocker: pytest_mock.MockerFixture, tmp_path):
    tempd = tmp_path.__str__()
    _glob_conf = os.path.join(tempd, "cli-config.yaml")
    mocker.patch("fair.common.global_fdpconfig", lambda: _glob_conf)
    with pytest.raises(fdp_exc.InternalError):
        fdp_com.default_data_dir()
    with open(_glob_conf, "w") as out_f:
        yaml.dump({"registries": {"local": {}}}, out_f)
    _fair_dir = os.path.join(tempd, fdp_com.FAIR_FOLDER)
    mocker.patch("fair.common.USER_FAIR_DIR", _fair_dir)
    assert fdp_com.default_data_dir() == os.path.join(
        _fair_dir, f"data{os.path.sep}"
    )
    with open(_glob_conf, "w") as out_f:
        yaml.dump(
            {"registries": {"local": {"data_store": "data_store_1"}}},
            out_f,
        )
    assert fdp_com.default_data_dir() == "data_store_1"


@pytest.mark.faircli_common
def test_registry_home(mocker: pytest_mock.MockerFixture, tmp_path):
    tempd = tmp_path.__str__()
    _glob_conf = os.path.join(tempd, "cli-config.yaml")
    mocker.patch("fair.common.global_fdpconfig", lambda: _glob_conf)
    with open(_glob_conf, "w") as out_f:
        yaml.dump({}, out_f)
    assert fdp_com.registry_home() == fdp_com.DEFAULT_REGISTRY_LOCATION
    with open(_glob_conf, "w") as out_f:
        yaml.dump({"registries": {}}, out_f)
    with pytest.raises(fdp_exc.CLIConfigurationError):
        fdp_com.registry_home()
    with open(_glob_conf, "w") as out_f:
        yaml.dump({"registries": {"local": {}}}, out_f)
    with pytest.raises(fdp_exc.CLIConfigurationError):
        fdp_com.registry_home()
    with open(_glob_conf, "w") as out_f:
        yaml.dump(
            {"registries": {"local": {"directory": "registry"}}}, out_f
        )
    assert fdp_com.registry_home() == "registry"
