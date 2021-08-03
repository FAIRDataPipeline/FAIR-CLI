import pytest
import os
import pathlib
import yaml

import fair.common as fdp_com
import fair.session as fdp_s


@pytest.mark.session
@pytest.mark.dependency()
def test_initialise(no_init_session):

    no_init_session.initialise()
    assert os.path.exists(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER)
    )


@pytest.mark.session
@pytest.mark.dependency()
def test_file_add(no_init_session):
    """Test staging of a file works.

    Expect that a new entry to be added matching relative file path of
    new file, and its status to be "True"
    """
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    pathlib.Path(_staged_file).touch()
    no_init_session.change_staging_state(_staged_file)
    no_init_session.close_session()
    assert yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )["../temp"]


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_add"])
def test_file_reset(no_init_session):
    """Test staging of a file works.

    Expect that a new entry to be added matching relative file path of
    new file, and its status to be "True"
    """
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.change_staging_state(_staged_file, False)
    no_init_session.close_session()
    assert not yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )["../temp"]
    no_init_session.change_staging_state(_staged_file, True)


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_reset"])
def test_file_remove_soft(no_init_session):
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.remove_file(_staged_file, cached=True)
    no_init_session.close_session()
    assert os.path.exists(_staged_file)
    assert "../temp" not in yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )


@pytest.mark.session
@pytest.mark.dependency(depends=["test_file_remove_soft"])
def test_file_remove(no_init_session):
    _staged_file = os.path.join(no_init_session._session_loc, "temp")
    no_init_session.change_staging_state(_staged_file)
    no_init_session.remove_file(_staged_file, cached=False)
    no_init_session.close_session()
    assert not os.path.exists(_staged_file)
    assert "../temp" not in yaml.safe_load(
        open(fdp_com.staging_cache(no_init_session._session_loc))
    )


@pytest.mark.session
def test_get_status(no_init_session, capfd):
    _tempdir = os.path.join(no_init_session._session_loc, "tempdir")
    os.makedirs(_tempdir)
    _staged_files = [os.path.join(_tempdir, f"temp_{i}") for i in range(5)]
    no_init_session.status()
    out, _ = capfd.readouterr()
    assert out == "Nothing marked for tracking.\n"
    for _staged in _staged_files:
        pathlib.Path(_staged).touch()
        no_init_session.change_staging_state(_staged)
    no_init_session.status()
    out, _ = capfd.readouterr()
    _expect = "Changes to be synchronized:\n\t\t"
    _expect += (
        "\n\t\t".join(
            f"../tempdir/{os.path.basename(file)}" for file in _staged_files
        )
        + "\n"
    )
    assert out == _expect
    for _staged in _staged_files:
        pathlib.Path(_staged).touch()
        no_init_session.change_staging_state(_staged, False)
    no_init_session.status()
    out, _ = capfd.readouterr()
    _expect = "Files not staged for synchronization:\n\t"
    _expect += '(use "fair add <file>..." to stage files)\n\t\t'
    _expect += (
        "\n\t\t".join(
            f"../tempdir/{os.path.basename(file)}" for file in _staged_files
        )
        + "\n"
    )
    assert out == _expect


@pytest.mark.session
@pytest.mark.dependency(depends=["test_initialise"])
def test_make_config(no_init_session):
    _cfg_yaml = os.path.join(no_init_session._session_loc, "config.yaml")
    os.remove(_cfg_yaml)
    no_init_session.make_starter_config()
    assert os.path.exists(_cfg_yaml)
    _config = yaml.safe_load(open(_cfg_yaml))
    _expected_meta_start = [
        "write_data_store",
        "default_input_namespace",
        "default_output_namespace",
        "description",
        "local_data_registry",
        "local_repo",
    ]
    for i in _expected_meta_start:
        assert i in _config["run_metadata"]


@pytest.mark.session
def test_init_cli(repo_root, no_prompt):
    fdp_s.FAIR(repo_root)
