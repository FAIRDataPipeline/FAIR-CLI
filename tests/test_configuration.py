import pytest
import tempfile
import yaml
import os
import click

import fair.configuration as fdp_conf
import fair.common as fdp_com
import fair.exceptions as fdp_exc


@pytest.mark.configuration
def test_local_config_read(mocker):
    _dummy_cfg = {"tag": "value"}
    _temp_cfg = tempfile.mktemp()
    mocker.patch.object(fdp_com, "local_fdpconfig", lambda *args: _temp_cfg)
    assert not fdp_conf.read_local_fdpconfig("")
    yaml.dump(_dummy_cfg, open(_temp_cfg, "w"))
    assert fdp_conf.read_local_fdpconfig("")


@pytest.mark.configuration
def test_get_current_user_name(no_init_session):
    assert (
        fdp_conf.get_current_user_name(no_init_session._session_loc)
        == "Joe Bloggs"
    )


@pytest.mark.configuration
def test_access_bad_property(no_init_session):
    with pytest.raises(fdp_exc.CLIConfigurationError):
        fdp_conf._get_config_property(
            fdp_conf.read_global_fdpconfig(), "false", "false"
        )


@pytest.mark.configuration
def test_email_set(mocker, no_init_session):
    _email = "noreply@test"
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    mocker.patch.object(
        fdp_com,
        "local_fdpconfig",
        lambda *args: os.path.join(
            no_init_session._session_loc,
            fdp_com.FAIR_FOLDER,
            fdp_com.FAIR_CLI_CONFIG,
        ),
    )
    no_init_session.close_session()
    fdp_conf.set_email(no_init_session._session_loc, _email)
    assert (
        fdp_conf.read_local_fdpconfig(no_init_session._session_loc)["user"][
            "email"
        ]
        == _email
    )


@pytest.mark.configuration
def test_email_name_set(mocker, no_init_session):
    _email = "noreply@test"
    _name = "NoReply"
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    mocker.patch.object(
        fdp_com,
        "local_fdpconfig",
        lambda *args: os.path.join(
            no_init_session._session_loc,
            fdp_com.FAIR_FOLDER,
            fdp_com.FAIR_CLI_CONFIG,
        ),
    )
    no_init_session.close_session()
    fdp_conf.set_email(no_init_session._session_loc, _email, True)
    fdp_conf.set_user(no_init_session._session_loc, _name, True)
    assert (
        fdp_conf.read_local_fdpconfig(no_init_session._session_loc)["user"][
            "email"
        ]
        == _email
    )

    assert fdp_conf.read_global_fdpconfig()["user"]["email"] == _email

    assert (
        fdp_conf.read_local_fdpconfig(no_init_session._session_loc)["user"][
            "name"
        ]
        == _name
    )

    assert fdp_conf.read_global_fdpconfig()["user"]["name"] == _name


@pytest.mark.configuration
def test_glob_cfg_query(mocker, no_init_session):
    mocker.patch.object(click, "prompt", lambda *args, **kwargs: "TEST")
    _out_dict = fdp_conf.global_config_query()

    assert all(i in _out_dict for i in ["user", "remotes", "namespaces"])
    assert len(list(set(_out_dict["user"].values())))
    assert list(set(_out_dict["user"].values()))[0] == "TEST"
    assert "local" in _out_dict["remotes"] and "origin" in _out_dict["remotes"]
    assert (
        _out_dict["remotes"]["local"]
        == _out_dict["remotes"]["origin"]
        == "TEST"
    )
    assert (
        "input" in _out_dict["namespaces"]
        and "output" in _out_dict["namespaces"]
    )
    assert (
        _out_dict["namespaces"]["output"]
        == _out_dict["namespaces"]["input"]
        == "TEST"
    )
