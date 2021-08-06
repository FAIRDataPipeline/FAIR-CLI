import pytest
import tempfile
import yaml
import os

import fair.configuration as fdp_conf
import fair.common as fdp_com
import fair.server as fdp_serv


@pytest.mark.configuration
def test_local_config_read(mocker, no_registry_autoinstall):
    _dummy_cfg = {"tag": "value"}
    _temp_cfg = tempfile.mktemp()
    mocker.patch.object(fdp_com, "local_fdpconfig", lambda *args: _temp_cfg)
    assert not fdp_conf.read_local_fdpconfig("")
    yaml.dump(_dummy_cfg, open(_temp_cfg, "w"))
    assert fdp_conf.read_local_fdpconfig("")


@pytest.mark.configuration
def test_get_current_user_name(no_init_session):
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    no_init_session.close_session()
    assert (
        " ".join(fdp_conf.get_current_user_name(no_init_session._session_loc))
        == "Joe Bloggs"
    )


@pytest.mark.configuration
def test_get_current_user_uuid(no_init_session):
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    no_init_session.close_session()
    assert fdp_conf.get_current_user_uuid(no_init_session._session_loc)


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
def test_orcid_retrieval(mocker, no_init_session):
    assert fdp_conf.get_current_user_orcid(no_init_session._session_loc) == "000"


@pytest.mark.configuration
def test_glob_cfg_query(mocker, no_prompt, no_registry_autoinstall, subprocess_do_nothing, fake_token):
    mocker.patch.object(fdp_serv, 'launch_server', lambda *args, **kwargs: None)
    mocker.patch.object(os.path, 'exists', lambda x : True)
    _out_dict = fdp_conf.global_config_query()

    assert all(i in _out_dict for i in ["user", "remotes", "namespaces"])
    assert _out_dict["user"]["given_name"] == "Joe"
    assert _out_dict["user"]["family_name"] == "Bloggs"
    assert "local" in _out_dict["remotes"] and "origin" in _out_dict["remotes"]
    assert _out_dict["remotes"]["local"] == "http://localhost:8000/api/"
    assert _out_dict["remotes"]["origin"] == "http://noserver/api/"
    assert _out_dict["namespaces"]["output"] == "jbloggs"
    assert _out_dict["namespaces"]["input"] == "SCRC"


@pytest.mark.configuration
def test_local_cfg_query(mocker, no_init_session, no_prompt, no_registry_autoinstall, subprocess_do_nothing, fake_token):
    mocker.patch.object(fdp_serv, 'launch_server', lambda *args, **kwargs: None)
    mocker.patch.object(os.path, 'exists', lambda x : True)
    
    _out_dict = fdp_conf.local_config_query(no_init_session._global_config)
    
    assert all(i in _out_dict for i in ["user", "remotes", "namespaces"])
    assert _out_dict["user"]["given_name"] == "Joe"
    assert _out_dict["user"]["family_name"] == "Bloggs"
    assert "local" in _out_dict["remotes"] and "origin" in _out_dict["remotes"]
    assert _out_dict["remotes"]["local"] == "http://localhost:8000/api/"
    assert _out_dict["remotes"]["origin"] == "http://noserver/api"
    assert _out_dict["namespaces"]["input"] == "SCRC"