import os
import typing
import tempfile

import deepdiff
import pytest
import pytest_mock

import fair.configuration as fdp_conf
import fair.identifiers as fdp_id
import fair.common as fdp_com


@pytest.mark.configuration
def test_local_cli_config_read(local_config: typing.Tuple[str, str]):
    _read = fdp_conf.read_local_fdpconfig(local_config[1])
    assert _read["git"]["local_repo"] == os.path.join(
        local_config[1], "project"
    )
    assert _read["namespaces"]["input"] == "testing"


@pytest.mark.configuration
def test_global_cli_config_read(local_config: typing.Tuple[str, str]):
    _read = fdp_conf.read_global_fdpconfig()
    assert _read["git"]["local_repo"] == os.path.join(
        local_config[0], "project"
    )
    assert _read["namespaces"]["input"] == "testing"


@pytest.mark.configuration
def test_email_set(local_config: typing.Tuple[str, str]):
    TEST_EMAIL = "testemail@nowhere"
    TEST_EMAIL2 = "otheremail@nowhere"
    fdp_conf.set_email(local_config[1], TEST_EMAIL)
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["email"]
        == TEST_EMAIL
    )
    assert fdp_conf.read_global_fdpconfig()["user"]["email"] != TEST_EMAIL
    fdp_conf.set_email(local_config[1], TEST_EMAIL2, True)
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["email"]
        == TEST_EMAIL2
    )
    assert fdp_conf.read_global_fdpconfig()["user"]["email"] == TEST_EMAIL2


@pytest.mark.configuration
def test_user_set(local_config: typing.Tuple[str, str]):
    TEST_USER = "john smith"
    TEST_USER2 = "victor Chester bloggs"
    fdp_conf.set_user(local_config[1], TEST_USER)
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["given_names"]
        == "John"
    )
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["family_name"]
        == "Smith"
    )
    assert fdp_conf.read_global_fdpconfig()["user"]["given_names"] != "John"
    assert fdp_conf.read_global_fdpconfig()["user"]["family_name"] != "Smith"
    fdp_conf.set_user(local_config[1], TEST_USER2, True)
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["given_names"]
        == "Victor Chester"
    )
    assert (
        fdp_conf.read_local_fdpconfig(local_config[1])["user"]["family_name"]
        == "Bloggs"
    )
    assert (
        fdp_conf.read_global_fdpconfig()["user"]["given_names"]
        == "Victor Chester"
    )
    assert fdp_conf.read_global_fdpconfig()["user"]["family_name"] == "Bloggs"


@pytest.mark.configuration
def test_get_user(local_config: typing.Tuple[str, str]):
    assert fdp_conf.get_current_user_name(local_config[1]) == (
        "Interface",
        "Test",
    )


@pytest.mark.configuration
def test_get_remote_uri(local_config: typing.Tuple[str, str]):
    assert (
        fdp_conf.get_remote_uri(local_config[1])
        == "http://127.0.0.1:8001/api/"
    )


@pytest.mark.configuration
def test_get_remote_token(mocker: pytest_mock.MockerFixture):
    with tempfile.TemporaryDirectory() as tempd:
        _token = "t35tt0k3n"
        _token_file = os.path.join(tempd, "token")
        open(_token_file, "w").write(_token)
        mocker.patch(
            "fair.configuration.read_local_fdpconfig",
            lambda *args: {"registries": {"origin": {"token": _token_file}}}
        )
        assert fdp_conf.get_remote_token("") == _token


@pytest.mark.configuration
def test_get_git_remote(local_config: typing.Tuple[str, str]):
    _proj_dir = os.path.join(local_config[0], "project")
    assert fdp_conf.get_session_git_remote(_proj_dir) == "origin"
    assert (
        fdp_conf.get_session_git_remote(_proj_dir, True)
        == "git@notagit.com/nope"
    )


@pytest.mark.configuration
def test_get_orcid(local_config: typing.Tuple[str, str]):
    assert (
        fdp_conf.get_current_user_uri(local_config[0])
        == f'{fdp_id.ID_URIS["orcid"]}000-0000-0000-0000'
    )


@pytest.mark.configuration
def test_get_uuid(local_config: typing.Tuple[str, str]):
    assert (
        fdp_conf.get_current_user_uuid(local_config[0])
        == "2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1"
    )


@pytest.mark.configuration
def test_registry_exists(
    mocker: pytest_mock.MockerFixture, local_config: typing.Tuple[str, str]
):
    mocker.patch("fair.common.DEFAULT_REGISTRY_LOCATION", local_config[0])
    assert fdp_conf.check_registry_exists()
    assert fdp_conf.check_registry_exists(local_config[0])


@pytest.mark.configuration
def test_local_uri(local_config: typing.Tuple[str, str]):
    assert fdp_conf.get_local_uri() == "http://127.0.0.1:8000/api/"


@pytest.mark.configuration
def test_local_port(local_config: typing.Tuple[str, str]):
    assert fdp_conf.get_local_port() == 8000


@pytest.mark.configuration
def test_user_info(mocker: pytest_mock.MockerFixture):
    _namepaces = {"input": "ispace", "output": "jbloggs"}
    _override = {
        "Email": "jbloggs@nowhere.com",
        "Full Name": "Joseph Bloggs",
        "Default input namespace": _namepaces["input"],
        "Default output namespace": _namepaces["output"],
        "User ID system (ORCID/ROR/GRID/None)": "None",
    }

    _orcid_override = {
        "family_name": "Bloggs",
        "given_names": "Joseph",
        "uuid": None,
        "email": _override["Email"],
    }
    _uuid_override = _orcid_override.copy()
    _uuid_override["uuid"] = "f45sasd832j234gjk"

    mocker.patch(
        "click.prompt", lambda x, default=None: _override[x] or default
    )
    mocker.patch("uuid.uuid4", lambda: _uuid_override["uuid"])
    _noorc = fdp_conf._get_user_info_and_namespaces()

    _override["User ID system (ORCID/ROR/GRID/None)"] = "ORCID"
    _override["ORCID"] = "0000-0000-0000"

    mocker.patch(
        "click.prompt", lambda x, default=None: _override[x] or default
    )
    mocker.patch("fair.identifiers.check_orcid", lambda *args: _orcid_override)
    _orc = fdp_conf._get_user_info_and_namespaces()

    _expect_noorc = {"user": _uuid_override, "namespaces": _namepaces}

    _expect_orcid = {"user": _orcid_override, "namespaces": _namepaces}

    assert not deepdiff.DeepDiff(_noorc, _expect_noorc)
    assert not deepdiff.DeepDiff(_orc, _expect_orcid)


@pytest.mark.configuration
def test_global_config_query(
    mocker: pytest_mock.MockerFixture, local_config: typing.Tuple[str, str]
):
    _override = {
        "Remote Data Storage Root": "",
        "Remote API Token File": os.path.join(local_config[0], "token.txt"),
        "Default Data Store": "data_store/",
        "Local Registry Port": "8001",
        "Remote API URL": "http://127.0.0.1:8007/api/",
    }
    _default_user = {
        "family_name": "Bloggs",
        "given_names": "Joseph",
        "orcid": "0000-0000-0000-0000",
        "uuid": None,
        "email": "jbloggs@nowhere.com",
    }
    mocker.patch(
        "fair.registry.server.launch_server", lambda *args, **kwargs: None
    )
    mocker.patch(
        "fair.registry.server.stop_server", lambda *args, **kwargs: None
    )
    mocker.patch(
        "fair.registry.requests.local_token",
        lambda *args, **kwargs: "92342343243224",
    )
    mocker.patch(
        "click.prompt", lambda x, default=None: _override[x] or default
    )
    mocker.patch("click.confirm", lambda *args, **kwargs: False)
    mocker.patch(
        "fair.configuration._get_user_info_and_namespaces",
        lambda: _default_user,
    )

    _expected = {
        "registries": {
            "local": {
                "uri": "http://127.0.0.1:8001/api/",
                "directory": local_config[0],
                "data_store": _override["Default Data Store"],
                "token": os.path.join(local_config[0], "token")
            },
            "origin": {
                "uri": _override["Remote API URL"],
                "token": _override["Remote API Token File"],
                "data_store": _override["Remote API URL"].replace(
                    "api", "data"
                ),
            },
        }
    }
    _expected.update(_default_user)

    assert not deepdiff.DeepDiff(
        _expected, fdp_conf.global_config_query(local_config[0])
    )


@pytest.mark.configuration
def test_local_config_query(
    local_config: typing.Tuple[str, str], mocker: pytest_mock.MockerFixture
):

    # First check that the global setup is called when a global
    # configuration does not exist
    mock_gc = mocker.patch("fair.configuration.global_config_query")
    mocker.patch("click.prompt", lambda x, default=None: None)
    try:
        fdp_conf.local_config_query({})
    except AssertionError:
        mock_gc.assert_called_with()

    _glob_conf = {
        "user": {
            "family_name": "Bloggs",
            "given_names": "Joseph",
            "orcid": "0000-0000-0000-0000",
            "uuid": None,
            "email": "jbloggs@nowhere.com",
        },
        "registries": {
            "local": {
                "uri": "http://127.0.0.1:8001/api/",
                "directory": local_config[0],
                "data_store": "data_store/",
            },
            "origin": {
                "uri": "http://127.0.0.1:8007/api/",
                "token": os.path.join(local_config[0], "token.txt"),
                "data_store": "http://127.0.0.1:8007/data/",
            },
        },
        "namespaces": {"input": "ispace", "output": "jbloggs"},
    }

    mocker.patch("fair.configuration.global_config_query", lambda: _glob_conf)

    _override = {
        "Local Git repository": os.path.join(local_config[0], "project"),
        "Git remote name": "origin",
        "Remote API URL": "",
        "Remote API Token File": "",
        "Default output namespace": "",
        "Default input namespace": "",
    }

    mocker.patch(
        "click.prompt", lambda x, default=None: _override[x] or default
    )

    _usr_config = fdp_conf.local_config_query(_glob_conf)

    _glob_conf["git"] = {
        "local_repo": os.path.join(local_config[0], "project"),
        "remote": "origin",
        "remote_repo": "git@notagit.com/nope",
    }
    del _glob_conf["registries"]["local"]

    assert not deepdiff.DeepDiff(_glob_conf, _usr_config)

@pytest.mark.configuration
def test_update_port(local_config: typing.Tuple[str, str]):
    assert fdp_conf.get_local_uri() == fdp_com.DEFAULT_LOCAL_REGISTRY_URL
    fdp_conf.update_local_port()
    _new_url = fdp_com.DEFAULT_LOCAL_REGISTRY_URL.replace("8000", "8001")
    assert fdp_conf.get_local_uri() == _new_url
