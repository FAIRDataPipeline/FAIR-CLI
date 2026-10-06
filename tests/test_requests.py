import json
import os
import tempfile

import pytest
import pytest_mock
import requests

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.sync as fdp_sync

from . import conftest as conf

LOCAL_URL = "http://127.0.0.1:8000/api"


@pytest.mark.faircli_requests
def test_split_url():
    _test_url = "https://not_a_site.com/api/object?something=other"
    assert fdp_req.split_api_url(_test_url) == (
        "https://not_a_site.com/api",
        "object?something=other",
    )
    assert fdp_req.split_api_url(_test_url, "com") == (
        "https://not_a_site.com",
        "api/object?something=other",
    )


@pytest.mark.faircli_requests
def test_local_token(mocker: pytest_mock.MockerFixture, tmp_path):
    _dummy_key = "sdfd234ersdf45234"
    tempd = tmp_path.__str__()
    _token_file = os.path.join(tempd, "token")
    mocker.patch("fair.common.registry_home", lambda: tempd)
    with pytest.raises(fdp_exc.FileNotFoundError):
        fdp_req.local_token()
    open(_token_file, "w").write(_dummy_key)
    assert fdp_req.local_token() == _dummy_key


@pytest.mark.faircli_requests
def test_request_error_registy_not_running():
    with pytest.raises(Exception) as e_info:
        fdp_req._access(LOCAL_URL)
        assert e_info.match(r"^Failed to make registry API request.*")


@pytest.mark.faircli_requests
def test_post_keeps_false_values(mocker: pytest_mock.MockerFixture):
    # False and 0 are values. Only what is empty is left out of a post, for
    # the registry to fill with its default
    _access = mocker.patch("fair.registry.requests._access")
    fdp_req.post(
        LOCAL_URL,
        "storage_location",
        "",
        data={
            "path": "a/b.csv",
            "public": False,
            "severity": 0,
            "description": "",
            "website": None,
            "authors": [],
        },
    )
    assert json.loads(_access.call_args.kwargs["data"]) == {
        "path": "a/b.csv",
        "public": False,
        "severity": 0,
    }


@pytest.mark.faircli_requests
def test_get_follows_pages(mocker: pytest_mock.MockerFixture):
    # A registry lists a page at a time, giving with each the address of the
    # next - here an address it could not be reached at
    _elsewhere = "http://registry.internal/api/data_product/?name=a%2F%2A"
    _pages = {
        None: {"next": f"{_elsewhere}&cursor=p2", "results": [5, 4]},
        "p2": {"next": f"{_elsewhere}&cursor=p3", "results": [3, 2]},
        "p3": {"next": None, "results": [1]},
    }
    _requests = []

    def dummy_get(url, headers=None, params=None):
        _requests.append((url, dict(params)))
        _response = mocker.Mock(status_code=200)
        _response.json.return_value = _pages[params.get("cursor")]
        return _response

    mocker.patch("requests.get", dummy_get)

    assert fdp_req.get(
        LOCAL_URL, "data_product", "", params={"name": "a/*"}
    ) == [
        5,
        4,
        3,
        2,
        1,
    ]
    # Every page is asked of the registry where it was reached, with the
    # search as it was and the cursor the registry gave
    assert _requests == [
        (f"{LOCAL_URL}/data_product/", {"name": "a/*"}),
        (f"{LOCAL_URL}/data_product/", {"name": "a/*", "cursor": "p2"}),
        (f"{LOCAL_URL}/data_product/", {"name": "a/*", "cursor": "p3"}),
    ]


@pytest.mark.faircli_requests
def test_get_fails_on_a_broken_page(mocker: pytest_mock.MockerFixture):
    # Part of a list must not pass for the whole of it
    _pages = {
        None: {"next": f"{LOCAL_URL}/data_product/?cursor=p2", "results": [2]},
        "p2": {"detail": "Invalid cursor"},
    }

    def dummy_get(url, headers=None, params=None):
        _response = mocker.Mock(
            status_code=404 if params.get("cursor") else 200
        )
        _response.json.return_value = _pages[params.get("cursor")]
        return _response

    mocker.patch("requests.get", dummy_get)

    with pytest.raises(fdp_exc.RegistryAPICallError) as _error:
        fdp_req.get(LOCAL_URL, "data_product", "")
    assert _error.value.error_code == 404


@pytest.mark.faircli_requests
@pytest.mark.dependency(name="post")
def test_post(local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    _name = "Joseph Bloggs"
    _orcid = "https://orcid.org/0000-0000-0000-0000"
    with local_registry:
        _result = fdp_req.post(
            LOCAL_URL,
            "author",
            local_registry._token,
            data={"name": _name, "identifier": _orcid},
        )
        assert _result["url"]


@pytest.mark.faircli_requests
@pytest.mark.dependency(name="get_author_exists", depends=["post"])
def test_get_author_exists(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    _name = "Joseph Bloggs"
    _orcid = "https://orcid.org/0000-0000-0000-0000"
    with local_registry:
        _author_exists = fdp_req.get_author_exists(
            LOCAL_URL, local_registry._token, name=_name
        )
        assert _author_exists
        _author_exists = fdp_req.get_author_exists(
            LOCAL_URL, local_registry._token, identifier=_orcid
        )
        assert _author_exists
        _author_exists = fdp_req.get_author_exists(
            LOCAL_URL, local_registry._token, name=_name, identifier=_orcid
        )
        assert _author_exists
        _author_does_not_exists = fdp_req.get_author_exists(
            LOCAL_URL, local_registry._token
        )
        assert not _author_does_not_exists
        _author_does_not_exists = fdp_req.get_author_exists(
            LOCAL_URL, local_registry._token, identifier=_orcid, name="Incorrect Nname"
        )
        assert not _author_does_not_exists
        _author_does_not_exists = fdp_req.get_author_exists(
            LOCAL_URL,
            local_registry._token,
            identifier="https://github.com/FAIRDataPipeline",
            name=_name,
        )
        assert not _author_does_not_exists


@pytest.mark.faircli_requests
@pytest.mark.dependency(name="get", depends=["post"])
def test_get(local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert fdp_req.get(LOCAL_URL, "author", local_registry._token)


@pytest.mark.faircli_requests
@pytest.mark.dependency(depends=["get"])
def test_get_404(local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        with pytest.raises(Exception) as e_info:
            fdp_req.get(LOCAL_URL, "nothing_here", local_registry._token)
            assert e_info.match(
                r"^Attempt to access an unrecognised resource on registry.*"
            )


@pytest.mark.faircli_requests
@pytest.mark.dependency(depends=["get"])
def test_registry_403(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        with pytest.raises(Exception) as e_info:
            fdp_req._access(
                LOCAL_URL,
                method="patch",
                token=local_registry._token,
                obj_path="user",
                data={"name": "forbidden"},
            )
            assert e_info.match(r"^Failed to run method.*")


@pytest.mark.faircli_requests
@pytest.mark.dependency(depends=["get"])
def test_get_incorrect_responce_code(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        with pytest.raises(Exception) as e_info:
            fdp_req.get(LOCAL_URL, "nothing_here", local_registry._token)
            assert e_info.match(
                r"^Attempt to access an unrecognised resource on registry.*"
            )


@pytest.mark.faircli_requests
def test_post_else_get(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:

        _data = {"name": "Comma Separated Values", "extension": "csv"}
        _params = {"extension": "csv"}
        _obj_path = "file_type"

        mock_post = mocker.patch("fair.registry.requests.post")
        mock_get = mocker.patch("fair.registry.requests.get")
        # Perform method twice, first should post, second retrieve
        assert fdp_req.post_else_get(
            LOCAL_URL,
            _obj_path,
            local_registry._token,
            data=_data,
            params=_params,
        )

        mock_post.assert_called_once()
        mock_get.assert_not_called()

        mocker.resetall()

        def raise_it(*kwargs, **args):
            raise fdp_exc.RegistryAPICallError("woops", error_code=409)

        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        mocker.patch("fair.registry.requests.post", raise_it)
        mock_get = mocker.patch("fair.registry.requests.get")

        assert fdp_req.post_else_get(
            LOCAL_URL,
            "file_type",
            local_registry._token,
            data={"name": "Comma Separated Values", "extension": "csv"},
            params={"extension": "csv"},
        )

        mock_get.assert_called_once()


@pytest.mark.faircli_requests
def test_filter_variables(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert fdp_req.get_filter_variables(
            LOCAL_URL, "data_product", local_registry._token
        )


@pytest.mark.faircli_requests
def test_writable_fields(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        fdp_req.filter_object_dependencies(
            LOCAL_URL,
            "data_product",
            local_registry._token,
            {"read_only": True},
        )


@pytest.mark.faircli_requests
def test_download(local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        _example_file = "https://data.fairdatapipeline.org/static/localregistry.sh"
        _out_file = fdp_req.download_file(_example_file)
        assert os.path.exists(_out_file)


@pytest.mark.faircli_requests
def test_download_http_error(file_server: str):
    _out_file = fdp_req.download_file(f"{file_server}data.csv")
    with open(_out_file) as out_f:
        assert out_f.read() == "a,b\n1,2\n"

    with pytest.raises(requests.HTTPError):
        fdp_req.download_file(f"{file_server}missing.csv")
    with pytest.raises(fdp_exc.UserConfigError, match="status code 404"):
        fdp_sync.download_from_registry("", file_server, "missing.csv")


@pytest.mark.faircli_requests
def test_download_is_streamed(mocker: pytest_mock.MockerFixture):
    # A file is written as it arrives; the whole of it is never held
    _response = mocker.Mock(status_code=200)
    _response.iter_content.return_value = iter([b"a,b\n", b"1,2\n"])
    type(_response).content = mocker.PropertyMock(
        side_effect=AssertionError("the whole body was asked for")
    )
    _get = mocker.patch("requests.get", return_value=_response)

    _out_file = fdp_req.download_file("http://example.org/data.csv")

    with open(_out_file, "rb") as out_f:
        assert out_f.read() == b"a,b\n1,2\n"
    assert _get.call_args.kwargs["stream"] is True
    os.remove(_out_file)


@pytest.mark.faircli_requests
def test_failed_download_leaves_no_file(
    file_server: str, mocker: pytest_mock.MockerFixture, tmp_path
):
    _temp_dir = tmp_path / "temp"
    _temp_dir.mkdir()
    mocker.patch.object(tempfile, "tempdir", str(_temp_dir))

    with pytest.raises(requests.HTTPError):
        fdp_req.download_file(f"{file_server}missing.csv")
    assert not os.listdir(_temp_dir)

    # A file that stops arriving part of the way through
    _response = mocker.Mock(status_code=200)
    _response.iter_content.side_effect = (
        requests.exceptions.ChunkedEncodingError
    )
    mocker.patch("requests.get", return_value=_response)
    with pytest.raises(
        fdp_exc.FAIRCLIException, match="Failed to download all"
    ):
        fdp_req.download_file("http://example.org/data.csv")
    assert not os.listdir(_temp_dir)


@pytest.mark.faircli_requests
@pytest.mark.parametrize("content", [b"a,b\n1,2\n", b""])
def test_put_file_sends_the_open_file(
    mocker: pytest_mock.MockerFixture, tmp_path, content: bytes
):
    # Sent from the open file, not read into memory first; an empty file is
    # sent as no bytes, so that its length is stated
    _file = tmp_path / "data.csv"
    _file.write_bytes(content)
    _sent = {}

    def dummy_put(session, url, data=None, **kwargs):
        _sent["from_file"] = hasattr(data, "read")
        _sent["data"] = data.read() if _sent["from_file"] else data
        return mocker.Mock(status_code=200)

    mocker.patch("requests.Session.put", dummy_put)

    assert fdp_req.put_file("http://example.org/upload", str(_file))
    assert _sent == {"from_file": bool(content), "data": content}


@pytest.mark.faircli_requests
def test_dependency_list(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        _reqs = fdp_req.get_dependency_listing(LOCAL_URL, local_registry._token)
        # What a data product must depend on; a registry may list more
        assert {"object", "namespace"} <= set(_reqs["data_product"])


@pytest.mark.faircli_requests
def test_object_type_fetch(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        for obj in ["object", "data_product", "author", "file_type"]:
            assert (
                fdp_req.get_obj_type_from_url(
                    f"{LOCAL_URL}/{obj}", local_registry._token
                )
                == obj
            )
