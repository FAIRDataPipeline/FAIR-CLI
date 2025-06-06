import os

import pytest
import pytest_mock

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req

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
def test_dependency_list(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        _reqs = fdp_req.get_dependency_listing(LOCAL_URL, local_registry._token)
        assert _reqs["data_product"] == ["object", "namespace"]


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
