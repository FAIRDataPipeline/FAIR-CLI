import pytest
import pytest_mock

import fair.exceptions as fdp_exc
import fair.identifiers as fdp_id
from . import conftest as conf
import warnings

GITHUB_USER = "FAIRDataPipeline"
ORCID_ID = "0000-0002-6773-1049"
ROR_ID = "049s0ch10"
GRID_ID = "grid.438622.9"
DOI_RESOLVER = "https://doi.org/"


@pytest.mark.faircli_ids
def test_check_orcid():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["orcid"]}{ORCID_ID}'):
        warnings.warn(f'Orcid API {fdp_id.QUERY_URLS["orcid"]} Unavailable')
        pytest.skip("Cannot Reach Orcid API")
    _data = fdp_id.check_orcid(ORCID_ID)
    assert _data["name"] == "Kristian Zarębski"
    assert _data["family_name"] == "Zarębski"
    assert _data["given_names"] == "Kristian"
    assert _data["orcid"] == ORCID_ID
    assert not fdp_id.check_orcid("notanid!")


@pytest.mark.faircli_ids
def test_check_generic_ror():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["ror"]}{ROR_ID}'):
        warnings.warn("ROR API Unavailable")
        pytest.skip("Cannot Reach ROR API")
    _data = fdp_id._check_generic_ror(ROR_ID)
    assert _data["name"] == "Rakon (France)" == _data["family_name"]
    assert "ror" not in _data
    assert "grid" not in _data
    assert not fdp_id.check_ror("notanid!")


@pytest.mark.faircli_ids
def test_check_ror():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["ror"]}{ROR_ID}'):
        warnings.warn("ROR API Unavailable")
        pytest.skip("Cannot Reach ROR API")
    _data = fdp_id.check_ror(ROR_ID)
    assert _data["name"] == "Rakon (France)" == _data["family_name"]
    assert _data["ror"] == ROR_ID
    assert _data["uri"] == "https://ror.org/049s0ch10"
    assert not fdp_id.check_ror("notanid!")


@pytest.mark.faircli_ids
def test_check_grid():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["ror"]}{ROR_ID}'):
        warnings.warn("ROR API Unavailable")
        pytest.skip("Cannot Reach ROR API")
    _data = fdp_id.check_grid(GRID_ID)
    assert _data["name"] == "Rakon (France)" == _data["family_name"]
    assert _data["grid"] == GRID_ID
    assert _data["uri"] == "https://ror.org/049s0ch10"
    assert not fdp_id.check_grid("notanid!")


@pytest.mark.faircli_ids
def test_check_github():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["github"]}{GITHUB_USER}'):
        warnings.warn("GitHub API Unavailable")
        pytest.skip("Cannot Reach GitHub API")
    _data = fdp_id.check_github("FAIRDataPipeline")
    assert _data["name"] == "FAIR Data Pipeline"
    assert _data["family_name"] == "Pipeline"
    assert _data["given_names"] == "FAIR Data"
    assert _data["github"] == GITHUB_USER
    assert _data["uri"] == f"https://github.com/{GITHUB_USER}"
    assert not fdp_id.check_github("notanid!")


@pytest.mark.faircli_ids
def test_check_permitted():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["orcid"]}{ORCID_ID}'):
        warnings.warn("Orcid API Unavailable")
        pytest.skip("Cannot Reach Orcid API")
    assert fdp_id.check_id_permitted("https://orcid.org/0000-0002-6773-1049")
    assert not fdp_id.check_id_permitted("notanid!")


@pytest.mark.faircli_ids
def test_check_permitted_doi():
    if not conf.test_can_be_run(DOI_RESOLVER):
        warnings.warn("DOI resolver Unavailable")
        pytest.skip("Cannot Reach DOI resolver")
    # This DOI redirects to a publisher that answers 403 to anything
    # automated; the redirect is what shows that the DOI resolved.
    assert fdp_id.check_id_permitted(f"{DOI_RESOLVER}10.1002/joc.5086")
    # A DOI the resolver does not know is still refused. retries=1 because
    # each further attempt sleeps three seconds, and nothing here is
    # expected to succeed on a second try.
    assert not fdp_id.check_id_permitted(
        f"{DOI_RESOLVER}10.1002/not.a.real.doi", retries=1
    )


@pytest.mark.faircli_ids
def test_check_permitted_redirect_to_missing():
    _url = f"http://github.com/{GITHUB_USER}/not-a-real-repo"
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["github"]}{GITHUB_USER}'):
        warnings.warn("GitHub Unavailable")
        pytest.skip("Cannot Reach GitHub")
    # A redirect is not enough on its own: this one (http -> https) leads to
    # a 404, so the URL does not resolve.
    assert not fdp_id.check_id_permitted(_url, retries=1)


class _GitHubResponse:
    """Stand-in for a GitHub API response"""

    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return {"login": GITHUB_USER, "name": "FAIR Data Pipeline"}


def _fake_github(mocker, *responses):
    """Patch the GitHub request to answer with each response in turn"""
    mocker.patch.object(fdp_id.time, "sleep")
    return mocker.patch.object(fdp_id.requests, "get", side_effect=list(responses))


def _sent_token(call):
    return call.kwargs["headers"].get("Authorization")


@pytest.mark.faircli_ids
@pytest.mark.parametrize("variable", ["GITHUB_TOKEN", "GITHUB_PAT"])
def test_check_github_sends_token(mocker: pytest_mock.MockerFixture, variable):
    mocker.patch.dict(fdp_id.os.environ, {variable: "abc"}, clear=True)
    _get = _fake_github(mocker, _GitHubResponse(200))
    assert fdp_id.check_github(GITHUB_USER)["github"] == GITHUB_USER
    assert _sent_token(_get.call_args) == "Bearer abc"


@pytest.mark.faircli_ids
def test_check_github_without_token(mocker: pytest_mock.MockerFixture):
    mocker.patch.dict(fdp_id.os.environ, {}, clear=True)
    _get = _fake_github(mocker, _GitHubResponse(200))
    assert fdp_id.check_github(GITHUB_USER)
    assert _sent_token(_get.call_args) is None


@pytest.mark.faircli_ids
def test_check_github_expired_token(mocker: pytest_mock.MockerFixture):
    # A refused token is dropped, for the retry after a 403 as well
    mocker.patch.dict(fdp_id.os.environ, {"GITHUB_TOKEN": "expired"}, clear=True)
    mocker.patch.object(fdp_id, "UserAgent")
    _get = _fake_github(
        mocker, _GitHubResponse(401), _GitHubResponse(403), _GitHubResponse(200)
    )
    assert fdp_id.check_github(GITHUB_USER)["github"] == GITHUB_USER
    assert [_sent_token(c) for c in _get.call_args_list] == [
        "Bearer expired",
        None,
        None,
    ]


@pytest.mark.faircli_ids
@pytest.mark.parametrize("status", [403, 429])
@pytest.mark.parametrize("token", [None, "abc"])
def test_check_github_rate_limited(mocker: pytest_mock.MockerFixture, status, token):
    mocker.patch.dict(
        fdp_id.os.environ, {"GITHUB_TOKEN": token} if token else {}, clear=True
    )
    _fake_github(
        mocker,
        _GitHubResponse(
            status, {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1700000000"}
        ),
    )
    with pytest.raises(fdp_exc.FAIRCLIException) as _error:
        fdp_id.check_github(GITHUB_USER)
    assert "rate limit" in _error.value.msg
    assert ("GITHUB_TOKEN" in _error.value.hint) == (token is None)


@pytest.mark.faircli_ids
def test_check_github_forbidden_not_rate_limited(mocker: pytest_mock.MockerFixture):
    # A 403 with requests to spare is not a rate limit: retried, then not found
    mocker.patch.dict(fdp_id.os.environ, {}, clear=True)
    mocker.patch.object(fdp_id, "UserAgent")
    _forbidden = _GitHubResponse(403, {"X-RateLimit-Remaining": "42"})
    _get = _fake_github(mocker, _forbidden, _forbidden)
    assert fdp_id.check_github(GITHUB_USER) == {}
    assert _get.call_count == 2


@pytest.mark.faircli_ids
@pytest.mark.parametrize("known", [True, False])
def test_check_permitted_ror(mocker: pytest_mock.MockerFixture, known):
    # A ROR identifier is looked up through ROR's API, never its website,
    # which refuses automated requests that come too often
    mocker.patch.object(
        fdp_id.requests,
        "get",
        side_effect=AssertionError("ror.org was fetched"),
    )
    _check_ror = mocker.patch.object(
        fdp_id, "check_ror", return_value={"name": "A body"} if known else {}
    )
    assert fdp_id.check_id_permitted(f"https://ror.org/{ROR_ID}") is known
    _check_ror.assert_called_once_with(ROR_ID)


@pytest.mark.faircli_ids
def test_check_permitted_ror_through_the_api():
    if not conf.test_can_be_run(f'{fdp_id.QUERY_URLS["ror"]}{ROR_ID}'):
        warnings.warn("ROR API Unavailable")
        pytest.skip("Cannot Reach ROR API")
    assert fdp_id.check_id_permitted(f"https://ror.org/{ROR_ID}")
    assert not fdp_id.check_id_permitted("https://ror.org/0000notaror")


@pytest.mark.faircli_ids
def test_check_github_gives_a_public_email(mocker: pytest_mock.MockerFixture):
    mocker.patch.dict(fdp_id.os.environ, {}, clear=True)
    _response = _GitHubResponse(200)
    _response.json = lambda: {
        "login": GITHUB_USER,
        "name": "FAIR Data Pipeline",
        "email": "pipeline@example.org",
    }
    _fake_github(mocker, _response)
    assert fdp_id.check_github(GITHUB_USER)["email"] == "pipeline@example.org"
    # None where its holder made none public
    _fake_github(mocker, _GitHubResponse(200))
    assert fdp_id.check_github(GITHUB_USER)["email"] is None


@pytest.mark.faircli_ids
@pytest.mark.parametrize(
    "status, found, expected",
    [
        (
            200,
            {"expanded-result": [{"email": ["a@example.org", "b@example.org"]}]},
            "a@example.org",
        ),
        (200, {"expanded-result": [{"email": []}]}, None),
        (200, {"expanded-result": None}, None),
        (500, {}, None),
    ],
    ids=["public", "none public", "no such record", "refused"],
)
def test_check_orcid_email(
    mocker: pytest_mock.MockerFixture, status, found, expected
):
    _get = mocker.patch.object(
        fdp_id.requests,
        "get",
        return_value=mocker.Mock(status_code=status, json=lambda: found),
    )
    assert fdp_id.check_orcid_email(f"https://orcid.org/{ORCID_ID}") == expected
    assert _get.call_args.kwargs["params"] == {"q": f"orcid:{ORCID_ID}"}


@pytest.mark.faircli_ids
def test_check_orcid_email_unreachable(mocker: pytest_mock.MockerFixture):
    mocker.patch.object(
        fdp_id.requests,
        "get",
        side_effect=fdp_id.requests.exceptions.ConnectionError("down"),
    )
    assert fdp_id.check_orcid_email(ORCID_ID) is None
