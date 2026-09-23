import pytest

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
