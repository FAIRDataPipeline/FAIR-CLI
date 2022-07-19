import pytest

import fair.identifiers as fdp_id


@pytest.mark.faircli_ids
def test_check_orcid():
    _data = fdp_id.check_orcid("0000-0002-6773-1049")
    assert _data["name"] == "Kristian Zarębski"
    assert _data["family_name"] == "Zarębski"
    assert _data["given_names"] == "Kristian"
    assert _data["orcid"] == "0000-0002-6773-1049"
    assert not fdp_id.check_orcid("notanid!")


@pytest.mark.faircli_ids
def test_check_ror():
    _data = fdp_id.check_ror("049s0ch10")
    assert _data["name"] == "Rakon (France)" == _data["family_name"]
    assert _data["ror"] == "049s0ch10"
    assert not fdp_id.check_ror("notanid!")

@pytest.mark.faircli_ids
def test_check_grid():
    _data = fdp_id.check_grid("grid.438622.9")
    assert _data["name"] == "Rakon (France)" == _data["family_name"]
    assert _data["grid"] == "grid.438622.9"
    assert not fdp_id.check_grid("notanid!")


@pytest.mark.faircli_ids
def test_check_permitted():
    assert fdp_id.check_id_permitted("https://orcid.org/0000-0002-6773-1049")
    assert not fdp_id.check_id_permitted("notanid!")
