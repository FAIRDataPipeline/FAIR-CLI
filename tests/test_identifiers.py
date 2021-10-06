import pytest

import fair.identifiers as fair_id


@pytest.mark.ids
def test_check_orcid():
    assert fair_id.check_orcid('0000-0002-6773-1049')
