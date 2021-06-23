import pytest

import fair.common as fdp_com


@pytest.mark.common
def test_fair_root(set_constant_paths, initialise_test_repo):
    assert fdp_com.find_fair_root(initialise_test_repo) == initialise_test_repo
