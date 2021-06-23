import pytest

from fair.utilities import expand_dict, flatten_dict


@pytest.mark.utilities
def test_expand_dict():
    _test_dict = {"X.Y.Z": 10, "X.Y.U": 11, "X.N": 12, "Z.U": 13, "Z.V.W": 14}
    _expect = {
        "X": {"Y": {"Z": 10, "U": 11}, "N": 12},
        "Z": {"U": 13, "V": {"W": 14}},
    }
    assert expand_dict(_test_dict) == _expect


@pytest.mark.utilities
def test_flatten_dict():
    _test_dict = {
        "X": {"Y": {"Z": 10, "W": 11}, "U": 12},
        "Z": {"W": 13, "V": {"N": 14}},
    }
    _expect = {"X.Y.Z": 10, "X.Y.W": 11, "X.U": 12, "Z.W": 13, "Z.V.N": 14}
    assert flatten_dict(_test_dict) == _expect
