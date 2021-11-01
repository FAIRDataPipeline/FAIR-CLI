import pytest
import datetime
import json

import fair.utilities as fdp_util


@pytest.mark.utilities
def test_flatten_dict():
    _input = {'X': {'Y': 'Z'}, 'A': 'B', 'C': {'D': {'E' : 'F'}}}
    _expect = {'X.Y': 'Z', 'A': 'B', 'C.D.E' : 'F'}
    assert fdp_util.flatten_dict(_input) == _expect


@pytest.mark.utilities
def test_expand_dict():
    _expect = {'X': {'Y': 'Z'}, 'A': 'B', 'C': {'D': {'E' : 'F'}}}
    _input = {'X.Y': 'Z', 'A': 'B', 'C.D.E' : 'F'}
    assert fdp_util.expand_dict(_input) == _expect


@pytest.mark.utilities
def test_remove_dictlist_dupes():
    _a = {'X': 'Y', 'A': 'B'}
    _b = {'X': 'B', 'A': 'Z'}
    _c = {'K': 'L', 'M': 'O'}
    _input = [_a, _a, _b, _c, _b, _c]
    _expect = [_a, _b, _c]
    assert fdp_util.remove_dictlist_dupes(_input) == _expect


@pytest.mark.utilities
def test_json_datetime_encoder():
    _input = {'A': datetime.datetime.strptime('10:04', '%H:%M')}
    _expect = {'A': '1900-01-01 10:04:00'}
    assert json.loads(fdp_util.JSONDateTimeEncoder().encode(_input)) == _expect
