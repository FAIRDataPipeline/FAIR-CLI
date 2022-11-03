import datetime
import json

import pytest

import fair.utilities as fdp_util


@pytest.mark.faircli_utilities
def test_flatten_dict():
    _input = {"X": {"Y": "Z"}, "A": "B", "C": {"D": {"E": "F"}}}
    _expect = {"X.Y": "Z", "A": "B", "C.D.E": "F"}
    assert fdp_util.flatten_dict(_input) == _expect


@pytest.mark.faircli_utilities
def test_expand_dict():
    _expect = {"X": {"Y": "Z"}, "A": "B", "C": {"D": {"E": "F"}}}
    _input = {"X.Y": "Z", "A": "B", "C.D.E": "F"}
    assert fdp_util.expand_dict(_input) == _expect


@pytest.mark.faircli_utilities
def test_remove_dictlist_dupes():
    _a = {"X": "Y", "A": "B"}
    _b = {"X": "B", "A": "Z"}
    _c = {"K": "L", "M": "O"}
    _input = [_a, _a, _b, _c, _b, _c]
    _expect = [_a, _b, _c]
    assert fdp_util.remove_dictlist_dupes(_input) == _expect


@pytest.mark.faircli_utilities
def test_json_datetime_encoder():
    _input = {"A": datetime.datetime.strptime("10:04", "%H:%M")}
    _expect = {"A": "1900-01-01 10:04:00"}
    assert json.loads(fdp_util.JSONDateTimeEncoder().encode(_input)) == _expect


@pytest.mark.faircli_utilities
@pytest.mark.parametrize(
    "test_input,expected", [("lallero", "lallero/"), ("lallero/", "lallero/")]
)
def test_trailing_slash(test_input, expected):
    result = fdp_util.check_trailing_slash(test_input)
    assert result == expected

@pytest.mark.faircli_utilities
@pytest.mark.parametrize(
    "test_input,expected", [("lallero/", "lallero"), ("lallero", "lallero")]
)
def test_remove_trailing_slash(test_input, expected):
    result = fdp_util.remove_trailing_slash(test_input)
    assert result == expected

@pytest.mark.faircli_utilities
def test_api_url_check():
    _test_url = "http://127.0.0.1:8000/api/test-url"
    _not_url = "notaurl"
    _wrong_endpoint = "http://127.0.0.1:8001/api/"
    _test_endpoint = "http://127.0.0.1:8000"
    assert fdp_util.is_api_url(_test_endpoint, _test_url)
    assert not fdp_util.is_api_url(_test_endpoint, _not_url)
    assert not fdp_util.is_api_url(_test_endpoint, _wrong_endpoint)
