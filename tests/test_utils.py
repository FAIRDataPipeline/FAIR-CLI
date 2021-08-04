import pytest

import fair.utilities as fdp_util

@pytest.mark.utilities
def test_unique_dict_diff_keys():
    _dict_in = [
        {'a': 1, 'b': 2},
        {'a': 1, 'b': 2},
        {'c': 3, 'b': 2},
        {'a': 1, 'b': 2, 'c': 3}
    ]
    _dict_out = [
        {'a': 1, 'b': 2},
        {'c': 3, 'b': 2},
        {'a': 1, 'b': 2, 'c': 3}
    ]
    assert fdp_util.remove_dictlist_dupes(_dict_in) == _dict_out


@pytest.mark.utilities
def test_unique_dict_same_keys():
    _dict_in = [
        {'name': 'Steve'},
        {'name': 'Tim'},
        {'name': 'Steve'},
        {'name': 'Tim'},
        {'name': 'Jack'}
    ]
    _dict_out = [
        {'name': 'Steve'},
        {'name': 'Tim'},
        {'name': 'Jack'},
    ]
    assert fdp_util.remove_dictlist_dupes(_dict_in) == _dict_out


@pytest.mark.utilities
def test_dict_expand():
    _in_dict = {'a.b.c': 'd', 'b.k': 'f'}
    _out_dict = {'a': {'b': {'c': 'd'}}, 'b': {'k': 'f'}}

    assert fdp_util.expand_dict(_in_dict) == _out_dict


@pytest.mark.utilities
def test_dict_flatten():
    _out_dict = {'a.b.c': 'd', 'b.k': 'f'}
    _in_dict = {'a': {'b': {'c': 'd'}}, 'b': {'k': 'f'}}

    assert fdp_util.flatten_dict(_in_dict) == _out_dict