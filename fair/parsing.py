import typing

import fair.registry.requests as fdp_reg_req
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc

def glob_read_write(local_repo: str, config_dict_sub: typing.List, search_key: str = 'name') -> typing.List:
    """Substitute the 'read' or 'write' part of a user config with glob expressions
    
    Parameters
    ----------
    config_dict_sub : List[Dict]
        entries to read/write from registry
    """
    _parsed: typing.List[typing.Dict] = []
    for entry in config_dict_sub:
        _glob_vals = [(k, v) for k, v in entry.items() if '*' in v]
        if len(_glob_vals) > 1:
            raise fdp_exc.NotImplementedError(
                "Only one key-value pair in a 'read' list entry may contain a"
                " globbable value"
            )
        elif len(_glob_vals) == 0:
            _parsed.append(entry)
            continue

        _key_glob, _globbable = _glob_vals[0]

        _results = fdp_reg_req.get(
            fdp_conf.get_local_uri(local_repo),
            (_key_glob,),
            params = {search_key: _globbable}
        )

        for result in _results:
            _entry_dict = entry.copy()
            _entry_dict[_key_glob] = result[search_key]
            _parsed.append(_entry_dict)
    return remove_dict_dupes(_parsed)        


def remove_dict_dupes(dicts: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
    """Remove duplicate dictionaries from a list of dictionaries"""
    _tupleify = [
        [(k, v) for k, v in d.items()]
        for d in dicts
    ]
    _set_tupleify = []
    for t in _tupleify:
        if t not in _set_tupleify:
            _set_tupleify.append(t)
    return [
        {i[0]: i[1] for i in kv}
        for kv in _set_tupleify
    ]
