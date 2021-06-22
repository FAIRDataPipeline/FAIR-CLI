from typing import Dict


def flatten_dict(
    in_dict: Dict,
    out_dict: Dict = None,
    parent_key: str = None,
    separator: str = ".",
) -> Dict:
    if out_dict is None:
        out_dict = {}

    for label, value in in_dict.items():
        new_label = f"{parent_key}{separator}{label}" if parent_key else label
        if isinstance(value, dict):
            flatten_dict(
                in_dict=value, output_dict=out_dict, parent_key=new_label
            )
            continue

        out_dict[new_label] = value

    return out_dict


def expand_dictionary(
    in_dict: Dict, out_dict: Dict = None, separator: str = "."
) -> Dict:
    if out_dict is None:
        out_dict = {}

    for label, value in in_dict.items():
        if separator not in label:
            out_dict.update({label: value})
            continue
        key, _components = label.split(separator, 1)
        if key not in out_dict:
            out_dict[key] = {}
        expand_dictionary({_components: value}, out_dict[key])

    return out_dict
