import pytest
import semver

import fair.registry.versioning as fdp_ver


@pytest.mark.versioning
def test_version_bumps():
    _version_bumps= {
        "minor": '${{ MINOR }}',
        "major": '${{ MAJOR }}',
        "patch": '${{ PATCH }}'
    }

    _expect = {
        "minor": "0.2.0",
        "major": "1.0.0",
        "patch": "0.1.1"
    }

    _orig_version = semver.VersionInfo.parse("0.1.0")

    for bump, var in _version_bumps.items():
        _func = fdp_ver.parse_incrementer(var)
        assert bump in _func 
        assert getattr(_orig_version, _func)() == semver.VersionInfo.parse(_expect[bump])
