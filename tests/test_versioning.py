import pytest
import semver

import fair.registry.versioning as fdp_ver


@pytest.mark.versioning
def test_incrementer_parsing():
    for key in fdp_ver.BUMP_FUNCS:
        assert (
            fdp_ver.parse_incrementer("${{" + key + "}}")
            == fdp_ver.BUMP_FUNCS[key]
        )


@pytest.mark.versioning
def test_remove_incrementing():
    assert fdp_ver.undo_incrementer("${{MINOR}}") == "${{ LATEST }}"


@pytest.mark.versioning
def test_get_latest():
    assert fdp_ver.get_latest_version() == semver.VersionInfo(0, 0, 0)
    results = [
        {"version": "0.1.0"},
        {"version": "1.2.3-rc4"},
        {"version": "2.1.0"},
    ]
    assert fdp_ver.get_latest_version(results) == semver.VersionInfo(2, 1, 0)


@pytest.mark.versioning
def test_default_bump():
    assert fdp_ver.default_bump(
        semver.VersionInfo(0, 1, 0)
    ) == semver.VersionInfo(0, 1, 1)
