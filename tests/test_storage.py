import string
import tempfile
import typing

import pytest
import pytest_mock
import yaml

import fair.registry.file_types as fdp_file
import fair.registry.storage as fdp_store
from tests.test_requests import LOCAL_URL

from . import conftest as conf

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"


@pytest.mark.storage
@pytest.mark.dependency(name="store_author")
def test_store_user(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert fdp_store.store_user(local_config[1], LOCAL_URL, local_registry._token)


@pytest.mark.storage
def test_populate_file_type(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert len(fdp_store.populate_file_type(LOCAL_URL, local_registry._token)) == len(
            fdp_file.FILE_TYPES
        )


@pytest.mark.storage
def test_store_working_config(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".yaml", delete=False
        ) as tempf:
            yaml.dump(
                {"run_metadata": {"write_data_store": "data_store"}}, tempf
            )

        assert fdp_store.store_working_config(
            local_config[1], LOCAL_URL, tempf.name, local_registry._token
        )


@pytest.mark.storage
def test_store_working_script(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        with tempfile.NamedTemporaryFile(
            mode="w+", suffix=".yaml", delete=False
        ) as tempf:
            yaml.dump(
                {"run_metadata": {"write_data_store": "data_store"}}, tempf
            )

        _temp_script = tempfile.NamedTemporaryFile(suffix=".sh", delete=False)

        assert fdp_store.store_working_script(
            local_config[1], LOCAL_URL, _temp_script.name, tempf.name, local_registry._token
        )


@pytest.mark.storage
def test_store_namespace(
    local_registry: conf.RegistryTest, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert fdp_store.store_namespace(
            LOCAL_URL,
            local_registry._token,
            "test_namespace",
            "Testing Namespace",
            "https://www.notarealsite.com",
        )


@pytest.mark.storage
def test_calc_file_hash():
    with tempfile.NamedTemporaryFile(
        mode="w+", suffix=".txt", delete=False
    ) as tempf:
        tempf.write(string.ascii_letters)
    _HASH = "db16441c4b330570a9ac83b0e0b006fcd74cc32b"
    # Based on hash calculated at 2021-10-15
    assert fdp_store.calculate_file_hash(tempf.name) == _HASH
    assert fdp_store.check_match(tempf.name, [{"hash": _HASH}])
