import string
import typing
import os
import hashlib

import pytest
import pytest_mock
import yaml

import fair.registry.file_types as fdp_file
import fair.registry.storage as fdp_store
from tests.test_requests import LOCAL_URL

from . import conftest as conf

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"


@pytest.mark.faircli_storage
@pytest.mark.dependency(name="store_author")
def test_store_user(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert fdp_store.store_user(local_config[1], LOCAL_URL, local_registry._token)


@pytest.mark.faircli_storage
def test_populate_file_type(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        assert len(
            fdp_store.populate_file_type(LOCAL_URL, local_registry._token)
        ) == len(fdp_file.FILE_TYPES)


@pytest.mark.faircli_storage
def test_store_working_config(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        temp_file_name = os.path.join(
            tmp_path,
            f'{hashlib.sha1(tmp_path.__str__().encode("utf-8")).hexdigest()}.yaml',
        )
        with open(temp_file_name, "w") as tempf:
            yaml.dump(
                {"run_metadata": {"write_data_store": os.path.dirname(temp_file_name)}},
                tempf,
            )

        assert fdp_store.store_working_config(
            local_config[1], LOCAL_URL, temp_file_name, local_registry._token
        )


@pytest.mark.faircli_storage
def test_store_working_script(
    local_config: typing.Tuple[str, str],
    local_registry: conf.RegistryTest,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    mocker.patch("fair.common.registry_home", lambda: local_registry._install)
    with local_registry:
        temp_file_name = os.path.join(
            tmp_path,
            f'{hashlib.sha1(tmp_path.__str__().encode("utf-8")).hexdigest()}.yaml',
        )
        with open(temp_file_name, "w") as tempf:
            yaml.dump(
                {"run_metadata": {"write_data_store": os.path.dirname(temp_file_name)}},
                tempf,
            )

        temp_script_name = os.path.join(
            tmp_path,
            f'{hashlib.sha1(tmp_path.__str__().encode("utf-8")).hexdigest()}.sh',
        )
        with open(temp_script_name, "w") as _temp_script:
            _temp_script.write(string.ascii_letters)

        assert fdp_store.store_working_script(
            local_config[1],
            LOCAL_URL,
            temp_script_name,
            temp_file_name,
            local_registry._token,
        )


@pytest.mark.faircli_storage
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


@pytest.mark.faircli_storage
def test_calc_file_hash(tmp_path):
    temp_file_name = os.path.join(
        tmp_path, f'{hashlib.sha1(tmp_path.__str__().encode("utf-8")).hexdigest()}.txt'
    )
    with open(temp_file_name, "w") as tempf:
        tempf.write(string.ascii_letters)
    _HASH = "db16441c4b330570a9ac83b0e0b006fcd74cc32b"
    # Based on hash calculated at 2021-10-15
    assert fdp_store.calculate_file_hash(tempf.name) == _HASH
    assert fdp_store.check_match(tempf.name, [{"hash": _HASH}])


# @pytest.mark.faircli_storage
# @pytest.mark.skipif("FAIR_REMOTE_TOKEN" not in os.environ, reason="Fails on GH CI")
# def test_get_upload_url(
#     local_config: typing.Tuple[str, str],
#     local_registry: conf.RegistryTest,
#     remote_registry: conf.RegistryTest,
#     s3_bucket: conf.s3_test,
#     mocker: pytest_mock.MockerFixture,
# ):
#     mocker.patch(
#         "fair.configuration.get_remote_token",
#         lambda *args, **kwargs: remote_registry._token,
#     )
#     mocker.patch(
#         "fair.registry.requests.local_token",
#         lambda *args: local_registry._token,
#     )
#     mocker.patch(
#         "fair.registry.server.launch_server", lambda *args, **kwargs: True
#     )
#     mocker.patch("fair.registry.server.stop_server", lambda *args: True)
#     with remote_registry, local_registry, s3_bucket:

#         assert fdp_store.get_upload_url(_HASH, "http://127.0.0.1:8000/api", remote_registry._token )["url"]
