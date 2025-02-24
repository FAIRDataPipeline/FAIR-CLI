import os
import shutil
import typing
import uuid

import pytest
import pytest_mock
import yaml

import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.staging as fdp_stage

LOCAL_REGISTRY_URL = "http://127.0.0.1:8000/api"

TEST_DATA = os.path.join(os.path.dirname(__file__), "data")


@pytest.fixture
def stager(local_config: typing.Tuple[str, str]):
    _stager = fdp_stage.Stager(local_config[1])
    _stager.initialise()
    return _stager


@pytest.mark.faircli_staging
def test_job_status_change(stager: fdp_stage.Stager, mocker: pytest_mock.MockerFixture):
    _id = uuid.uuid4()

    with pytest.raises(fdp_exc.StagingError):
        stager.change_job_stage_status(_id, True)

    mocker.patch("fair.run.get_job_dir", lambda x: True)

    stager.add_to_staging(str(_id), "job")

    stager.change_job_stage_status(str(_id), True)

    with open(stager._staging_file) as stage_f:
        _dict = yaml.safe_load(stage_f)
        assert _dict["job"][str(_id)]
    stager.reset_staged()

    with open(stager._staging_file) as stage_f:
        _dict = yaml.safe_load(stage_f)
        assert not any(_dict["job"].values())


@pytest.mark.faircli_staging
def test_registry_entry_for_file(
    stager: fdp_stage.Stager, mocker: pytest_mock.MockerFixture
):
    _url = "http://127.0.0.1:8000/api/storage_location/1"

    def dummy_get(uri, obj_path, token, params):
        if uri != LOCAL_REGISTRY_URL:
            raise fdp_exc.RegistryError("No such registry")
        if obj_path != "storage_location":
            raise fdp_exc.RegistryError("Invalid object type")
        if "path" not in params:
            raise fdp_exc.RegistryError("Invalid call")
        return [_url]

    mocker.patch(
        "fair.registry.requests.get",
        lambda *args, **kwargs: dummy_get(*args, **kwargs),
    )
    mocker.patch("fair.registry.requests.local_token", lambda: "")
    assert (
        stager.find_registry_entry_for_file(LOCAL_REGISTRY_URL, "/not/a/path") == _url
    )


@pytest.mark.faircli_staging
def test_get_job_data(
    local_registry,
    stager: fdp_stage.Stager,
    local_config: typing.Tuple[str, str],
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    tmp_path,
):
    with local_registry:
        mocker.patch("fair.common.registry_home", lambda: local_registry._install)
        _id = uuid.uuid4()

        with pytest.raises(fdp_exc.StagingError):
            stager.get_job_data(LOCAL_REGISTRY_URL, _id)

        tempd = tmp_path.__str__()
        _job_dir = os.path.join(tempd, str(_id))
        os.makedirs(_job_dir)
        mocker.patch("fair.run.get_job_dir", lambda x: _job_dir)
        mocker.patch("fair.common.JOBS_DIR", tempd)
        with pytest.raises(fdp_exc.FileNotFoundError):
            stager.get_job_data(LOCAL_REGISTRY_URL, _id)
        _dummy_url = "http://not-a-url.com"
        mocker.patch.object(
            stager,
            "find_registry_entry_for_file",
            lambda *args: {"url": _dummy_url},
        )
        mocker.patch(
            "fair.registry.requests.get",
            lambda *args, **kwargs: [{"url": _dummy_url}],
        )
        _cfg_path = os.path.join(
            pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
        )
        shutil.copy(
            _cfg_path,
            os.path.join(_job_dir, fdp_com.USER_CONFIG_FILE),
        )
        _jobs = stager.get_job_data(LOCAL_REGISTRY_URL, _id)
        assert _jobs == {
            "jobs": [],
            "user_written_objects": 2 * [_dummy_url],
            "config_file": _dummy_url,
            "script_file": None,
        }
