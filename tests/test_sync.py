import os
import pathlib

import click.testing
import pytest
import pytest_mock
import yaml

import fair.exceptions as fdp_exc
import fair.registry.sync as fdp_sync
from fair.cli import cli
from fair.registry.requests import get
from tests.conftest import RegistryTest
from tests.conftest import MotoTestServer
import fair.common as fdp_com
import fair.testing as fdp_test

REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(os.path.dirname(__file__), "data", "test_pull_config.yaml")


@pytest.mark.faircli_sync
def test_pull_download(file_server: str):
    _file = fdp_sync.download_from_registry(
        "http://127.0.0.1:8000", file_server, "data.csv"
    )

    assert open(_file).read() == "a,b\n1,2\n"


@pytest.mark.faircli_sync
def test_fetch_data_product(
    mocker: pytest_mock.MockerFixture, tmp_path, file_server: str
):

    tempd = os.path.join(tmp_path, "store")
    _dummy_data_product_name = "test"
    _dummy_data_product_version = "2.3.0"
    _dummy_data_product_namespace = "testing"

    def mock_get(url, obj, *args, **kwargs):
        if obj == "storage_location":
            return [
                {
                    "path": "/this/is/a/dummy/path",
                    "storage_root": "https://dummyurl/",
                }
            ]
        elif obj == "storage_root":
            return [{"root": "https://fake/root/"}]
        elif obj == "namespace":
            return [{"name": _dummy_data_product_namespace, "url": "namespace"}]
        elif obj == "data_product":
            return [
                {
                    "data_product": _dummy_data_product_name,
                    "version": _dummy_data_product_version,
                    "namespace": "namespace",
                }
            ]

    def mock_url_get(url, *args, **kwargs):
        if "storage_location" in url:
            return {
                "path": "data.csv",
                "storage_root": "storage_root",
            }
        elif "storage_root" in url:
            return {"root": file_server}
        elif "namespace" in url:
            return {
                "name": _dummy_data_product_namespace,
                "url": "namespace",
            }
        elif "object" in url:
            return {
                "storage_location": "storage_location",
                "url": "object",
            }

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    _example_data_product = {
        "version": _dummy_data_product_version,
        "namespace": "namespace",
        "name": _dummy_data_product_name,
        "object": "object",
    }
    fdp_sync.fetch_data_product("", tempd, _example_data_product)
    _out_file = os.path.join(
        tempd, _dummy_data_product_namespace, _dummy_data_product_name, "2.3.0"
    )
    assert open(_out_file).read() == "a,b\n1,2\n"


@pytest.mark.faircli_sync
def test_sync_data_products_fetches_the_data_product(
    mocker: pytest_mock.MockerFixture,
):
    """Pulling passes the data product itself, not the external object"""
    _data_product = {
        "url": "data_product_url",
        "name": "test",
        "version": "1.0.0",
        "namespace": "namespace",
        "object": "object",
        "external_object": "external_object_url",
    }

    def mock_get(uri, obj, *args, **kwargs):
        if obj == "namespace":
            return [{"url": "http://example/api/namespace/1/"}]
        elif obj == "data_product":
            # Nothing on the destination, one match on the origin
            return [] if uri == "dest" else [_data_product]

    def mock_url_get(url, *args, **kwargs):
        if url == "object":
            return {"storage_location": "storage_location", "components": []}
        elif url == "storage_location":
            return {"public": True}
        # The external object, which replaces the data product in `result`
        return {"url": "external_object_url"}

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    mocker.patch("fair.registry.sync.sync_dependency_chain", lambda **kwargs: None)
    _fetch = mocker.patch("fair.registry.sync.fetch_data_product")

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:test@v1.0.0"],
        local_data_store="/data/store",
    )

    _fetch.assert_called_once_with("", "/data/store", _data_product)


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="init")
def test_init(
    global_config,
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    monkeypatch_module.chdir(pyDataPipeline)
    monkeypatch_module.setattr(
        "fair.registry.server.launch_server", lambda *args, **kwargs: False
    )
    _cli_runner = click.testing.CliRunner()
    config_path = os.path.join(pyDataPipeline, fdp_com.FAIR_CLI_CONFIG)
    _config = fdp_test.create_configurations(
        local_registry._install,
        pyDataPipeline,
        remote_registry._install,
        global_config,
        True,
    )
    yaml.dump(_config, open(config_path, "w"))
    with capsys.disabled():
        print(_config)
        print("\tRUNNING: fair init --debug")
    with local_registry, remote_registry:
        _res = _cli_runner.invoke(
            cli, ["init", "--debug", "--using", config_path], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
    assert _res.exit_code == 0


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="pull", depends=["init"])
def test_pull(
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")
    _cli_runner = click.testing.CliRunner()
    _cfg_path = os.path.join(pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml")
    with capsys.disabled():
        print(f"\tRUNNING: fair pull {_cfg_path} --debug")
    with local_registry, remote_registry:
        _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])
    assert _res.exit_code == 0


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="run", depends=["pull"])
def test_run(
    global_config: str,
    local_registry: str,
    remote_registry: str,
    mocker: pytest_mock.MockerFixture,
    pyDataPipeline: str,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry:
        _cfg_path = os.path.join(
            pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
        )
        with capsys.disabled():
            print(f"\tRUNNING: fair pull {_cfg_path} --debug --dirty")
        _res = _cli_runner.invoke(cli, ["run", _cfg_path, "--debug", "--dirty"])
        assert _res.exit_code == 0
        assert get(
            "http://127.0.0.1:8000/api/",
            "data_product",
            local_registry._token,
            params={
                "name": "SEIRS_model/results/figure/python",
                "version": "0.0.1",
            },
        )


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="push", depends=["run"])
def test_push(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(cli, ["list"])
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["add", "testing:SEIRS_model/results/figure/python@v0.0.1"]
        )
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(cli, ["push", "--debug"], catch_exceptions=True)
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0
        assert get(
            "http://127.0.0.1:8001/api/",
            "data_product",
            remote_registry._token,
            params={
                "name": "SEIRS_model/results/figure/python",
                "version": "0.0.1",
            },
        )


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="find", depends=["push"])
def test_find(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(
            cli,
            ["find", "--debug", "testing:SEIRS_model/results/figure/python@v0.0.1"],
            catch_exceptions=True,
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0

        _res = _cli_runner.invoke(
            cli,
            [
                "find",
                "--debug",
                "--local",
                "testing:SEIRS_model/results/figure/python@v0.0.1",
            ],
            catch_exceptions=True,
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="identify", depends=["push"])
def test_identify(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    capsys,
):
    try:
        import data_pipeline_api  # noqa
    except ModuleNotFoundError:
        pytest.skip("Python API implementation not installed")

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )

        _seirs_parameters_file = os.path.join(
            pyDataPipeline, "simpleModel", "ext", "static_params_SEIRS.csv"
        )

        _res = _cli_runner.invoke(
            cli, ["identify", "--debug", _seirs_parameters_file], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0

        _res = _cli_runner.invoke(
            cli,
            ["identify", "--debug", "--local", _seirs_parameters_file],
            catch_exceptions=True,
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0


_ORIGIN = "http://127.0.0.1:8000/api/"
_DEST = "http://127.0.0.1:8001/api/"
# The remote's own data store, deliberately not storage_root 1
_DEST_DATA_STORE = f"{_DEST}storage_root/5/"
# Two projects' data stores in one local registry (1 and 3) and a web root
_ROOTS = {
    f"{_ORIGIN}storage_root/1/": "file:///projects/a/.fair/data_store/",
    f"{_ORIGIN}storage_root/2/": "https://github.com/",
    f"{_ORIGIN}storage_root/3/": "file:///projects/b/.fair/data_store/",
}


@pytest.fixture
def push_mocks(mocker: pytest_mock.MockerFixture):
    mocker.patch(
        "fair.registry.requests.get_obj_type_from_url",
        lambda url, token=None: url.split("/")[-3],
    )
    mocker.patch(
        "fair.registry.requests.get_filter_variables",
        lambda *args: ["root", "path", "hash", "public", "storage_root"],
    )
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, token=None: {"url": url, "root": _ROOTS[url]},
    )

    def dummy_get(uri, obj_path, token, params=None, **kwargs):
        if obj_path == "storage_root" and params == {
            "root": "http://127.0.0.1:8001/data/"
        }:
            return [{"url": _DEST_DATA_STORE}]
        return []

    mocker.patch("fair.registry.requests.get", dummy_get)
    return mocker.patch(
        "fair.registry.requests.post_else_get",
        lambda uri, obj_type, data, token, params: {"data": data},
    )


@pytest.mark.faircli_sync
@pytest.mark.parametrize(
    "root_url,remapped",
    [(url, root.startswith("file://")) for url, root in _ROOTS.items()],
)
def test_push_storage_root(push_mocks, root_url: str, remapped: bool):
    _new_url = fdp_sync._get_new_url(
        origin_uri=_ORIGIN,
        origin_token="",
        dest_uri=_DEST,
        dest_token="",
        object_url=root_url,
        new_urls={},
        writable_data={"root": _ROOTS[root_url]},
        object_data={"url": root_url, "root": _ROOTS[root_url]},
        public=True,
    )
    if remapped:
        assert _new_url == _DEST_DATA_STORE
    else:
        assert _new_url == {"data": {"root": _ROOTS[root_url]}}


@pytest.mark.faircli_sync
@pytest.mark.parametrize(
    "root_url,remapped",
    [
        (f"{_ORIGIN}storage_root/3/", True),
        (f"{_ORIGIN}storage_root/2/", False),
    ],
)
def test_push_storage_location(push_mocks, root_url: str, remapped: bool):
    _location = {
        "path": "testing/output/abc123.csv",
        "hash": "abc123",
        "public": True,
        "storage_root": root_url,
    }
    _dest_root = f"{_DEST}storage_root/7/"
    _posted = fdp_sync._get_new_url(
        origin_uri=_ORIGIN,
        origin_token="",
        dest_uri=_DEST,
        dest_token="",
        object_url=f"{_ORIGIN}storage_location/9/",
        new_urls={root_url: _dest_root},
        writable_data=_location,
        object_data=_location,
        public=True,
    )["data"]
    if remapped:
        # Stored on the remote by hash, in the remote data store
        assert _posted["path"] == "abc123"
        assert _posted["storage_root"] == _DEST_DATA_STORE
    else:
        assert _posted["path"] == "testing/output/abc123.csv"
        assert _posted["storage_root"] == _dest_root


@pytest.mark.faircli_sync
def test_push_to_registry_without_data_store(
    push_mocks, mocker: pytest_mock.MockerFixture
):
    mocker.patch("fair.registry.requests.get", lambda *args, **kwargs: [])
    _root_url = f"{_ORIGIN}storage_root/3/"
    with pytest.raises(fdp_exc.RegistryError, match="no data store"):
        fdp_sync._get_new_url(
            origin_uri=_ORIGIN,
            origin_token="",
            dest_uri=_DEST,
            dest_token="",
            object_url=_root_url,
            new_urls={},
            writable_data={"root": _ROOTS[_root_url]},
            object_data={"url": _root_url, "root": _ROOTS[_root_url]},
            public=True,
        )
