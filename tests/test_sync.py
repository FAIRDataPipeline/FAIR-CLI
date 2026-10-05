import hashlib
import json
import os
import pathlib
import tempfile

import click.testing
import pytest
import pytest_mock
import requests
import yaml

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.sync as fdp_sync
from fair.cli import cli
from fair.registry.requests import get
from tests.conftest import RegistryTest
from tests.conftest import MotoTestServer
import fair.common as fdp_com
import fair.testing as fdp_test

# The function itself, for a test that needs it where a fixture has mocked it
_POST_ELSE_GET = fdp_req.post_else_get

REPO_ROOT = pathlib.Path(os.path.dirname(__file__)).parent
PULL_TEST_CFG = os.path.join(os.path.dirname(__file__), "data", "test_pull_config.yaml")


@pytest.mark.faircli_sync
def test_pull_download(file_server: str):
    _file = fdp_sync.download_from_registry(
        "http://127.0.0.1:8000", file_server, "data.csv"
    )

    assert open(_file).read() == "a,b\n1,2\n"


@pytest.mark.faircli_sync
@pytest.mark.parametrize("as_recorded", [True, False])
def test_fetch_data_product(
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    file_server: str,
    as_recorded: bool,
):
    # The bytes of the file served, which is written as text and so differs
    # with the platform, and the hash the registry holds for it
    _served = requests.get(f"{file_server}data.csv").content
    recorded = hashlib.sha1(_served).hexdigest() if as_recorded else "0" * 40

    tempd = os.path.join(tmp_path, "store")
    _dummy_data_product_name = "test"
    _dummy_data_product_version = "2.3.0"
    _dummy_data_product_namespace = "testing"
    _temp_dir = tmp_path / "temp"
    _temp_dir.mkdir()
    mocker.patch.object(tempfile, "tempdir", str(_temp_dir))

    def mock_url_get(url, *args, **kwargs):
        if "storage_location" in url:
            return {
                "path": "data.csv",
                "hash": recorded,
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

    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    _example_data_product = {
        "version": _dummy_data_product_version,
        "namespace": "namespace",
        "name": _dummy_data_product_name,
        "object": "object",
    }
    _out_file = os.path.join(
        tempd, _dummy_data_product_namespace, _dummy_data_product_name, "2.3.0"
    )
    if as_recorded:
        fdp_sync.fetch_data_product("", tempd, _example_data_product)
        assert open(_out_file, "rb").read() == _served
    else:
        # Not the file the registry describes, so not kept as it
        with pytest.raises(
            fdp_exc.SynchronisationError, match="was not fetched"
        ):
            fdp_sync.fetch_data_product("", tempd, _example_data_product)
        assert not os.path.exists(_out_file)
    # Moved into the data store, or removed: either way not left behind
    assert not os.listdir(_temp_dir)


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
@pytest.mark.parametrize("local_data_store", [None, "/data/store"])
def test_file_moves_before_its_records(
    mocker: pytest_mock.MockerFixture, local_data_store
):
    """A failure to move the file must leave no record for a retry to find"""
    _data_product = {
        "url": "data_product_url",
        "name": "test",
        "version": "1.0.0",
        "namespace": "namespace",
        "object": "object",
        "external_object": None,
    }
    _moved = []

    def mock_get(uri, obj, *args, **kwargs):
        if obj == "namespace":
            return [{"url": "http://example/api/namespace/1/"}]
        elif obj == "data_product":
            # Nothing on the destination, one match on the origin
            return [] if uri == "dest" else [_data_product]

    def mock_url_get(url, *args, **kwargs):
        if url == "object":
            return {
                "url": "object",
                "storage_location": "location",
                "components": [],
            }
        return {"public": True}

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    mocker.patch(
        "fair.registry.sync.sync_dependency_chain",
        lambda **kwargs: _moved.append("records"),
    )
    for _mover in ("fetch_data_product", "upload_object"):
        mocker.patch(
            f"fair.registry.sync.{_mover}",
            lambda *args, _mover=_mover: _moved.append(_mover),
        )

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:test@v1.0.0"],
        local_data_store=local_data_store,
    )

    _mover = "fetch_data_product" if local_data_store else "upload_object"
    assert _moved == [_mover, "records"]


# An object in the local registry whose file is under the given storage root
def _mock_object(mocker: pytest_mock.MockerFixture, root: str):
    def mock_url_get(url, *args, **kwargs):
        if url == "object":
            return {
                "storage_location": "location",
                "description": "A csv file",
            }
        elif url == "location":
            return {"path": "testing/data/abc123.csv", "storage_root": "root"}
        return {"root": root}

    mocker.patch("fair.registry.requests.url_get", mock_url_get)


@pytest.mark.faircli_sync
def test_upload_object_from_where_it_is(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # A file on this machine is uploaded as it stands, with no copy made
    _mock_object(mocker, f"file://{tmp_path}{os.path.sep}")
    mocker.patch(
        "fair.registry.sync.download_from_registry",
        side_effect=AssertionError("a local file was fetched"),
    )
    _upload = mocker.patch("fair.registry.storage.upload_remote_file")

    assert fdp_sync.upload_object(_ORIGIN, _DEST, "", "", "object")
    _upload.assert_called_once_with(
        f"{tmp_path}{os.path.sep}testing/data/abc123.csv", _DEST, ""
    )


@pytest.mark.faircli_sync
@pytest.mark.parametrize("local", [True, False])
def test_upload_object_failure(
    mocker: pytest_mock.MockerFixture, tmp_path, local: bool
):
    # A file that was not uploaded is an error, not a warning; and one that
    # was fetched to be uploaded is not left behind
    _fetched = tmp_path / "fetched"
    _fetched.write_bytes(b"a,b\n1,2\n")
    _mock_object(
        mocker, f"file://{tmp_path}/" if local else "https://example.org/"
    )
    mocker.patch(
        "fair.registry.sync.download_from_registry", return_value=str(_fetched)
    )
    mocker.patch(
        "fair.registry.storage.upload_remote_file",
        side_effect=fdp_exc.RegistryError("Registry Returned: 403"),
    )

    with pytest.raises(fdp_exc.SynchronisationError, match="was not uploaded"):
        fdp_sync.upload_object(_ORIGIN, _DEST, "", "", "object")
    assert _fetched.exists() is local


@pytest.mark.faircli_sync
@pytest.mark.parametrize("fetched_from_first", [True, False])
def test_dest_object_of_a_file_at_two_locations(
    mocker: pytest_mock.MockerFixture, fetched_from_first: bool
):
    # A registered file has two locations with one hash on the destination:
    # where it is stored, which an object is at, and where it was fetched
    # from, which none is. The registry may list them in either order
    _locations = [
        {"url": f"{_DEST}storage_location/1/"},
        {"url": f"{_DEST}storage_location/2/"},
    ]
    _stored = _locations[1 if fetched_from_first else 0]
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda *args, **kwargs: {
            "storage_location": "location",
            "hash": "abc",
        },
    )

    def mock_get(uri, obj_type, *args, params=None, **kwargs):
        if obj_type == "storage_location":
            assert params == {"hash": "abc"}
            return _locations
        _stored_id = fdp_req.get_obj_id_from_url(_stored["url"])
        if params["storage_location"] == _stored_id:
            return [{"url": f"{_DEST}object/7/"}]
        return []

    mocker.patch("fair.registry.requests.get", mock_get)

    assert (
        fdp_sync.get_dest_object_url("object", _DEST, "", "")
        == f"{_DEST}object/7/"
    )


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

        # The registered copy is found by its place in the data store, though
        # the registry also records it at the address it was fetched from.
        # (The copy in the clone may differ from it in its line endings.)
        _token = local_registry._token
        _data_product = fdp_req.get(
            local_registry._url,
            "data_product",
            _token,
            params={"name": "SEIRS_model/parameters"},
        )[0]
        _location = fdp_req.url_get(
            fdp_req.url_get(_data_product["object"], _token)[
                "storage_location"
            ],
            _token,
        )
        _root = fdp_req.url_get(_location["storage_root"], _token)["root"]
        _res = _cli_runner.invoke(
            cli,
            [
                "identify",
                "--local",
                f"{_root}{_location['path']}".replace("file://", ""),
            ],
            catch_exceptions=True,
        )
        assert _res.exit_code == 0
        assert (
            "Is linked to 'data_product': SEIRS_model/parameters"
            in _res.output
        )


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
@pytest.mark.parametrize(
    "obj_type,record,key",
    [
        (
            "external_object",
            {"title": "An extract", "primary_not_supplement": False},
            "primary_not_supplement",
        ),
        (
            "storage_location",
            {
                "path": "testing/output/abc123.csv",
                "hash": "abc123",
                "public": False,
                "storage_root": f"{_ORIGIN}storage_root/2/",
            },
            "public",
        ),
    ],
)
def test_push_keeps_false_values(
    push_mocks,
    mocker: pytest_mock.MockerFixture,
    obj_type: str,
    record,
    key: str,
):
    # What is false in the local registry is sent as false. Left out, the
    # remote would apply its own default, which for both of these is true
    mocker.patch("fair.registry.requests.post_else_get", _POST_ELSE_GET)
    _access = mocker.patch(
        "fair.registry.requests._access",
        return_value={"url": f"{_DEST}{obj_type}/1/"},
    )
    fdp_sync._get_new_url(
        origin_uri=_ORIGIN,
        origin_token="",
        dest_uri=_DEST,
        dest_token="",
        object_url=f"{_ORIGIN}{obj_type}/9/",
        new_urls={f"{_ORIGIN}storage_root/2/": f"{_DEST}storage_root/7/"},
        writable_data=record,
        object_data=record,
        public=False,
    )
    assert json.loads(_access.call_args.kwargs["data"])[key] is False


@pytest.mark.faircli_sync
def test_push_to_registry_without_data_store(
    push_mocks, mocker: pytest_mock.MockerFixture, caplog
):
    # As on a registry whose site URL differs from its API's address
    mocker.patch("fair.registry.requests.get", lambda *args, **kwargs: [])
    _root_url = f"{_ORIGIN}storage_root/3/"
    _new_url = fdp_sync._get_new_url(
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
    assert _new_url == f"{_DEST}storage_root/1/"
    assert any(
        record.levelname == "WARNING"
        and "has no storage root 'http://127.0.0.1:8001/data/'"
        in record.getMessage()
        for record in caplog.records
    )
