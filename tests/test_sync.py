import collections
import hashlib
import json
import os
import pathlib
import tempfile
import uuid

import click.testing
import pytest
import pytest_mock
import requests
import yaml

import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
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
        assert (
            fdp_sync.fetch_data_product("", tempd, _example_data_product)
            == _out_file
        )
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
    _fetch = mocker.patch("fair.registry.sync._place_pulled_file")

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:test@v1.0.0"],
        local_data_store="/data/store",
    )

    _fetch.assert_called_once_with(
        "dest", "", "", "/data/store", _data_product
    )


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
    for _mover in ("_place_pulled_file", "upload_object"):
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

    _mover = "_place_pulled_file" if local_data_store else "upload_object"
    assert _moved == [_mover, "records"]


@pytest.mark.faircli_sync
@pytest.mark.parametrize("there", [None, "the file", "another file"])
def test_fetch_data_product_to_a_place(
    mocker: pytest_mock.MockerFixture, tmp_path, file_server: str, there
):
    # Fetched to the place asked for. A file there already is the data
    # product's, with nothing fetched, if it is the one the registry
    # describes, and a refusal if it is another
    _served = requests.get(f"{file_server}data.csv").content
    _records = {
        "object": {"storage_location": "storage_location"},
        "storage_location": {
            "path": "data.csv",
            "hash": hashlib.sha1(_served).hexdigest(),
            "storage_root": "storage_root",
        },
        "storage_root": {"root": file_server},
        "namespace": {"name": "testing"},
    }
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )
    _data_product = {
        "version": "2.3.0",
        "namespace": "namespace",
        "name": "test",
        "object": "object",
    }
    _out_file = os.path.join(tmp_path, "store", "held", "already.csv")
    if there:
        os.makedirs(os.path.dirname(_out_file))
        with open(_out_file, "wb") as out_f:
            out_f.write(_served if there == "the file" else b"other")
        mocker.patch(
            "fair.registry.sync.download_from_registry",
            side_effect=AssertionError("the file was fetched"),
        )

    def _fetch():
        return fdp_sync.fetch_data_product(
            "", os.path.join(tmp_path, "store"), _data_product, _out_file
        )

    if there == "another file":
        with pytest.raises(
            fdp_exc.SynchronisationError, match="holds another file"
        ):
            _fetch()
        assert open(_out_file, "rb").read() == b"other"
    else:
        assert _fetch() == _out_file
        assert open(_out_file, "rb").read() == _served


@pytest.mark.faircli_sync
@pytest.mark.parametrize("held", [None, "the file", "as private"])
def test_pulled_file_is_recorded_where_it_is_put(
    mocker: pytest_mock.MockerFixture, tmp_path, held
):
    # The remote's record of a file says where it is on the remote. Pulled,
    # the file is recorded in the local registry at its place in the local
    # data store. One the store holds already, by its hash, is the one
    # recorded, and that is where the file is looked for or fetched to; a
    # private record of the same bytes is not that file
    _local, _remote = _ORIGIN, _DEST
    _store = str(tmp_path)
    _root_url = f"{_local}storage_root/2/"
    _records = {
        "object": {"storage_location": "location"},
        "location": {"hash": "abc", "public": True, "path": "abc"},
    }
    _held_location = {
        "url": f"{_local}storage_location/7/",
        "path": os.path.join("PSU", "first", "1.0.0.csv"),
        "public": held == "the file",
    }
    _fetched = os.path.join(_store, "testing", "second", "1.0.0.csv")
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )
    mocker.patch(
        "fair.registry.storage.get_write_storage", return_value=_root_url
    )
    _get = mocker.patch(
        "fair.registry.requests.get",
        return_value=[_held_location] if held else [],
    )
    _post = mocker.patch(
        "fair.registry.requests.post",
        return_value={"url": f"{_local}storage_location/8/"},
    )
    _fetch = mocker.patch(
        "fair.registry.sync.fetch_data_product",
        side_effect=lambda token, store, data_product, out_file: (
            out_file or _fetched
        ),
    )
    _data_product = {"object": "object"}

    _placed = fdp_sync._place_pulled_file(
        _local, "", "", _store, _data_product
    )

    assert _get.call_args.args[:2] == (_local, "storage_location")
    assert _get.call_args.kwargs["params"] == {
        "hash": "abc",
        "storage_root": fdp_req.get_obj_id_from_url(_root_url),
    }
    if held == "the file":
        _held_file = os.path.join(_store, _held_location["path"])
        _fetch.assert_called_once_with("", _store, _data_product, _held_file)
        _post.assert_not_called()
        assert _placed == {"location": _held_location["url"]}
    else:
        _fetch.assert_called_once_with("", _store, _data_product, None)
        assert _post.call_args.kwargs["data"] == {
            "path": os.path.join("testing", "second", "1.0.0.csv"),
            "storage_root": _root_url,
            "public": True,
            "hash": "abc",
        }
        assert _placed == {"location": f"{_local}storage_location/8/"}
    assert _remote not in str(_placed)


@pytest.mark.faircli_sync
def test_dependency_chain_with_a_placed_object(
    mocker: pytest_mock.MockerFixture,
):
    # An object recorded on the destination already in its own way, as a
    # pulled file's place in the local data store is, is not synchronised,
    # and what names it is given that record
    _local, _remote = _ORIGIN, _DEST
    _location = f"{_remote}storage_location/4/"
    _object = f"{_remote}object/5/"
    _placed = f"{_local}storage_location/9/"
    mocker.patch(
        "fair.registry.sync.get_dependency_chain",
        lambda *args: collections.deque([_location, _object]),
    )
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, token=None: {"storage_location": _location},
    )
    mocker.patch(
        "fair.registry.requests.get_obj_type_from_url",
        lambda url, token=None: url.split("/")[-3],
    )
    mocker.patch(
        "fair.registry.requests.get_writable_fields",
        lambda *args: ["storage_location"],
    )
    _synced = mocker.patch(
        "fair.registry.sync._get_new_url", return_value=f"{_local}object/6/"
    )

    _new_urls = fdp_sync.sync_dependency_chain(
        object_url=_object,
        dest_uri=_local,
        origin_uri=_remote,
        dest_token="",
        origin_token="token",
        placed={_location: _placed},
    )

    assert _new_urls == {_location: _placed, _object: f"{_local}object/6/"}
    _synced.assert_called_once()
    assert _synced.call_args.kwargs["object_url"] == _object
    assert _synced.call_args.kwargs["new_urls"][_location] == _placed


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
@pytest.mark.parametrize("wanted", [0, 1, None])
def test_dest_object_of_one_of_two_with_the_same_file(
    mocker: pytest_mock.MockerFixture, wanted
):
    # Two objects on the destination hold the same file, as when one file is
    # registered under two names. The one wanted has the origin object's data
    # product, wherever it is listed; with no data product, the first listed
    _objects = [{"url": f"{_DEST}object/5/"}, {"url": f"{_DEST}object/1/"}]
    _records = {
        "object": {
            "storage_location": "location",
            "data_products": [] if wanted is None else ["data_product"],
        },
        "location": {"hash": "abc"},
        "data_product": {
            "name": "same/second",
            "version": "1.0.0",
            "namespace": "namespace",
        },
        "namespace": {"name": "testing"},
    }
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )

    def mock_get(uri, obj_type, *args, **kwargs):
        if obj_type == "storage_location":
            return [{"url": f"{_DEST}storage_location/1/"}]
        elif obj_type == "object":
            return _objects
        elif obj_type == "namespace":
            return [{"url": f"{_DEST}namespace/3/"}]
        assert obj_type == "data_product"
        return [{"object": _objects[wanted]["url"]}]

    mocker.patch("fair.registry.requests.get", mock_get)

    assert (
        fdp_sync.get_dest_object_url("object", _DEST, "", "")
        == _objects[wanted or 0]["url"]
    )


@pytest.mark.faircli_sync
@pytest.mark.parametrize("on_destination", [True, False])
def test_dest_object_of_an_object_without_a_file(
    mocker: pytest_mock.MockerFixture, on_destination: bool
):
    # With no file there is no hash to find the destination's object by: it
    # is the object of the same data product there
    _records = {
        "object": {
            "url": "object",
            "storage_location": None,
            "data_products": ["data_product"],
        },
        "data_product": {
            "name": "deposit/whole",
            "version": "1.0.0",
            "namespace": "namespace",
        },
        "namespace": {"name": "testing"},
    }
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )
    _namespace_url = f"{_DEST}namespace/3/"

    def mock_get(uri, obj_type, *args, params=None, **kwargs):
        if obj_type == "namespace":
            assert params == {"name": "testing"}
            return [{"url": _namespace_url}]
        assert obj_type == "data_product"
        assert params == {
            "name": "deposit/whole",
            "version": "1.0.0",
            "namespace": fdp_req.get_obj_id_from_url(_namespace_url),
        }
        return [{"object": f"{_DEST}object/9/"}] if on_destination else []

    mocker.patch("fair.registry.requests.get", mock_get)

    if on_destination:
        assert (
            fdp_sync.get_dest_object_url("object", _DEST, "", "")
            == f"{_DEST}object/9/"
        )
    else:
        with pytest.raises(fdp_exc.RegistryError, match="has no file"):
            fdp_sync.get_dest_object_url("object", _DEST, "", "")


@pytest.mark.faircli_sync
@pytest.mark.parametrize("local_data_store", [None, "/data/store"])
def test_sync_data_product_without_a_file(
    mocker: pytest_mock.MockerFixture, local_data_store
):
    # A data product may have no file, its object having no storage location:
    # nothing is uploaded or fetched, and its records are written
    _data_product = {
        "url": "data_product_url",
        "name": "test",
        "version": "1.0.0",
        "namespace": "namespace",
        "object": "object",
        "external_object": None,
    }

    def mock_get(uri, obj, *args, **kwargs):
        if obj == "namespace":
            return [{"url": "http://example/api/namespace/1/"}]
        elif obj == "data_product":
            # Nothing on the destination, one match on the origin
            return [] if uri == "dest" else [_data_product]

    def mock_url_get(url, *args, **kwargs):
        # Only the object is asked for: it has no location to ask for
        assert url == "object"
        return {"url": "object", "storage_location": None, "components": []}

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    _records = mocker.patch("fair.registry.sync.sync_dependency_chain")
    _upload = mocker.patch("fair.registry.sync.upload_object")
    _download = mocker.patch("fair.registry.sync.download_from_registry")

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:test@v1.0.0"],
        local_data_store=local_data_store,
    )

    _upload.assert_not_called()
    _download.assert_not_called()
    assert _records.call_args.kwargs["object_url"] == "data_product_url"
    assert _records.call_args.kwargs["public"] is False


@pytest.mark.faircli_sync
def test_dependency_chain_with_substitute(mocker: pytest_mock.MockerFixture):
    # An external object shared by two data products names the first on its
    # record. Its chain holds the one it is told it is synchronised with
    _records = {
        "source": {"data_product": "first", "original_store": None},
        "first": {"object": None, "external_object": "source"},
        "second": {"object": None, "external_object": "source"},
    }
    mocker.patch(
        "fair.registry.requests.split_api_url", lambda url: (_ORIGIN, url)
    )
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: dict(_records[url]),
    )
    mocker.patch(
        "fair.registry.requests.get_obj_type_from_url",
        lambda url, *args: (
            "external_object" if url == "source" else "data_product"
        ),
    )
    mocker.patch(
        "fair.registry.requests.get_dependency_listing",
        lambda *args: {
            "external_object": ["data_product", "original_store"],
            "data_product": ["object", "external_object"],
        },
    )

    _chain = fdp_sync.get_dependency_chain("source", "")
    assert list(_chain) == ["first", "source"]
    _chain = fdp_sync.get_dependency_chain(
        "source", "", {"source": {"data_product": "second"}}
    )
    assert list(_chain) == ["second", "source"]


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
@pytest.mark.dependency(name="shared", depends=["push"])
def test_push_data_products_of_one_source(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # One file registered under two names is two data products of one source,
    # which a registry may record as one external object for both: each is
    # pushed as a data product
    _names = ["shared/source/one", "shared/source/two"]
    _data_dir = os.path.join(os.path.dirname(__file__), "data")
    _cfg = {
        "run_metadata": {
            "description": "Two data products of one source",
            "script": "echo done",
        },
        "register": [
            {"namespace": "PSU", "full_name": "Pennsylvania State University"}
        ]
        + [
            {
                "external_object": _name,
                "namespace_name": "PSU",
                "root": f"file://{_data_dir}{os.path.sep}",
                "path": "test1.csv",
                "title": "Two data products of one source",
                "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
                "file_type": "csv",
                "release_date": "2021-09-20T12:00",
                "version": "1.0.0",
                "primary": False,
            }
            for _name in _names
        ],
    }
    _cfg_path = os.path.join(tmp_path, "shared.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(_cfg, f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])
        assert _res.exit_code == 0
        for _name in _names:
            _res = _cli_runner.invoke(cli, ["add", f"PSU:{_name}@v1.0.0"])
            assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["push", "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0
        for _name in _names:
            assert get(
                "http://127.0.0.1:8001/api/",
                "data_product",
                remote_registry._token,
                params={"name": _name, "version": "1.0.0"},
            )


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="no_file", depends=["push"])
def test_push_code_run_that_read_a_data_product_without_a_file(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # A data product may stand for something that has no file of its own, as
    # a deposit of several files does: its object has no storage location.
    # It is pushed with a code run that read it, and is that run's input there
    _name = "deposit/whole"
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _object = fdp_req.post(
            _ORIGIN, "object", _token, {"description": "A deposit of files"}
        )
        _data_product = fdp_req.post(
            _ORIGIN,
            "data_product",
            _token,
            {
                "namespace": get(
                    _ORIGIN, "namespace", _token, params={"name": "testing"}
                )[0]["url"],
                "name": _name,
                "version": "1.0.0",
                "object": _object["url"],
            },
        )
        fdp_req.post(
            _ORIGIN,
            "external_object",
            _token,
            {
                "data_product": _data_product["url"],
                "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
                "title": "A deposit of files",
                "primary_not_supplement": True,
                "release_date": "2021-09-20T12:00:00Z",
            },
        )
        # A run that read it, with the configuration and script of the one
        # the model made
        _model_run = get(_ORIGIN, "code_run", _token)[0]
        _whole = fdp_req.url_get(_object["url"], _token)["components"]
        _uuid = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "code_run",
            _token,
            {
                "run_date": _model_run["run_date"],
                "description": "A run that read the deposit",
                "model_config": _model_run["model_config"],
                "submission_script": _model_run["submission_script"],
                "code_repo": _model_run["code_repo"],
                "inputs": _whole,
                "uuid": _uuid,
            },
        )

        _res = _cli_runner.invoke(cli, ["add", _uuid])
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["push", "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0

        _token = remote_registry._token
        _pushed = get(
            _DEST,
            "data_product",
            _token,
            params={"name": _name, "version": "1.0.0"},
        )
        assert _pushed
        _pushed_object = fdp_req.url_get(_pushed[0]["object"], _token)
        assert not _pushed_object["storage_location"]
        _pushed_run = get(_DEST, "code_run", _token, params={"uuid": _uuid})
        assert _pushed_run[0]["inputs"] == _pushed_object["components"]


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="same_file", depends=["push"])
def test_push_code_run_that_read_one_data_product_of_a_shared_file(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # One file registered under two names is two data products of one file.
    # A code run that read one of them and wrote something new has that one
    # alone as its input when it is pushed, whichever was pushed first
    _names = ["same/file/first", "same/file/second"]
    with open(os.path.join(tmp_path, "same.csv"), "wb") as out_f:
        out_f.write(b"one,file\nunder,two names\n")
    _cfg = {
        "run_metadata": {
            "description": "One file under two names",
            "script": "echo done",
        },
        "register": [
            {"namespace": "PSU", "full_name": "Pennsylvania State University"}
        ]
        + [
            {
                "external_object": _name,
                "namespace_name": "PSU",
                "root": f"file://{tmp_path}{os.path.sep}",
                "path": "same.csv",
                "title": "One file under two names",
                "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
                "file_type": "csv",
                "release_date": "2021-09-20T12:00",
                "version": "1.0.0",
                "primary": False,
            }
            for _name in _names
        ],
    }
    _cfg_path = os.path.join(tmp_path, "same.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(_cfg, f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _res = _cli_runner.invoke(cli, ["pull", _cfg_path, "--debug"])
        assert _res.exit_code == 0
        # Each is pushed by itself, so that the first is the older there
        for _name in _names:
            _res = _cli_runner.invoke(cli, ["add", f"PSU:{_name}@v1.0.0"])
            assert _res.exit_code == 0
            _res = _cli_runner.invoke(
                cli, ["push", "--debug"], catch_exceptions=True
            )
            assert _res.exit_code == 0

        _token = local_registry._token
        _read = get(
            _ORIGIN,
            "data_product",
            _token,
            params={"name": _names[0], "version": "1.0.0"},
        )
        _read_object = fdp_req.url_get(_read[0]["object"], _token)
        _location = fdp_req.url_get(_read_object["storage_location"], _token)
        _root = fdp_req.url_get(_location["storage_root"], _token)

        # A new output in the data store, and a run that read the first name
        # and wrote it, with the configuration and script of the model's run
        _written = b"written by the run that read the first\n"
        _hash = hashlib.sha1(_written).hexdigest()
        _path = f"testing/same/file/output/{_hash}.csv"
        _file = f'{_root["root"]}{_path}'.replace("file://", "")
        os.makedirs(os.path.dirname(_file), exist_ok=True)
        with open(_file, "wb") as out_f:
            out_f.write(_written)
        _written_location = fdp_req.post(
            _ORIGIN,
            "storage_location",
            _token,
            {
                "path": _path,
                "hash": _hash,
                "public": True,
                "storage_root": _root["url"],
            },
        )
        _written_object = fdp_req.post(
            _ORIGIN,
            "object",
            _token,
            {
                "description": "Written by the run that read the first",
                "storage_location": _written_location["url"],
            },
        )
        fdp_req.post(
            _ORIGIN,
            "data_product",
            _token,
            {
                "namespace": get(
                    _ORIGIN, "namespace", _token, params={"name": "testing"}
                )[0]["url"],
                "name": "same/file/output",
                "version": "1.0.0",
                "object": _written_object["url"],
            },
        )
        _model_run = get(_ORIGIN, "code_run", _token)[0]
        _written_object = fdp_req.url_get(_written_object["url"], _token)
        _uuid = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "code_run",
            _token,
            {
                "run_date": _model_run["run_date"],
                "description": "A run that read the first name",
                "model_config": _model_run["model_config"],
                "submission_script": _model_run["submission_script"],
                "code_repo": _model_run["code_repo"],
                "inputs": _read_object["components"],
                "outputs": _written_object["components"],
                "uuid": _uuid,
            },
        )

        _res = _cli_runner.invoke(cli, ["add", _uuid])
        assert _res.exit_code == 0
        _res = _cli_runner.invoke(
            cli, ["push", "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0

        _token = remote_registry._token
        _pushed = get(
            _DEST,
            "data_product",
            _token,
            params={"name": _names[0], "version": "1.0.0"},
        )
        _pushed_object = fdp_req.url_get(_pushed[0]["object"], _token)
        _pushed_run = get(_DEST, "code_run", _token, params={"uuid": _uuid})
        assert _pushed_run[0]["inputs"] == _pushed_object["components"]


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="pull_placed", depends=["push"])
def test_pull_records_a_file_where_it_is_put(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # What a push from another machine leaves on a remote: a file in its
    # store, here under two names. Pulled, each is recorded in the local
    # registry at the file's place in the local data store, which is where a
    # model is sent to read it, and the one file is fetched once
    _names = ["elsewhere/first", "elsewhere/second"]
    _made = b"made,elsewhere\n1,2\n"
    _hash = hashlib.sha1(_made).hexdigest()
    _file = os.path.join(tmp_path, "elsewhere.csv")
    with open(_file, "wb") as out_f:
        out_f.write(_made)
    _cfg = {
        "run_metadata": {
            "description": "What was made elsewhere",
            "script": "echo done",
        },
        "read": [
            {
                "data_product": _name,
                "use": {"namespace": "testing", "version": "1.0.0"},
            }
            for _name in _names
        ],
    }
    _cfg_path = os.path.join(tmp_path, "elsewhere.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(_cfg, f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = remote_registry._token
        fdp_store.upload_remote_file(_file, _DEST, _token)
        _remote_location = fdp_req.post(
            _DEST,
            "storage_location",
            _token,
            {
                "path": _hash,
                "hash": _hash,
                "public": True,
                "storage_root": fdp_sync._remote_data_store_url(_DEST, _token),
            },
        )
        _namespace = get(
            _DEST, "namespace", _token, params={"name": "testing"}
        )[0]
        for _name in _names:
            _object = fdp_req.post(
                _DEST,
                "object",
                _token,
                {
                    "description": "Made elsewhere",
                    "storage_location": _remote_location["url"],
                },
            )
            fdp_req.post(
                _DEST,
                "data_product",
                _token,
                {
                    "namespace": _namespace["url"],
                    "name": _name,
                    "version": "1.0.0",
                    "object": _object["url"],
                },
            )

        _res = _cli_runner.invoke(
            cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0

        _token = local_registry._token
        _files = set()
        for _name in _names:
            _pulled = get(
                _ORIGIN,
                "data_product",
                _token,
                params={"name": _name, "version": "1.0.0"},
            )
            _object = fdp_req.url_get(_pulled[0]["object"], _token)
            _location = fdp_req.url_get(_object["storage_location"], _token)
            _root = fdp_req.url_get(_location["storage_root"], _token)["root"]
            assert _root.startswith("file://")
            _files.add(f'{_root}{_location["path"]}'.replace("file://", ""))
        assert len(_files) == 1
        with open(_files.pop(), "rb") as in_f:
            assert in_f.read() == _made


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
