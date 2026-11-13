import collections
import hashlib
import json
import os
import pathlib
import tempfile
import typing
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
@pytest.mark.parametrize(
    "caches,cache",
    [
        (None, None),
        (
            {"testing:test@v1.0.0": "mine.csv", "testing:other@v1.0.0": "no.csv"},
            "mine.csv",
        ),
    ],
)
def test_sync_data_products_fetches_the_data_product(
    mocker: pytest_mock.MockerFixture, caches, cache
):
    """Pulling passes the data product itself, not the external object, and
    the user's copy of its file where there is one"""
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
            return {
                "url": "object",
                "storage_location": "storage_location",
                "components": [],
            }
        elif url == "storage_location":
            return {"public": True}
        # The external object, which replaces the data product in `result`
        return {"url": "external_object_url"}

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    mocker.patch(
        "fair.registry.sync.sync_dependency_chain",
        lambda **kwargs: {"object": "dest_object"},
    )
    mocker.patch("fair.registry.sync.sync_attached")
    mocker.patch("fair.registry.sync._has_file", return_value=True)
    _fetch = mocker.patch("fair.registry.sync._place_pulled_file")

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:test@v1.0.0"],
        local_data_store="/data/store",
        caches=caches,
    )

    _fetch.assert_called_once_with("dest", "", "", "/data/store", _data_product, cache)


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
        lambda **kwargs: _moved.append("records") or {"object": "dest_object"},
    )
    mocker.patch("fair.registry.sync.sync_attached")
    mocker.patch("fair.registry.sync._has_file", return_value=True)
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
@pytest.mark.parametrize("cache", ["the file", "another file", "missing", "held"])
def test_fetch_data_product_from_a_cache(
    mocker: pytest_mock.MockerFixture, tmp_path, cache
):
    # The user's copy of the file stands in for the download, if it is the
    # file the registry describes, and is left as it is. A file the data
    # store holds already is the data product's, the copy not read
    _made = b"made,elsewhere\n1,2\n"
    _records = {
        "object": {"storage_location": "storage_location"},
        "storage_location": {
            "path": "data.csv",
            "hash": hashlib.sha1(_made).hexdigest(),
            "storage_root": "storage_root",
        },
        "storage_root": {"root": "file:///elsewhere/"},
        "namespace": {"name": "testing"},
    }
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )
    mocker.patch(
        "fair.registry.sync.download_from_registry",
        side_effect=AssertionError("the file was downloaded"),
    )
    _data_product = {
        "version": "2.3.0",
        "namespace": "namespace",
        "name": "test",
        "object": "object",
    }
    _copy = os.path.join(tmp_path, "copy.csv")
    _copied = b"another,file\n" if cache == "another file" else _made
    if cache != "missing":
        with open(_copy, "wb") as out_f:
            out_f.write(_copied)
    _store = os.path.join(tmp_path, "store")
    _out_file = os.path.join(_store, "testing", "test", "2.3.0")
    if cache == "held":
        os.remove(_copy)
        os.makedirs(os.path.dirname(_out_file))
        with open(_out_file, "wb") as out_f:
            out_f.write(_made)

    def _fetch():
        return fdp_sync.fetch_data_product("", _store, _data_product, cache=_copy)

    if cache in ("another file", "missing"):
        with pytest.raises(
            fdp_exc.SynchronisationError,
            match="has hash" if cache == "another file" else "is not a file",
        ):
            _fetch()
        assert not os.path.exists(_store)
    else:
        assert _fetch() == _out_file
        assert open(_out_file, "rb").read() == _made
        assert os.listdir(os.path.dirname(_out_file)) == ["2.3.0"]
    if cache in ("the file", "another file"):
        assert open(_copy, "rb").read() == _copied


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
        side_effect=lambda token, store, data_product, out_file, cache=None: (
            out_file or _fetched
        ),
    )
    _data_product = {"object": "object"}

    _placed = fdp_sync._place_pulled_file(
        _local, "", "", _store, _data_product, cache="mine.csv"
    )

    assert _get.call_args.args[:2] == (_local, "storage_location")
    assert _get.call_args.kwargs["params"] == {
        "hash": "abc",
        "storage_root": fdp_req.get_obj_id_from_url(_root_url),
    }
    if held == "the file":
        _held_file = os.path.join(_store, _held_location["path"])
        _fetch.assert_called_once_with(
            "", _store, _data_product, _held_file, cache="mine.csv"
        )
        _post.assert_not_called()
        assert _placed == {"location": _held_location["url"]}
    else:
        _fetch.assert_called_once_with(
            "", _store, _data_product, None, cache="mine.csv"
        )
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


@pytest.mark.faircli_sync
def test_dependency_chain_with_a_code_repo_release(
    mocker: pytest_mock.MockerFixture,
):
    # A release of a code repository points at its object, and nothing in the
    # object's chain points at it: it is synchronised after the object, named
    # by the object's new URL
    _local, _remote = _ORIGIN, _DEST
    _object = f"{_local}object/5/"
    _release = f"{_local}code_repo_release/2/"
    _rows = {
        _object: {
            "description": "A repository",
            "code_repo_release": _release,
        },
        _release: {"object": _object, "name": "model", "version": "1.0.0"},
    }
    mocker.patch(
        "fair.registry.sync.get_dependency_chain",
        lambda *args: collections.deque([_object]),
    )
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, token=None: dict(_rows[url]),
    )
    mocker.patch(
        "fair.registry.requests.get_obj_type_from_url",
        lambda url, token=None: url.split("/")[-3],
    )
    mocker.patch(
        "fair.registry.requests.get_writable_fields",
        lambda *args: ["description", "object", "name", "version"],
    )
    _synced = mocker.patch(
        "fair.registry.sync._get_new_url",
        side_effect=lambda **kwargs: kwargs["object_url"].replace(
            _local, _remote
        ),
    )

    _new_urls = fdp_sync.sync_dependency_chain(
        object_url=_object,
        dest_uri=_remote,
        origin_uri=_local,
        dest_token="",
        origin_token="token",
    )

    assert _new_urls == {
        _object: f"{_remote}object/5/",
        _release: f"{_remote}code_repo_release/2/",
    }
    assert [c.kwargs["object_url"] for c in _synced.call_args_list] == [
        _object,
        _release,
    ]
    _release_call = _synced.call_args_list[1].kwargs
    assert _release_call["writable_data"]["object"] == _object
    assert _release_call["new_urls"][_object] == f"{_remote}object/5/"


# An object row's storage location, for the tests of what has a file to move
_AT_LOCATION = {"storage_location": "location"}


@pytest.mark.faircli_sync
@pytest.mark.parametrize(
    "row, runs, expected",
    [
        ({"storage_location": None}, None, False),
        (_AT_LOCATION | {"code_repo_release": "release"}, None, False),
        (_AT_LOCATION | {"code_repo_release": None}, ["run"], False),
        (_AT_LOCATION | {"code_repo_release": None}, [], True),
    ],
)
def test_has_file(
    mocker: pytest_mock.MockerFixture, row, runs, expected: bool
):
    # An object has a file to move unless it has no storage location or is a
    # code repository: one with a release, or one a code run names. The code
    # runs are asked for only when nothing else has decided
    _asked = []

    def mock_get(uri, obj, token, params=None):
        _asked.append((uri.rstrip("/"), obj, params))
        return runs

    mocker.patch("fair.registry.requests.get", mock_get)
    _object = {"url": f"{_ORIGIN}object/5/", **row}
    assert fdp_sync._has_file(_object, "token") is expected
    if runs is None:
        assert not _asked
    else:
        assert _asked == [
            (_ORIGIN.rstrip("/"), "code_run", {"code_repo": "5"})
        ]


@pytest.mark.faircli_sync
@pytest.mark.parametrize("local_data_store", [None, "/data/store"])
def test_sync_data_product_on_a_code_repo(
    mocker: pytest_mock.MockerFixture, local_data_store
):
    # A data product on a code repository's object has a location and no file:
    # nothing is uploaded or fetched, and its location is not moved into the
    # remote's data store
    _data_product = {
        "url": "data_product_url",
        "name": "release",
        "version": "1.0.0",
        "namespace": "namespace",
        "object": "object",
        "external_object": None,
    }

    def mock_get(uri, obj, *args, **kwargs):
        if obj == "namespace":
            return [{"url": "http://example/api/namespace/1/"}]
        elif obj == "data_product":
            return [] if uri == "dest" else [_data_product]

    def mock_url_get(url, *args, **kwargs):
        assert url == "object"
        return {"url": "object", "components": [], **_AT_LOCATION}

    # The destination holds no object for the repository's commit
    mocker.patch("fair.registry.sync._dest_repository", return_value=None)
    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)
    mocker.patch("fair.registry.sync._has_file", return_value=False)
    _records = mocker.patch("fair.registry.sync.sync_dependency_chain")
    mocker.patch("fair.registry.sync.sync_attached")
    _upload = mocker.patch("fair.registry.sync.upload_object")
    _place = mocker.patch("fair.registry.sync._place_pulled_file")

    fdp_sync.sync_data_products(
        origin_uri="origin",
        dest_uri="dest",
        dest_token="",
        origin_token="",
        remote_label="origin",
        data_products=["testing:release@v1.0.0"],
        local_data_store=local_data_store,
    )

    _upload.assert_not_called()
    _place.assert_not_called()
    assert _records.call_args.kwargs["public"] is False
    assert _records.call_args.kwargs["placed"] == {}


@pytest.mark.faircli_sync
def test_sync_code_run_pushes_its_code_repo_as_it_is(
    mocker: pytest_mock.MockerFixture,
):
    # A run's code repository is pushed without moving its location into the
    # remote's data store, as a public file's would be
    _run = {
        "uuid": "abc",
        "run_date": "2026-10-17T00:00:00Z",
        "description": "A run",
        "model_config": "config",
        "submission_script": "script",
        "code_repo": "repo",
    }
    # The destination holds no object for the repository's commit
    mocker.patch("fair.registry.sync._repository_placed", return_value={})
    mocker.patch(
        "fair.registry.requests.get",
        lambda uri, obj, *args, **kwargs: [] if uri == "dest" else [_run],
    )
    mocker.patch("fair.registry.sync.upload_object")
    mocker.patch(
        "fair.registry.sync.get_dest_object_url", lambda url, *args: url
    )
    mocker.patch("fair.registry.requests.post", return_value={})
    _records = mocker.patch("fair.registry.sync.sync_dependency_chain")
    mocker.patch("fair.registry.sync.sync_attached")

    fdp_sync.sync_code_run("origin", "dest", "", "", "abc")

    _public = {
        c.kwargs["object_url"]: c.kwargs["public"]
        for c in _records.call_args_list
    }
    assert _public == {"config": True, "repo": False, "script": True}


@pytest.mark.faircli_sync
def test_sync_code_run_patches_each_run_with_its_own_links(
    mocker: pytest_mock.MockerFixture,
):
    # Two runs on the destination already, each given an input and no
    # outputs: each is patched with its own outputs alone
    def mock_get(uri, obj, token, params=None, **kwargs):
        _uuid = params["uuid"]
        _objects = {"model_config": "config", "submission_script": "script"}
        if uri == "origin":
            return [{"uuid": _uuid, "code_repo": None, **_objects}]
        return [
            {
                "url": f"dest/{_uuid}",
                "inputs": [],
                "outputs": [f"output of {_uuid}"],
                "code_repo": None,
                **_objects,
            }
        ]

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.sync.sync_attached")
    _patch = mocker.patch("fair.registry.requests._access")

    for _uuid in ("a", "b"):
        fdp_sync.sync_code_run("origin", "dest", "", "", _uuid, inputs=[f"in {_uuid}"])

    _patched = {c.args[0]: c.kwargs["data"] for c in _patch.call_args_list}
    assert _patched == {
        f"dest/{_uuid}": {"inputs": [f"in {_uuid}"], "outputs": [f"output of {_uuid}"]}
        for _uuid in ("a", "b")
    }


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
    # A file is not a code repository
    mocker.patch("fair.registry.sync._dest_repository", return_value=None)
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
    # A file is not a code repository
    mocker.patch("fair.registry.sync._dest_repository", return_value=None)
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
    mocker.patch("fair.registry.sync.sync_attached")
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
    # The machine's first initialisation, so the file's settings become its
    # global ones: the fixture's placeholder is emptied, which counts as none
    open(fdp_com.global_fdpconfig(), "w").close()
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
@pytest.mark.dependency(name="release", depends=["push"])
def test_push_code_run_of_a_released_code_repo(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # A run whose code repository is an official release of the code is pushed
    # with the release, on the remote's object for the repository
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _model_run = get(_ORIGIN, "code_run", _token)[0]
        fdp_req.post(
            _ORIGIN,
            "code_repo_release",
            _token,
            {
                "object": _model_run["code_repo"],
                "name": "pySimpleModel",
                "version": "0.1.0",
                "website": "https://github.com/FAIRDataPipeline/pySimpleModel",
            },
        )
        # A run of its own, since a run on the remote already is not
        # synchronised again
        _uuid = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "code_run",
            _token,
            {
                "run_date": _model_run["run_date"],
                "description": "A run of the released code",
                "model_config": _model_run["model_config"],
                "submission_script": _model_run["submission_script"],
                "code_repo": _model_run["code_repo"],
                "inputs": _model_run["inputs"],
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
        _pushed_run = get(_DEST, "code_run", _token, params={"uuid": _uuid})
        _pushed_repo = fdp_req.url_get(_pushed_run[0]["code_repo"], _token)
        assert _pushed_repo["code_repo_release"]
        _release = fdp_req.url_get(_pushed_repo["code_repo_release"], _token)
        assert _release["object"] == _pushed_repo["url"]
        assert (_release["name"], _release["version"]) == (
            "pySimpleModel",
            "0.1.0",
        )
        assert _release["website"] == (
            "https://github.com/FAIRDataPipeline/pySimpleModel"
        )


@pytest.mark.faircli_sync
@pytest.mark.dependency(name="on_code_repo", depends=["push"])
def test_push_and_pull_a_data_product_on_a_code_repo(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # A data product may be a code repository's object - a release of the
    # code, registered with its DOI as its source - whose location is the
    # repository at a commit. It is pushed and pulled with that location as
    # it is, and nothing is downloaded: the repository's address is a web page
    _cli_runner = click.testing.CliRunner()
    _download = mocker.spy(fdp_sync, "download_from_registry")
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _repo = get(_ORIGIN, "code_run", _token)[0]["code_repo"]
        _repo_location = fdp_req.url_get(
            fdp_req.url_get(_repo, _token)["storage_location"], _token
        )
        _namespace = get(
            _ORIGIN, "namespace", _token, params={"name": "testing"}
        )
        fdp_req.post(
            _ORIGIN,
            "data_product",
            _token,
            {
                "namespace": _namespace[0]["url"],
                "name": "release/pushed",
                "version": "1.0.0",
                "object": _repo,
            },
        )

        _res = _cli_runner.invoke(
            cli, ["add", "testing:release/pushed@v1.0.0"]
        )
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
            params={"name": "release/pushed", "version": "1.0.0"},
        )
        _remote_repo = fdp_req.url_get(_pushed[0]["object"], _token)
        _location = fdp_req.url_get(_remote_repo["storage_location"], _token)
        assert _location["hash"] == _repo_location["hash"]
        assert _location["path"] == _repo_location["path"]
        _root = fdp_req.url_get(_location["storage_root"], _token)
        assert _root["root"] == "https://github.com/"

        # A data product made on the remote, on its object for the repository
        _namespace = get(
            _DEST, "namespace", _token, params={"name": "testing"}
        )
        fdp_req.post(
            _DEST,
            "data_product",
            _token,
            {
                "namespace": _namespace[0]["url"],
                "name": "release/pulled",
                "version": "1.0.0",
                "object": _remote_repo["url"],
            },
        )
        _cfg_path = os.path.join(tmp_path, "release.yaml")
        with open(_cfg_path, "w") as f:
            yaml.dump(
                {
                    "run_metadata": {
                        "description": "Reading a release",
                        "script": "echo done",
                    },
                    "read": [
                        {
                            "data_product": "release/pulled",
                            "use": {
                                "namespace": "testing",
                                "version": "1.0.0",
                            },
                        }
                    ],
                },
                f,
                sort_keys=False,
            )
        _res = _cli_runner.invoke(
            cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0, _res.output

        _token = local_registry._token
        _pulled = get(
            _ORIGIN,
            "data_product",
            _token,
            params={"name": "release/pulled", "version": "1.0.0"},
        )
        _object = fdp_req.url_get(_pulled[0]["object"], _token)
        _location = fdp_req.url_get(_object["storage_location"], _token)
        assert _location["hash"] == _repo_location["hash"]
        _root = fdp_req.url_get(_location["storage_root"], _token)
        assert _root["root"] == "https://github.com/"

    _download.assert_not_called()


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


@pytest.mark.faircli_sync
def test_ci_remote_user_is_on_the_remote(remote_registry: RegistryTest, tmp_path):
    # 'fair init --ci' names the user the remote's token belongs to, which a
    # push then finds there; a fresh remote has no 'FAIRDataPipeline'
    import fair.session

    _config = fdp_test.create_configurations(
        str(tmp_path), str(tmp_path), str(tmp_path), str(tmp_path)
    )
    _config["registries"]["origin"]["uri"] = remote_registry._url
    with open(_config["registries"]["origin"]["token"], "w") as token_f:
        token_f.write(remote_registry._token)
    with remote_registry:
        _user = fair.session.FAIR._ci_remote_user(_config)
        assert _user != "FAIRDataPipeline"
        assert fdp_req.get(
            remote_registry._url,
            "users",
            remote_registry._token,
            params={"username": _user},
        )


# A licence's identifier, as the registry holds one
_LICENCE = "https://creativecommons.org/licenses/by/4.0/"

# A source as an external object's record gives it
_SOURCE = {
    "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
    "alternate_identifier": None,
    "alternate_identifier_type": None,
    "title": "A deposit",
    "version": "1.0.0",
}


# Stand-ins for two registries, 'here' and 'there', each holding the data
# product 'PSU:x@v1.0.0' with the contents given - ("file", hash), ("source",
# record) or ("nothing",) - or None for no such data product
def _two_registries(
    mocker: pytest_mock.MockerFixture, contents: typing.Dict[str, typing.Any]
) -> None:
    def mock_get(uri, obj, token=None, params=None, **kwargs):
        if obj == "namespace":
            return [{"url": f"http://{uri}/api/namespace/1/"}]
        _held = contents[uri]
        if obj != "data_product" or _held is None:
            return []
        return [
            {
                "url": f"http://{uri}/api/data_product/1/",
                "object": f"http://{uri}/api/object/1/",
                "external_object": (
                    f"http://{uri}/api/external_object/1/"
                    if _held[0] == "source"
                    else None
                ),
            }
        ]

    def mock_url_get(url, token=None, **kwargs):
        _held = contents[url.split("/")[2]]
        if "/object/" in url:
            return {
                "storage_location": (
                    url.replace("object", "storage_location")
                    if _held[0] == "file"
                    else None
                )
            }
        if "/storage_location/" in url:
            return {"hash": _held[1]}
        return _held[1]

    mocker.patch("fair.registry.requests.get", mock_get)
    mocker.patch("fair.registry.requests.url_get", mock_url_get)


@pytest.mark.faircli_sync
@pytest.mark.parametrize(
    "here, there, refused",
    [
        (("file", "a" * 40), ("file", "a" * 40), False),
        (("file", "a" * 40), ("file", "b" * 40), True),
        (("file", "a" * 40), ("nothing",), True),
        (("file", "a" * 40), None, False),
        (("source", _SOURCE), ("source", dict(_SOURCE)), False),
        (("source", _SOURCE), ("source", {**_SOURCE, "title": "Another"}), True),
        (("source", _SOURCE), ("source", {**_SOURCE, "version": "2.0.0"}), True),
        (("source", _SOURCE), ("nothing",), True),
        (("nothing",), ("nothing",), False),
    ],
)
def test_a_data_product_held_with_other_contents_is_refused(
    mocker: pytest_mock.MockerFixture,
    here: typing.Any,
    there: typing.Any,
    refused: bool,
):
    _two_registries(mocker, {"here": here, "there": there})
    _arguments = ("here", "there", "", "", ["PSU:x@v1.0.0"], "origin")
    if not refused:
        fdp_sync.refuse_conflicts(*_arguments)
        return
    with pytest.raises(fdp_exc.RegistryError, match="Nothing was pushed") as e:
        fdp_sync.refuse_conflicts(*_arguments)
    assert "'PSU:x@v1.0.0'" in e.value.msg
    assert "a version the remote does not have" in e.value.hint


@pytest.mark.faircli_sync
def test_a_refusal_names_each_side(mocker: pytest_mock.MockerFixture):
    _two_registries(mocker, {"here": ("file", "a" * 40), "there": ("file", "b" * 40)})
    _arguments = ("here", "there", "", "", ["PSU:x@v1.0.0"], "origin")
    with pytest.raises(fdp_exc.RegistryError) as e:
        fdp_sync.refuse_conflicts(*_arguments)
    assert f"on 'origin' a file with hash {'b' * 40}" in e.value.msg
    assert f"in the local registry a file with hash {'a' * 40}" in e.value.msg
    # Pulled, 'here' is the remote and 'there' the local registry
    with pytest.raises(fdp_exc.RegistryError, match="Nothing was pulled") as e:
        fdp_sync.refuse_conflicts(*_arguments, pulling=True)
    assert f"on 'origin' a file with hash {'a' * 40}" in e.value.msg
    assert f"in the local registry a file with hash {'b' * 40}" in e.value.msg
    assert "'fair registry install --directory" in e.value.hint
    assert "a data store of its own" in e.value.hint


@pytest.mark.faircli_sync
def test_a_conflict_is_refused_by_sync_whatever_force_says(
    mocker: pytest_mock.MockerFixture,
):
    _two_registries(mocker, {"here": ("file", "a" * 40), "there": ("file", "b" * 40)})
    _records = mocker.patch("fair.registry.sync.sync_dependency_chain")
    with pytest.raises(fdp_exc.RegistryError, match="Nothing was pushed"):
        fdp_sync.sync_data_products(
            origin_uri="here",
            dest_uri="there",
            dest_token="",
            origin_token="",
            remote_label="origin",
            data_products=["PSU:x@v1.0.0"],
            force=True,
        )
    _records.assert_not_called()


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["init"])
def test_push_and_pull_refuse_a_name_held_with_another_file(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # The remote holds a data product with one file; the local registry
    # registers another file under the same namespace, name and version
    _name = f"conflict/{uuid.uuid4().hex[:8]}"
    _theirs = os.path.join(tmp_path, "theirs.csv")
    _ours = os.path.join(tmp_path, "ours.csv")
    with open(_theirs, "wb") as out_f:
        out_f.write(b"the remote's,file\n")
    with open(_ours, "wb") as out_f:
        out_f.write(b"another,file\n")
    _their_hash = fdp_store.calculate_file_hash(_theirs)
    _our_hash = fdp_store.calculate_file_hash(_ours)
    _cfg = {
        "run_metadata": {"description": "Another file", "script": "echo done"},
        "register": [
            {"namespace": "PSU", "full_name": "Pennsylvania State University"},
            {
                "external_object": _name,
                "namespace_name": "PSU",
                "root": f"file://{tmp_path}{os.path.sep}",
                "path": "ours.csv",
                "title": f"Another file {_name}",
                "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
                "file_type": "csv",
                "release_date": "2021-09-20T12:00",
                "version": "1.0.0",
                "primary": False,
            },
        ],
    }
    _cfg_path = os.path.join(tmp_path, "ours.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(_cfg, f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = remote_registry._token
        _root = fdp_req.post_else_get(
            _DEST,
            "storage_root",
            _token,
            data={"root": f"file://{tmp_path}{os.path.sep}", "local": False},
        )
        _location = fdp_req.post(
            _DEST,
            "storage_location",
            _token,
            data={
                "path": "theirs.csv",
                "hash": _their_hash,
                "public": True,
                "storage_root": _root,
            },
        )
        _object = fdp_req.post(
            _DEST,
            "object",
            _token,
            data={"description": "The remote's", "storage_location": _location["url"]},
        )
        _namespace = fdp_req.post_else_get(
            _DEST, "namespace", _token, data={"name": "PSU"}, params={"name": "PSU"}
        )
        fdp_req.post(
            _DEST,
            "data_product",
            _token,
            data={
                "name": _name,
                "version": "1.0.0",
                "namespace": _namespace,
                "object": _object["url"],
            },
        )

        # Registered here: a pull fetches nothing it registers
        _res = _cli_runner.invoke(cli, ["pull", _cfg_path])
        assert _res.exit_code == 0, _res.output

        _res = _cli_runner.invoke(cli, ["add", f"PSU:{_name}@v1.0.0"])
        assert _res.exit_code == 0, _res.output
        _res = _cli_runner.invoke(cli, ["push"])
        assert _res.exit_code != 0
        assert "Nothing was pushed" in _res.output, _res.output
        assert _their_hash in _res.output and _our_hash in _res.output
        # Left staged, it would be refused in every later test's push
        _res = _cli_runner.invoke(cli, ["unstage", f"PSU:{_name}@v1.0.0"])
        assert _res.exit_code == 0, _res.output

        # Read from there, it is refused too
        _read_path = os.path.join(tmp_path, "read.yaml")
        with open(_read_path, "w") as f:
            yaml.dump(
                {
                    "run_metadata": {"description": "Reads it", "script": "echo"},
                    "read": [
                        {
                            "data_product": _name,
                            "use": {"namespace": "PSU", "version": "1.0.0"},
                        }
                    ],
                },
                f,
            )
        _res = _cli_runner.invoke(cli, ["pull", _read_path])
        assert _res.exit_code != 0
        assert "Nothing was pulled" in _res.output, _res.output
        assert _their_hash in _res.output and _our_hash in _res.output

        # The remote's is as it was
        _held = get(_DEST, "data_product", _token, params={"name": _name})
        assert len(_held) == 1
        _held_object = fdp_req.url_get(_held[0]["object"], _token)
        assert (
            fdp_req.url_get(_held_object["storage_location"], _token)["hash"]
            == _their_hash
        )


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["init"])
@pytest.mark.parametrize("cache", [None, "the file", "another file"])
def test_pull_a_private_data_product_from_a_copy_of_its_file(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
    cache,
):
    # A private data product's file is not sent to the remote, which records
    # it where it was made, on another machine, so a plain read cannot fetch
    # it. Read from the user's own copy, its 'cache', it is pulled as any
    # other read, if the copy is the file the remote describes
    _name = f"private/{uuid.uuid4().hex[:8]}"
    _made = f"made,elsewhere\n{_name},2\n".encode()
    _hash = hashlib.sha1(_made).hexdigest()
    _copy = os.path.join(tmp_path, "copy.csv")
    _copied = b"another,file\n" if cache == "another file" else _made
    with open(_copy, "wb") as out_f:
        out_f.write(_copied)
    _use = {"namespace": "testing", "version": "1.0.0"}
    if cache:
        _use["cache"] = _copy
    _cfg_path = os.path.join(tmp_path, "private.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(
            {
                "run_metadata": {"description": "Reads it", "script": "echo"},
                "read": [{"data_product": _name, "use": _use}],
            },
            f,
        )

    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = remote_registry._token
        _root = fdp_req.post_else_get(
            _DEST,
            "storage_root",
            _token,
            data={"root": "file:///elsewhere/data_store/", "local": False},
        )
        _location = fdp_req.post(
            _DEST,
            "storage_location",
            _token,
            data={
                "path": f"{_name}.csv",
                "hash": _hash,
                "public": False,
                "storage_root": _root,
            },
        )
        _object = fdp_req.post(
            _DEST,
            "object",
            _token,
            data={
                "description": "Made elsewhere",
                "storage_location": _location["url"],
            },
        )
        _namespace = fdp_req.post_else_get(
            _DEST,
            "namespace",
            _token,
            data={"name": "testing"},
            params={"name": "testing"},
        )
        fdp_req.post(
            _DEST,
            "data_product",
            _token,
            data={
                "name": _name,
                "version": "1.0.0",
                "namespace": _namespace,
                "object": _object["url"],
            },
        )

        _res = _cli_runner.invoke(cli, ["pull", _cfg_path])

        _token = local_registry._token
        _pulled = get(
            _ORIGIN,
            "data_product",
            _token,
            params={"name": _name, "version": "1.0.0"},
        )
        if cache == "the file":
            assert _res.exit_code == 0, _res.output
            assert len(_pulled) == 1
            _object = fdp_req.url_get(_pulled[0]["object"], _token)
            _location = fdp_req.url_get(_object["storage_location"], _token)
            assert _location["hash"] == _hash
            assert _location["public"] is False
            _root = fdp_req.url_get(_location["storage_root"], _token)["root"]
            assert _root.startswith("file://")
            _file = f'{_root}{_location["path"]}'.replace("file://", "")
            with open(_file, "rb") as in_f:
                assert in_f.read() == _made
        else:
            assert _res.exit_code != 0
            _why = "has hash" if cache else "Failed to download"
            assert _why in _res.output, _res.output
            assert not _pulled
        # The copy is the user's, and is left as it was
        with open(_copy, "rb") as in_f:
            assert in_f.read() == _copied


@pytest.mark.faircli_sync
def test_a_file_missing_from_the_destination_is_named(
    mocker: pytest_mock.MockerFixture,
):
    _urls = {
        "origin_object": {"storage_location": "origin_location"},
        "origin_location": {"hash": "c" * 40},
    }
    # A file is not a code repository
    mocker.patch("fair.registry.sync._dest_repository", return_value=None)
    mocker.patch(
        "fair.registry.requests.url_get", lambda url, *args, **kwargs: _urls[url]
    )
    mocker.patch("fair.registry.requests.get", lambda *args, **kwargs: [])
    with pytest.raises(fdp_exc.RegistryError, match="c" * 40) as e:
        fdp_sync.get_dest_object_url("origin_object", "dest", "", "")
    assert "'fair add" in e.value.hint


# A data product registered in the origin registry by the API, with a file in
# the data store; returns its object, as the registry gives it
def _registered_data_product(
    token: str, name: str, contents: bytes
) -> typing.Dict[str, typing.Any]:
    _root = next(
        _root
        for _root in get(_ORIGIN, "storage_root", token)
        if _root["local"] and _root["root"].startswith("file://")
    )
    _hash = hashlib.sha1(contents).hexdigest()
    _path = f"testing/{name}/{_hash}.csv"
    _file = f'{_root["root"]}{_path}'.replace("file://", "")
    os.makedirs(os.path.dirname(_file), exist_ok=True)
    with open(_file, "wb") as out_f:
        out_f.write(contents)
    _location = fdp_req.post(
        _ORIGIN,
        "storage_location",
        token,
        {"path": _path, "hash": _hash, "public": True, "storage_root": _root["url"]},
    )
    _object = fdp_req.post(
        _ORIGIN,
        "object",
        token,
        {"description": f"The file of {name}", "storage_location": _location["url"]},
    )
    fdp_req.post(
        _ORIGIN,
        "data_product",
        token,
        {
            "namespace": get(_ORIGIN, "namespace", token, params={"name": "testing"})[
                0
            ]["url"],
            "name": name,
            "version": "1.0.0",
            "object": _object["url"],
        },
    )
    return fdp_req.url_get(_object["url"], token)


# What a registry holds that points at a data product's object: its named
# components, licences, keywords, key-values and the issues on its components
def _held_with(uri: str, token: str, name: str) -> typing.Dict[str, typing.Any]:
    _product = get(uri, "data_product", token, params={"name": name})[0]
    _object = fdp_req.url_get(_product["object"], token)
    _components = {
        _component["name"]: _component
        for _component in (
            fdp_req.url_get(_url, token) for _url in _object["components"]
        )
    }
    _issues = {}
    for _component in _components.values():
        for _url in _component["issues"]:
            _issue = fdp_req.url_get(_url, token)
            _issues[_issue["uuid"]] = (
                _issue["severity"],
                _issue["description"],
                sorted(
                    fdp_req.url_get(_c, token)["name"]
                    for _c in _issue["component_issues"]
                ),
            )
    _of_object = {"object": fdp_req.get_obj_id_from_url(_object["url"])}
    return {
        "components": {
            _name: _component["description"]
            for _name, _component in _components.items()
            if not _component["whole_object"]
        },
        "licences": sorted(
            (_l["identifier"] or "", _l["licence_info"])
            for _l in get(uri, "licence", token, params=_of_object)
        ),
        "keywords": sorted(
            _k["keyphrase"] for _k in get(uri, "keyword", token, params=_of_object)
        ),
        "key_values": sorted(
            (_kv["key"], _kv["value"])
            for _kv in get(uri, "key_value", token, params=_of_object)
        ),
        "issues": _issues,
        "component_urls": {_n: _c["url"] for _n, _c in _components.items()},
    }


# Records attached to an object, as an API writes them: two named
# components, two licences (one with no identifier), a keyword, a key-value
# and an issue on the first component; returns the components' URLs by name
# and the issue's UUID
def _attach_records(
    uri: str, token: str, object_url: str
) -> typing.Tuple[typing.Dict[str, str], str]:
    _parts = {
        _part: fdp_req.post(
            uri,
            "object_component",
            token,
            {"object": object_url, "name": _part, "description": f"The {_part}"},
        )["url"]
        for _part in ("first", "second")
    }
    for _record, _data in (
        ("licence", {"licence_info": "CC BY 4.0", "identifier": _LICENCE}),
        ("licence", {"licence_info": "Only for this test"}),
        ("keyword", {"keyphrase": "ecology"}),
        ("key_value", {"key": "scale", "value": "50km"}),
    ):
        fdp_req.post(uri, _record, token, {"object": object_url, **_data})
    _issue = str(uuid.uuid4())
    fdp_req.post(
        uri,
        "issue",
        token,
        {
            "severity": 2,
            "description": "The first part is wrong",
            "component_issues": [_parts["first"]],
            "uuid": _issue,
        },
    )
    return _parts, _issue


# The records _attach_records makes, as _held_with reports them
def _attached(issue: str) -> typing.Dict[str, typing.Any]:
    return {
        "components": {"first": "The first", "second": "The second"},
        "licences": [("", "Only for this test"), (_LICENCE, "CC BY 4.0")],
        "keywords": ["ecology"],
        "key_values": [("scale", "50km")],
        "issues": {issue: (2, "The first part is wrong", ["first"])},
    }


# A code run registered in the origin registry with the model run's
# configuration and script; returns its UUID
def _registered_run(
    token: str,
    inputs: typing.List[str],
    outputs: typing.List[str],
    code_repo: str = None,
) -> str:
    _model_run = get(_ORIGIN, "code_run", token)[0]
    _run = str(uuid.uuid4())
    fdp_req.post(
        _ORIGIN,
        "code_run",
        token,
        {
            "run_date": _model_run["run_date"],
            "description": "A run",
            "model_config": _model_run["model_config"],
            "submission_script": _model_run["submission_script"],
            "code_repo": code_repo or _model_run["code_repo"],
            "inputs": inputs,
            "outputs": outputs,
            "uuid": _run,
        },
    )
    return _run


# Stage each item and push them, leaving none staged, whatever the push did,
# so that a failed push is not tried again by a later test's
def _push(cli_runner: click.testing.CliRunner, *items: str) -> click.testing.Result:
    for _item in items:
        _res = cli_runner.invoke(cli, ["add", _item])
        assert _res.exit_code == 0, _res.output
    _pushed = cli_runner.invoke(cli, ["push"])
    for _item in items:
        _res = cli_runner.invoke(cli, ["unstage", _item])
        assert _res.exit_code == 0, _res.output
    return _pushed


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_push_sends_what_points_at_what_it_sends(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    caplog,
):
    # A data product written in two named parts, with a licence, a keyword, a
    # key-value and an issue on one part, and a run that read that part
    _name = f"attached/{uuid.uuid4().hex[:8]}"
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _object = _registered_data_product(_token, _name, b"in,two\nparts,here\n")
        _parts, _issue = _attach_records(_ORIGIN, _token, _object["url"])
        _output = _registered_data_product(_token, f"{_name}_read", b"read the first\n")
        _run = _registered_run(_token, [_parts["first"]], _output["components"])
        # Its document reaches the remote after it does, in the same push
        fdp_req.post(
            _ORIGIN,
            "quality_controlled",
            _token,
            {"object": _object["url"], "document": _output["url"]},
        )

        _product = f"testing:{_name}@v1.0.0"
        _res = _push(_cli_runner, _product, _run)
        assert _res.exit_code == 0, _res.output
        _there = _held_with(_DEST, remote_registry._token, _name)
        _arrived = {_key: _there[_key] for _key in _there if _key != "component_urls"}
        _pushed_run = get(
            _DEST, "code_run", remote_registry._token, params={"uuid": _run}
        )
        _arrived["run_inputs"] = (
            _pushed_run[0]["inputs"] if _pushed_run else "the run is not there"
        )
        assert _arrived == {
            **_attached(_issue),
            "run_inputs": [_there["component_urls"].get("first")],
        }
        _remote_objects = [
            get(_DEST, "data_product", remote_registry._token, params={"name": _n})[0][
                "object"
            ]
            for _n in (_name, f"{_name}_read")
        ]
        _checked = get(
            _DEST,
            "quality_controlled",
            remote_registry._token,
            params={"object": fdp_req.get_obj_id_from_url(_remote_objects[0])},
        )
        assert [_c["document"] for _c in _checked] == [_remote_objects[1]]

        # Pushed again, each is there once
        _res = _push(_cli_runner, _product, _run)
        assert _res.exit_code == 0, _res.output
        assert _held_with(_DEST, remote_registry._token, _name) == _there

        # Records attached since are sent. The issue, changed here since it was
        # sent, and a key-value the remote holds with another value are kept as
        # they are there
        fdp_req.post(
            _ORIGIN, "keyword", _token, {"object": _object["url"], "keyphrase": "maps"}
        )
        _here = get(_ORIGIN, "issue", _token, params={"uuid": _issue})[0]
        fdp_req._access(
            _here["url"],
            "patch",
            _token,
            data={"severity": 3, "description": "The first part is very wrong"},
        )
        fdp_req.post(
            _ORIGIN,
            "key_value",
            _token,
            {"object": _object["url"], "key": "grid", "value": "here"},
        )
        _remote_object = get(
            _DEST, "data_product", remote_registry._token, params={"name": _name}
        )[0]["object"]
        fdp_req.post(
            _DEST,
            "key_value",
            remote_registry._token,
            {"object": _remote_object, "key": "grid", "value": "there"},
        )
        _res = _push(_cli_runner, _product)
        assert _res.exit_code == 0, _res.output
        _now = _held_with(_DEST, remote_registry._token, _name)
        assert _now["keywords"] == ["ecology", "maps"]
        assert _now["issues"] == _there["issues"]
        assert _now["key_values"] == [("grid", "there"), ("scale", "50km")]
        assert f"The issue {_issue} on '{_DEST}' is kept as it is there" in caplog.text
        assert "The key_value of" in caplog.text


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_push_sends_a_run_by_itself_with_what_it_read(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # A run that read one part of a file in two parts, of a code repository
    # with an issue on it, staged by itself; then a release of its code
    # registered after it was pushed
    _name = f"read_alone/{uuid.uuid4().hex[:8]}"
    _commit = hashlib.sha1(_name.encode()).hexdigest()
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _object = _registered_data_product(_token, _name, b"one,of\ntwo,parts\n")
        _parts, _ = _attach_records(_ORIGIN, _token, _object["url"])
        _output = _registered_data_product(_token, f"{_name}_out", b"read one\n")
        _repo_root = fdp_req.post_else_get(
            _ORIGIN,
            "storage_root",
            _token,
            data={"root": "https://github.com/", "local": False},
            params={"root": "https://github.com/"},
        )
        _repo = fdp_req.post(
            _ORIGIN,
            "object",
            _token,
            {
                "description": "Remote code repository.",
                "storage_location": fdp_req.post(
                    _ORIGIN,
                    "storage_location",
                    _token,
                    {
                        "path": f"FAIRDataPipeline/{_commit[:8]}",
                        "hash": _commit,
                        "public": True,
                        "storage_root": _repo_root,
                    },
                )["url"],
            },
        )
        _repo_issue = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "issue",
            _token,
            {
                "severity": 1,
                "description": "Run from uncommitted changes",
                "component_issues": _repo["components"],
                "uuid": _repo_issue,
            },
        )
        _run = _registered_run(
            _token, [_parts["first"]], _output["components"], _repo["url"]
        )

        _res = _push(_cli_runner, _run)
        assert _res.exit_code == 0, _res.output
        _remote_token = remote_registry._token
        _pushed_run = get(_DEST, "code_run", _remote_token, params={"uuid": _run})[0]
        _there = _held_with(_DEST, _remote_token, _name)
        assert _pushed_run["inputs"] == [_there["component_urls"]["first"]]
        _pushed_repo = fdp_req.url_get(_pushed_run["code_repo"], _remote_token)
        _issue_there = get(_DEST, "issue", _remote_token, params={"uuid": _repo_issue})
        assert _issue_there[0]["component_issues"] == _pushed_repo["components"]
        assert not _pushed_repo["code_repo_release"]

        fdp_req.post(
            _ORIGIN,
            "code_repo_release",
            _token,
            {
                "object": _repo["url"],
                "name": _name,
                "version": "1.0.0",
                "website": "https://github.com/FAIRDataPipeline",
            },
        )
        _res = _push(_cli_runner, _run)
        assert _res.exit_code == 0, _res.output
        _pushed_repo = fdp_req.url_get(_pushed_run["code_repo"], _remote_token)
        _release = fdp_req.url_get(_pushed_repo["code_repo_release"], _remote_token)
        assert (_release["name"], _release["version"]) == (_name, "1.0.0")


# Data products registered as one run's outputs, each with a file of its own,
# their whole-object components in order
def _written(token: str, name: str, count: int) -> typing.List[str]:
    return [
        _registered_data_product(
            token, f"{name}/{_i}", f"{name} {_i}\n".encode()
        )["components"][0]
        for _i in range(count)
    ]


# A code run as the destination holds it
def _run_there(token: str, run: str) -> typing.Dict[str, typing.Any]:
    return get(_DEST, "code_run", token, params={"uuid": run})[0]


# The data products a call of refuse_conflicts looked at
def _looked_at(call) -> typing.List[str]:
    if "data_products" in call.kwargs:
        return call.kwargs["data_products"]
    return call.args[4]


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_a_push_handles_each_data_product_and_run_once(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # A run that wrote twenty data products and a run that read them all,
    # pushed by the first. Each data product is checked once and sent once,
    # each run linked once, and no data product is sent inside the sending of
    # another. Calls are counted, not requests, so a call grown dearer passes
    _name = f"many/{uuid.uuid4().hex[:8]}"
    _count = 20
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _components = _written(_token, _name, _count)
        _writer = _registered_run(_token, [], _components)
        _reader = _registered_run(_token, _components, [])
        _checks = mocker.spy(fdp_sync, "refuse_conflicts")
        _links = mocker.spy(fdp_sync, "sync_code_run")
        _depth = {"now": 0, "most": 0}
        _sync_data_products = fdp_sync.sync_data_products

        def _nested(*args, **kwargs):
            _depth["now"] += 1
            _depth["most"] = max(_depth["most"], _depth["now"])
            try:
                return _sync_data_products(*args, **kwargs)
            finally:
                _depth["now"] -= 1

        mocker.patch("fair.registry.sync.sync_data_products", _nested)

        _res = _push(_cli_runner, _writer)
        assert _res.exit_code == 0, _res.output
        _checked = collections.Counter(
            _product
            for _call in _checks.call_args_list
            for _product in _looked_at(_call)
        )
        assert _checked == {
            f"testing:{_name}/{_i}@v1.0.0": 1 for _i in range(_count)
        }
        assert collections.Counter(
            _call.args[4] for _call in _links.call_args_list
        ) == {_writer: 1, _reader: 1}
        assert _depth["most"] == 1
        _remote_token = remote_registry._token
        _outputs = _run_there(_remote_token, _writer)["outputs"]
        assert len(_outputs) == _count
        assert sorted(_run_there(_remote_token, _reader)["inputs"]) == sorted(
            _outputs
        )


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_a_push_that_fails_links_what_it_sent(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # Three data products of one run, pushed without the run, the third's
    # file refused. The run is linked with the two sent; pushed again, with all
    # three
    _name = f"failed/{uuid.uuid4().hex[:8]}"
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _components = _written(_token, _name, 3)
        _writer = _registered_run(_token, [], _components)
        _refused = fdp_req.url_get(_components[2], _token)["object"]
        _upload_object = fdp_sync.upload_object

        def _refusing(*args):
            if args[4] == _refused:
                raise fdp_exc.SynchronisationError("Refused", error_code=1)
            return _upload_object(*args)

        mocker.patch("fair.registry.sync.upload_object", _refusing)
        _products = [f"testing:{_name}/{_i}@v1.0.0" for _i in range(3)]

        _res = _push(_cli_runner, *_products)
        assert _res.exit_code != 0, _res.output
        _remote_token = remote_registry._token
        assert len(_run_there(_remote_token, _writer)["outputs"]) == 2

        mocker.patch("fair.registry.sync.upload_object", _upload_object)
        _res = _push(_cli_runner, *_products)
        assert _res.exit_code == 0, _res.output
        assert len(_run_there(_remote_token, _writer)["outputs"]) == 3


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_push_sends_an_issue_on_two_data_products_pushed_apart(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    _names = [f"apart/{uuid.uuid4().hex[:8]}/{_n}" for _n in ("one", "two")]
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _objects = [
            _registered_data_product(_token, _name, _name.encode()) for _name in _names
        ]
        _issue = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "issue",
            _token,
            {
                "severity": 2,
                "description": "Both are wrong",
                "component_issues": sum((_o["components"] for _o in _objects), []),
                "uuid": _issue,
            },
        )

        _remote_token = remote_registry._token
        _wholes = []
        for _name in _names:
            _res = _push(_cli_runner, f"testing:{_name}@v1.0.0")
            assert _res.exit_code == 0, _res.output
            _wholes.append(
                _held_with(_DEST, _remote_token, _name)["component_urls"][
                    "whole_object"
                ]
            )
            _there = get(_DEST, "issue", _remote_token, params={"uuid": _issue})
            assert sorted(_there[0]["component_issues"]) == sorted(_wholes)


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_an_issue_raised_in_both_registries_is_not_doubled(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # The same issue - its severity and description - raised on a data product
    # in each registry separately, after the data product was pushed
    _name = f"raised_twice/{uuid.uuid4().hex[:8]}"
    _cfg_path = os.path.join(tmp_path, "read.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(
            {
                "run_metadata": {"description": "Reads it", "script": "echo"},
                "read": [
                    {
                        "data_product": _name,
                        "use": {"namespace": "testing", "version": "1.0.0"},
                    }
                ],
            },
            f,
        )
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _remote_token = remote_registry._token
        _object = _registered_data_product(_token, _name, _name.encode())
        _res = _push(_cli_runner, f"testing:{_name}@v1.0.0")
        assert _res.exit_code == 0, _res.output
        _issue = {"severity": 2, "description": "Raised in both"}
        _ours = str(uuid.uuid4())
        fdp_req.post(
            _ORIGIN,
            "issue",
            _token,
            {**_issue, "component_issues": _object["components"], "uuid": _ours},
        )
        _theirs = str(uuid.uuid4())
        _remote_whole = _held_with(_DEST, _remote_token, _name)["component_urls"][
            "whole_object"
        ]
        fdp_req.post(
            _DEST,
            "issue",
            _remote_token,
            {**_issue, "component_issues": [_remote_whole], "uuid": _theirs},
        )

        _res = _push(_cli_runner, f"testing:{_name}@v1.0.0")
        assert _res.exit_code == 0, _res.output
        assert list(_held_with(_DEST, _remote_token, _name)["issues"]) == [_theirs]

        _res = _cli_runner.invoke(cli, ["pull", _cfg_path])
        assert _res.exit_code == 0, _res.output
        assert list(_held_with(_ORIGIN, _token, _name)["issues"]) == [_ours]


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_pull_brings_what_points_at_what_it_pulls(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # A data product pushed from elsewhere with records attached to it
    _name = f"pulled_with/{uuid.uuid4().hex[:8]}"
    _made = _name.encode()
    _hash = hashlib.sha1(_made).hexdigest()
    _file = os.path.join(tmp_path, "made.csv")
    with open(_file, "wb") as out_f:
        out_f.write(_made)
    _cfg_path = os.path.join(tmp_path, "read.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(
            {
                "run_metadata": {"description": "Reads it", "script": "echo"},
                "read": [
                    {
                        "data_product": _name,
                        "use": {"namespace": "testing", "version": "1.0.0"},
                    }
                ],
            },
            f,
        )
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = remote_registry._token
        fdp_store.upload_remote_file(_file, _DEST, _token)
        _object = fdp_req.post(
            _DEST,
            "object",
            _token,
            {
                "description": "Made elsewhere",
                "storage_location": fdp_req.post(
                    _DEST,
                    "storage_location",
                    _token,
                    {
                        "path": _hash,
                        "hash": _hash,
                        "public": True,
                        "storage_root": fdp_sync._remote_data_store_url(_DEST, _token),
                    },
                )["url"],
            },
        )
        fdp_req.post(
            _DEST,
            "data_product",
            _token,
            {
                "namespace": fdp_req.post_else_get(
                    _DEST,
                    "namespace",
                    _token,
                    data={"name": "testing"},
                    params={"name": "testing"},
                ),
                "name": _name,
                "version": "1.0.0",
                "object": _object["url"],
            },
        )
        _, _issue = _attach_records(_DEST, _token, _object["url"])

        for _ in range(2):
            _res = _cli_runner.invoke(cli, ["pull", _cfg_path])
            assert _res.exit_code == 0, _res.output
            _here = _held_with(_ORIGIN, local_registry._token, _name)
            del _here["component_urls"]
            assert _here == _attached(_issue)


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_push_warns_of_a_run_the_remote_lacks(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
):
    # A run read a data product another run wrote; the remote holds that data
    # product, sent before its run was, and not the run
    _name = f"behind/{uuid.uuid4().hex[:8]}"
    _contents = _name.encode()
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _token = local_registry._token
        _read = _registered_data_product(_token, _name, _contents)
        _out = _registered_data_product(_token, f"{_name}_out", b"made from it\n")
        _res = _push(_cli_runner, f"testing:{_name}@v1.0.0")
        assert _res.exit_code == 0, _res.output
        _writer = _registered_run(_token, [], _read["components"])
        _reader = _registered_run(_token, _read["components"], _out["components"])

        _res = _push(_cli_runner, _reader)
        assert _res.exit_code == 0, _res.output
        assert (
            f"Warning: the remote 'origin' does not hold code run {_writer}, which "
            f"wrote testing:{_name}@v1.0.0. 'fair add {_writer}' and 'fair push' "
            "would send it."
        ) in _res.output

        _res = _push(_cli_runner, _writer, _reader)
        assert _res.exit_code == 0, _res.output
        assert "does not hold code run" not in _res.output


@pytest.mark.faircli_sync
@pytest.mark.parametrize(
    "objects,expected",
    [([(10, None), (9, None)], 9), ([(9, None), (10, "a release")], 10), ([], None)],
)
def test_dest_repository_is_the_released_object_else_the_oldest(
    mocker: pytest_mock.MockerFixture, objects, expected
):
    # The destination's objects at a commit, each as (id, release), one on each
    # of two locations whose paths differ, under one root
    _records = {
        "origin_location": {"hash": "c" * 40, "storage_root": "origin_root"},
        "origin_root": {"root": "https://github.com/"},
    }
    mocker.patch(
        "fair.registry.requests.url_get", lambda url, *args, **kwargs: _records[url]
    )
    mocker.patch("fair.registry.sync._has_file", return_value=False)
    _locations = [f"{_DEST}storage_location/{_n}/" for _n in (1, 2)]

    def mock_get(uri, obj, token, params=None, **kwargs):
        if obj == "storage_root":
            assert params == {"root": "https://github.com/"}
            return [{"url": f"{_DEST}storage_root/3/"}]
        if obj == "storage_location":
            assert params == {"hash": "c" * 40, "public": True, "storage_root": "3"}
            return [{"url": _url} for _url in _locations]
        _at = _locations.index(f"{_DEST}storage_location/{params['storage_location']}/")
        return [
            {
                "url": f"{_DEST}object/{_id}/",
                "storage_location": _locations[_at],
                "code_repo_release": _release,
            }
            for _n, (_id, _release) in enumerate(objects)
            if _n == _at
        ]

    mocker.patch("fair.registry.requests.get", mock_get)
    _held = fdp_sync._dest_repository(
        {"url": "origin_object", "storage_location": "origin_location"}, _DEST, "", ""
    )
    assert (_held and int(fdp_req.get_obj_id_from_url(_held["url"]))) == expected


@pytest.mark.faircli_sync
def test_dest_repository_of_an_object_with_a_file_is_none(
    mocker: pytest_mock.MockerFixture,
):
    mocker.patch("fair.registry.sync._has_file", return_value=True)
    _get = mocker.patch("fair.registry.requests.get")
    assert (
        fdp_sync._dest_repository(
            {"url": "origin_object", "storage_location": "origin_location"},
            _DEST,
            "",
            "",
        )
        is None
    )
    _get.assert_not_called()


# A code repository's object at a commit, as an API registers one, with a
# release of the given name if one is given; returns the object's record. A
# registry holds one location for a commit under a host, so a second object at
# the commit is made on it, whatever path is given
def _repository(
    uri: str, token: str, commit: str, path: str, release: str = None
) -> typing.Dict[str, typing.Any]:
    _root = fdp_req.post_else_get(
        uri,
        "storage_root",
        token,
        data={"root": "https://github.com/", "local": False},
        params={"root": "https://github.com/"},
    )
    _location = fdp_req.post_else_get(
        uri,
        "storage_location",
        token,
        data={"path": path, "hash": commit, "public": True, "storage_root": _root},
        params={"hash": commit, "storage_root": fdp_req.get_obj_id_from_url(_root)},
    )
    _object = fdp_req.post(
        uri,
        "object",
        token,
        {"description": "Remote code repository.", "storage_location": _location},
    )
    if release:
        fdp_req.post(
            uri,
            "code_repo_release",
            token,
            {
                "object": _object["url"],
                "name": release,
                "version": "1.0.0",
                "website": "https://github.com/FAIRDataPipeline",
            },
        )
    return fdp_req.url_get(_object["url"], token)


# The objects a registry holds at a commit, by URL
def _objects_at(uri: str, token: str, commit: str) -> typing.List[str]:
    return sorted(
        _object["url"]
        for _location in get(uri, "storage_location", token, params={"hash": commit})
        for _object in get(
            uri,
            "object",
            token,
            params={"storage_location": fdp_req.get_obj_id_from_url(_location["url"])},
        )
    )


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
@pytest.mark.parametrize("there", ["released", "same path", "unreleased", "two"])
def test_push_gives_a_run_the_remotes_object_for_its_commit(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    caplog,
    there: str,
):
    # Another machine's push left the remote an object for a commit - with the
    # release, at this registry's path or another, without the release, or two
    # objects, the newer with it; a run here of that commit and release is
    # pushed
    _release = f"repository-{uuid.uuid4().hex[:8]}"
    _commit = hashlib.sha1(_release.encode()).hexdigest()
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _remote_token = remote_registry._token
        _held = []
        if there == "two":
            _held.append(
                _repository(_DEST, _remote_token, _commit, "FAIRDataPipeline/older")
            )
        _held.append(
            _repository(
                _DEST,
                _remote_token,
                _commit,
                f"FAIRDataPipeline/{'here' if there == 'same path' else 'elsewhere'}",
                _release if there != "unreleased" else None,
            )
        )
        _token = local_registry._token
        _repo = _repository(_ORIGIN, _token, _commit, "FAIRDataPipeline/here", _release)
        _output = _registered_data_product(
            _token, f"made_at/{_release}", f"made at {_release}".encode()
        )
        _run = _registered_run(_token, [], _output["components"], _repo["url"])

        _res = _push(_cli_runner, _run)
        assert _res.exit_code == 0, _res.output
        _pushed = get(_DEST, "code_run", _remote_token, params={"uuid": _run})[0]
        assert _pushed["code_repo"] == _held[-1]["url"]
        assert _objects_at(_DEST, _remote_token, _commit) == sorted(
            _object["url"] for _object in _held
        )
        _object = fdp_req.url_get(_pushed["code_repo"], _remote_token)
        assert (
            fdp_req.url_get(_object["code_repo_release"], _remote_token)["name"]
            == _release
        )
        assert not [
            _record.getMessage()
            for _record in caplog.records
            if _record.levelname == "WARNING"
            and "code_repo_release" in _record.getMessage()
        ]


@pytest.mark.faircli_sync
@pytest.mark.dependency(depends=["run"])
def test_pull_gives_a_data_product_the_local_object_for_its_commit(
    global_config: str,
    local_registry: RegistryTest,
    remote_registry: RegistryTest,
    pyDataPipeline: str,
    fair_bucket: MotoTestServer,
    mocker: pytest_mock.MockerFixture,
    tmp_path,
):
    # A release registered on the remote as a data product on its repository's
    # object, at a commit the local registry holds an object for at another path
    _release = f"repository-{uuid.uuid4().hex[:8]}"
    _commit = hashlib.sha1(_release.encode()).hexdigest()
    _name = f"release/{_release}"
    _cfg_path = os.path.join(tmp_path, "release.yaml")
    with open(_cfg_path, "w") as f:
        yaml.dump(
            {
                "run_metadata": {"description": "Reading a release", "script": "echo"},
                "read": [
                    {
                        "data_product": _name,
                        "use": {"namespace": "testing", "version": "1.0.0"},
                    }
                ],
            },
            f,
        )
    _cli_runner = click.testing.CliRunner()
    with remote_registry, local_registry, fair_bucket:
        mocker.patch(
            "fair.configuration.get_current_user_remote_user",
            lambda *args, **kwargs: "admin",
        )
        _remote_token = remote_registry._token
        _theirs = _repository(
            _DEST, _remote_token, _commit, "FAIRDataPipeline/there", _release
        )
        fdp_req.post(
            _DEST,
            "data_product",
            _remote_token,
            {
                "namespace": fdp_req.post_else_get(
                    _DEST,
                    "namespace",
                    _remote_token,
                    data={"name": "testing"},
                    params={"name": "testing"},
                ),
                "name": _name,
                "version": "1.0.0",
                "object": _theirs["url"],
            },
        )
        _token = local_registry._token
        _ours = _repository(_ORIGIN, _token, _commit, "FAIRDataPipeline/here")

        _res = _cli_runner.invoke(cli, ["pull", _cfg_path])
        assert _res.exit_code == 0, _res.output
        _pulled = get(_ORIGIN, "data_product", _token, params={"name": _name})
        assert _pulled[0]["object"] == _ours["url"]
        assert _objects_at(_ORIGIN, _token, _commit) == [_ours["url"]]
        _object = fdp_req.url_get(_ours["url"], _token)
        assert fdp_req.url_get(_object["code_repo_release"], _token)["name"] == _release
