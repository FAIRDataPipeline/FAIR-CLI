import json
import string
import typing
import os
import hashlib

import pytest
import pytest_mock
import yaml

import fair.exceptions as fdp_exc
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


@pytest.mark.faircli_storage
def test_presence_of_a_data_product_without_a_file(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # A data product may have no file, its object having no storage
    # location: one of the name is present, and matches no file
    _data_file = os.path.join(tmp_path, "data.csv")
    with open(_data_file, "w") as out_f:
        out_f.write("a,b\n1,2\n")
    _data_products = [{"object": "object", "version": "1.0.0"}]
    mocker.patch(
        "fair.registry.requests.get", lambda *args, **kwargs: _data_products
    )

    def mock_url_get(url, *args, **kwargs):
        assert url == "object"
        return {"storage_location": None}

    mocker.patch("fair.registry.requests.url_get", mock_url_get)

    assert (
        fdp_store.check_if_object_exists(
            local_uri=LOCAL_URL,
            file_loc=_data_file,
            obj_type="data_product",
            search_data={"name": "deposit/whole", "version": "1.0.0"},
            token="",
        )
        == _data_products
    )


@pytest.mark.faircli_storage
@pytest.mark.parametrize(
    "held",
    ["the same file", "another file", "no file", "older registry", None],
)
def test_source_is_one_file(mocker: pytest_mock.MockerFixture, tmp_path, held):
    # A source - an identifier, a title and a release version - is one file,
    # whatever data products hold it. A second file under its identity is
    # refused; the same file again is not, nor is a data product with no file
    # in the way. A registry from before sources were shared lists no data
    # products for one, and there nothing is compared
    _data_file = os.path.join(tmp_path, "data.csv")
    with open(_data_file, "wb") as out_f:
        out_f.write(b"a,b\n1,2\n")
    _hash = fdp_store.calculate_file_hash(_data_file)
    _entry = {
        "identifier": "https://doi.org/10.1038/s41592-020-0856-2",
        "title": "A source",
    }
    _sources = {
        None: [],
        "older registry": [{"data_product": "data_product"}],
    }.get(held, [{"data_products": ["data_product"]}])
    _records = {
        "data_product": {
            "object": "object",
            "namespace": "namespace",
            "name": "held/already",
            "version": "1.0.0",
        },
        "object": {"storage_location": held != "no file" and "location"},
        "location": {"hash": _hash if held == "the same file" else "0" * 40},
        "namespace": {"name": "PSU"},
    }
    _get = mocker.patch("fair.registry.requests.get", return_value=_sources)
    mocker.patch(
        "fair.registry.requests.url_get",
        lambda url, *args, **kwargs: _records[url],
    )

    def _check():
        fdp_store.check_source_is_one_file(
            LOCAL_URL, _data_file, "", _entry, "new/name"
        )

    if held == "another file":
        with pytest.raises(
            fdp_exc.UserConfigError, match="PSU:held/already@v1.0.0"
        ):
            _check()
    else:
        _check()
    assert _get.call_args.args[1] == "external_object"
    assert _get.call_args.kwargs["params"] == dict(_entry, version="1.0.0")


@pytest.mark.faircli_storage
def test_source_is_looked_up_by_its_identity(
    mocker: pytest_mock.MockerFixture,
):
    # By a unique name and its type where there is no identifier, and by the
    # release version the entry gives. An entry with no title has no identity
    # to look up, and is refused later for want of one
    _get = mocker.patch("fair.registry.requests.get", return_value=[])
    _entry = {
        "unique_name": "a source",
        "alternate_identifier_type": "local source descriptor",
        "release_version": "2.1.0",
    }

    fdp_store.check_source_is_one_file(LOCAL_URL, "", "", _entry, "name")
    _get.assert_not_called()

    fdp_store.check_source_is_one_file(
        LOCAL_URL, "", "", dict(_entry, title="A source"), "name"
    )
    assert _get.call_args.kwargs["params"] == {
        "alternate_identifier": "a source",
        "alternate_identifier_type": "local source descriptor",
        "title": "A source",
        "version": "2.1.0",
    }


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


# The storage roots and locations of a registry, with its rules on what may
# be recorded twice, in place of the calls that reach one
class _Storage:
    _unique = {
        "storage_root": ("root",),
        "storage_location": ("storage_root", "hash", "public"),
    }

    def __init__(self, mocker: pytest_mock.MockerFixture):
        self.rows = {"storage_root": [], "storage_location": []}
        mocker.patch("fair.registry.requests.post", self.post)
        mocker.patch("fair.registry.requests.get", self.get)
        mocker.patch("fair.registry.requests.url_get", self.url_get)

    def post(self, uri, obj_path, token, data, headers=None):
        if any(
            all(
                str(row[key]).lower() == str(data[key]).lower()
                for key in self._unique[obj_path]
            )
            for row in self.rows[obj_path]
        ):
            raise fdp_exc.RegistryAPICallError("exists", error_code=409)
        _row = {
            **data,
            "url": f"{uri}/{obj_path}/{len(self.rows[obj_path]) + 1}/",
        }
        if obj_path == "storage_location":
            _row["public"] = str(data["public"]).lower() == "true"
        # Newest first, as a registry lists them
        self.rows[obj_path].insert(0, _row)
        return _row

    def get(self, uri, obj_path, token, params=None, **kwargs):
        def _matches(row, key, value):
            if key == "storage_root":
                return row[key].endswith(f"/storage_root/{value}/")
            return row[key] == value

        return [
            row
            for row in self.rows[obj_path]
            if all(_matches(row, *item) for item in (params or {}).items())
        ]

    def url_get(self, url, token=None):
        return next(
            row
            for rows in self.rows.values()
            for row in rows
            if row["url"] == url
        )


@pytest.fixture
def stored_file(mocker: pytest_mock.MockerFixture, tmp_path):
    """A file in a data store, with the registry's records of where it is"""
    _storage = _Storage(mocker)
    _file = tmp_path / "1.0.0.nc"
    _file.write_bytes(b"a year of ERA5")
    _root_url = _storage.post(
        LOCAL_URL,
        "storage_root",
        "",
        {"root": f"file://{tmp_path}/", "local": True},
    )["url"]

    def _location_url():
        return fdp_store._get_url_from_storage_loc(
            local_file=str(_file),
            registry_uri=LOCAL_URL,
            registry_token="",
            relative_path="1.0.0.nc",
            root_store_url=_root_url,
            is_public=True,
        )

    return _storage, _location_url


@pytest.mark.faircli_storage
@pytest.mark.parametrize("first_is_there", [True, False])
def test_copy_of_a_file_held_already_is_not_kept(
    mocker: pytest_mock.MockerFixture, tmp_path, first_is_there: bool
):
    # One file registered under two names is recorded once under the data
    # store's root, at the first name's path, which is then the file of both:
    # the copy brought in for the second is removed, and its directory with
    # it. Not if the first is no longer there
    _storage = _Storage(mocker)
    _root_url = _storage.post(
        LOCAL_URL,
        "storage_root",
        "",
        {"root": f"file://{tmp_path}/", "local": True},
    )["url"]
    _files = []
    for _name in ("first", "second"):
        _file = tmp_path / "PSU" / _name / "1.0.0.csv"
        _file.parent.mkdir(parents=True)
        _file.write_bytes(b"a,b\n1,2\n")
        _files.append(_file)

    def _register(file):
        _location_url = fdp_store._get_url_from_storage_loc(
            local_file=str(file),
            registry_uri=LOCAL_URL,
            registry_token="",
            relative_path=os.path.relpath(file, tmp_path),
            root_store_url=_root_url,
            is_public=True,
        )
        fdp_store._remove_copy_held_elsewhere(
            str(file), str(tmp_path), _location_url, ""
        )

    _register(_files[0])
    assert _files[0].exists()
    if not first_is_there:
        _files[0].unlink()

    _register(_files[1])
    assert len(_storage.rows["storage_location"]) == 1
    assert _files[1].exists() != first_is_there
    assert _files[1].parent.exists() != first_is_there


_SOURCE = {"root": "https://example.org/data/", "path": "era5/1940.nc"}


@pytest.mark.faircli_storage
def test_original_store(stored_file, caplog):
    _storage, _location_url = stored_file
    _stored_url = _location_url()

    _original_url = fdp_store._get_url_from_original_store(
        dict(_SOURCE), LOCAL_URL, "", _stored_url, True
    )

    # Where the file came from is recorded beside where it is kept, as the
    # same file under another root
    _stored, _original = map(_storage.url_get, (_stored_url, _original_url))
    assert _original_url != _stored_url
    assert (_original["path"], _original["hash"]) == (
        "era5/1940.nc",
        _stored["hash"],
    )
    assert _storage.url_get(_original["storage_root"]) == {
        "root": "https://example.org/data/",
        "local": False,
        "url": _original["storage_root"],
    }

    def _warnings():
        return [
            record.getMessage()
            for record in caplog.records
            if record.levelname == "WARNING"
        ]

    # A file is recorded once under a root. The same file at another path
    # there is given the record it has, and that is said
    assert not _warnings()
    assert _original_url == fdp_store._get_url_from_original_store(
        {**_SOURCE, "path": "era5/copy_of_1940.nc"},
        LOCAL_URL,
        "",
        _stored_url,
        True,
    )
    assert "already recorded at 'https://example.org/data/era5/1940.nc'" in (
        _warnings()[0]
    )
    # A private file is another record
    assert _original_url != fdp_store._get_url_from_original_store(
        dict(_SOURCE), LOCAL_URL, "", _stored_url, False
    )

    # Its record in the data store is still found as that, not as the newer
    # record of the same file elsewhere
    assert _location_url() == _stored_url


@pytest.mark.faircli_storage
@pytest.mark.parametrize(
    "source",
    [
        {},
        {"root": "https://example.org/data/"},
        # A file beside the registry itself, not one fetched from elsewhere
        {"root": "/data/", "path": "era5/1940.nc"},
        # A file on this machine, where nobody else could fetch it
        {"root": "file:///home/me/downloads/", "path": "era5/1940.nc"},
    ],
)
def test_no_original_store(stored_file, source):
    _storage, _location_url = stored_file
    _stored_url = _location_url()

    assert (
        fdp_store._get_url_from_original_store(
            source, LOCAL_URL, "", _stored_url, True
        )
        is None
    )
    assert len(_storage.rows["storage_location"]) == 1


@pytest.mark.faircli_storage
@pytest.mark.parametrize(
    "original_store_url", [f"{LOCAL_URL}/storage_location/2/", None]
)
def test_external_object_names_its_original_store(
    mocker: pytest_mock.MockerFixture, original_store_url
):
    _access = mocker.patch("fair.registry.requests._access")
    fdp_store._get_url_from_external_obj(
        data={
            "title": "A year of ERA5",
            "primary": False,
            "release_date": "2026-01-01T00:00:00",
            "unique_name": "era5/1940",
        },
        local_file="1.0.0.nc",
        registry_uri=LOCAL_URL,
        registry_token="",
        data_product_url=f"{LOCAL_URL}/data_product/1/",
        original_store_url=original_store_url,
    )
    _posted = json.loads(_access.call_args.kwargs["data"])
    assert _posted.get("original_store") == original_store_url


@pytest.mark.faircli_storage
@pytest.mark.parametrize("release_version", ["2.1.0", None])
def test_external_object_release_version(
    mocker: pytest_mock.MockerFixture, release_version
):
    # The version of the source is sent when the entry gives one; without it
    # none is, and the registry applies its own
    _access = mocker.patch("fair.registry.requests._access")
    _data = {
        "title": "A year of ERA5",
        "primary": False,
        "release_date": "2026-01-01T00:00:00",
        "identifier": "https://doi.org/10.24381/cds.f17050d7",
    }
    if release_version:
        _data["release_version"] = release_version
    fdp_store._get_url_from_external_obj(
        data=_data,
        local_file="1.0.0.nc",
        registry_uri=LOCAL_URL,
        registry_token="",
        data_product_url=f"{LOCAL_URL}/data_product/1/",
    )
    _posted = json.loads(_access.call_args.kwargs["data"])
    assert _posted.get("version") == release_version


@pytest.mark.faircli_storage
def test_external_object_named_without_an_identifier(
    mocker: pytest_mock.MockerFixture,
):
    # With no identifier, a registry wants a name and what kind of name it is
    _access = mocker.patch("fair.registry.requests._access")
    fdp_store._get_url_from_external_obj(
        data={
            "title": "A year of ERA5",
            "primary": False,
            "release_date": "2026-01-01T00:00:00",
            "unique_name": "ERA5 monthly means, 1940",
            "alternate_identifier_type": "extract of a dataset",
        },
        local_file="1.0.0.nc",
        registry_uri=LOCAL_URL,
        registry_token="",
        data_product_url=f"{LOCAL_URL}/data_product/1/",
    )
    _posted = json.loads(_access.call_args.kwargs["data"])
    assert _posted["alternate_identifier"] == "ERA5 monthly means, 1940"
    assert _posted["alternate_identifier_type"] == "extract of a dataset"
    assert "identifier" not in _posted


@pytest.mark.faircli_storage
@pytest.mark.parametrize(
    "status,sent", [(200, True), (409, False), (403, None)]
)
def test_upload_remote_file(
    mocker: pytest_mock.MockerFixture, tmp_path, status: int, sent
):
    # A registry gives an address to upload a file to, refuses one (409) for
    # a file its store already holds, and may refuse for other reasons
    _file = tmp_path / "1.0.0.nc"
    _file.write_bytes(b"a year of ERA5")
    _response = mocker.Mock(status_code=status)
    _response.json.return_value = {"url": "http://example.org/upload"}
    _asked = mocker.patch("requests.post", return_value=_response)
    _put = mocker.patch("fair.registry.requests.put_file")

    if sent is None:
        with pytest.raises(fdp_exc.RegistryAPICallError):
            fdp_store.upload_remote_file(str(_file), f"{LOCAL_URL}/", "token")
    else:
        fdp_store.upload_remote_file(str(_file), f"{LOCAL_URL}/", "token")

    # Asked for by the file's hash, and sent only to an address given
    assert _asked.call_args.args[0] == (
        f"{LOCAL_URL}/data/{fdp_store.calculate_file_hash(str(_file))}"
    )
    assert _put.called is bool(sent)
