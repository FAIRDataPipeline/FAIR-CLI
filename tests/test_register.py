import glob
import os

import click.testing
import pytest
import yaml

from fair.cli import cli
import fair.common as fdp_com
import fair.exceptions as fdp_exc
import fair.register as fdp_reg
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.sync as fdp_sync
import fair.testing as fdp_test

TEST_DATA_DIR = f"file://{os.path.dirname(__file__)}{os.path.sep}data{os.path.sep}"

TEST_REGISTER_CFG = os.path.join(
    os.path.dirname(__file__), "data", "test_register.yaml"
)


@pytest.mark.faircli_register
@pytest.mark.parametrize("release_version", [2.1, "second"])
def test_register_release_version_not_a_version(mocker, release_version):
    # Refused before anything is fetched or registered
    mocker.patch(
        "fair.registry.sync.download_from_registry",
        side_effect=AssertionError("the file was fetched"),
    )
    _entry = {
        "external_object": "era5/1940",
        "use": {
            "data_product": "era5/1940",
            "namespace": "ECMWF",
            "version": "1.0.0",
        },
        "root": "https://example.com/",
        "path": "era5/1940.nc",
        "file_type": "nc",
        "primary": True,
        "public": True,
        "identifier": "https://doi.org/10.24381/cds.f17050d7",
        "release_version": release_version,
    }
    with pytest.raises(fdp_exc.UserConfigError, match="release_version"):
        fdp_reg.fetch_registrations(
            "http://127.0.0.1:8000/api/", "", "", [_entry]
        )


@pytest.mark.faircli_register
@pytest.mark.parametrize("cached", [False, True])
def test_register_refused_leaves_no_download(mocker, tmp_path, cached):
    # A file fetched for an entry that is then refused is not left behind;
    # one the entry was given from a cache is the user's, and stays
    _data_file = os.path.join(tmp_path, "data.csv")
    with open(_data_file, "w") as out_f:
        out_f.write("a,b\n1,2\n")
    mocker.patch(
        "fair.registry.sync.download_from_registry", return_value=_data_file
    )
    mocker.patch("fair.registry.requests.local_token", return_value="")
    mocker.patch(
        "fair.register.convert_key_value_to_id",
        side_effect=fdp_exc.RegistryError("no such namespace"),
    )
    mocker.patch(
        "fair.registry.storage.check_source_is_one_file",
        side_effect=fdp_exc.UserConfigError("another file"),
    )
    _entry = {
        "external_object": "era5/1940",
        "use": {
            "data_product": "era5/1940",
            "namespace": "ECMWF",
            "version": "1.0.0",
        },
        "root": "https://example.com/",
        "path": "era5/1940.nc",
        "file_type": "nc",
        "primary": True,
        "public": True,
        "identifier": "https://doi.org/10.24381/cds.f17050d7",
    }
    if cached:
        _entry["cache"] = _data_file
    with pytest.raises(fdp_exc.UserConfigError, match="another file"):
        fdp_reg.fetch_registrations(
            "http://127.0.0.1:8000/api/", "", str(tmp_path), [_entry]
        )
    assert os.path.exists(_data_file) == cached
    assert os.listdir(tmp_path) == (["data.csv"] if cached else [])


# A 'register' entry for a source with no file of its own, as written
_NO_FILE_ENTRY = {
    "external_object": "deposit/whole",
    "namespace_name": "ECMWF",
    "identifier": "https://doi.org/10.24381/cds.f17050d7",
    "title": "A deposit of files",
    "release_date": "2021-09-20T12:00",
    "primary": True,
    "version": "1.0.0",
    "no_file": True,
}


@pytest.mark.faircli_register
@pytest.mark.parametrize(
    "change, refused",
    [
        ({}, None),
        ({"root": "https://example.com/"}, "Unexpected key 'root'"),
        ({"path": "a.nc"}, "Unexpected key 'path'"),
        ({"file_type": "nc"}, "Unexpected key 'file_type'"),
        ({"cache": "a.nc"}, "Unexpected key 'cache'"),
        ({"use": {"cache": "a.nc"}}, "Unexpected key 'cache'"),
        ({"public": True}, "Unexpected key 'public'"),
        ({"version": None}, "Expected a version"),
        ({"version": "${{PATCH}}"}, "Expected a version"),
        ({"no_file": "yes"}, "Expected true or false"),
        ({"no_file": False}, None),
    ],
)
def test_check_no_file_entry(change, refused):
    _entry = {k: v for k, v in {**_NO_FILE_ENTRY, **change}.items() if v is not None}
    if refused:
        with pytest.raises(fdp_exc.UserConfigError, match=refused):
            fdp_reg.check_no_file_entry(_entry)
    else:
        fdp_reg.check_no_file_entry(_entry)


@pytest.mark.faircli_register
def test_check_no_file_entry_of_a_data_product():
    _entry = dict(_NO_FILE_ENTRY, data_product="deposit/whole")
    del _entry["external_object"]
    with pytest.raises(fdp_exc.UserConfigError, match="Only an 'external_object'"):
        fdp_reg.check_no_file_entry(_entry)


@pytest.mark.faircli_register
@pytest.mark.parametrize("registered", [False, True])
def test_register_no_file_fetches_nothing(mocker, tmp_path, registered):
    # Nothing is fetched or copied; the entry is registered unless it is there
    mocker.patch(
        "fair.registry.sync.download_from_registry",
        side_effect=AssertionError("a file was fetched"),
    )
    mocker.patch(
        "fair.registry.storage.check_source_is_one_file",
        side_effect=AssertionError("a file was compared"),
    )
    mocker.patch("fair.registry.requests.local_token", return_value="")
    mocker.patch("fair.register.convert_key_value_to_id", return_value=3)
    mocker.patch(
        "fair.registry.storage.check_registered_without_file",
        return_value=registered,
    )
    _store = mocker.patch(
        "fair.registry.storage.store_registration_without_file",
        return_value={"url": "external_object_url"},
    )
    _entry = dict(_NO_FILE_ENTRY)
    _entry["use"] = {
        "data_product": "deposit/whole",
        "namespace": "ECMWF",
        "version": _entry.pop("version"),
    }

    _stored = fdp_reg.fetch_registrations(
        "http://127.0.0.1:8000/api/", "", str(tmp_path), [_entry]
    )

    assert os.listdir(tmp_path) == []
    if registered:
        _store.assert_not_called()
        assert _stored == []
    else:
        assert _store.call_args.kwargs["data"]["namespace_name"] == "ECMWF"
        assert _stored == [{"url": "external_object_url"}]


@pytest.mark.faircli_register
@pytest.mark.parametrize(
    "version, present, outcome",
    [
        ("${{PATCH}}", "hash_match", "passed over"),
        ("${{PATCH}}", "results", "0.0.3"),
        ("${{PATCH}}", "absent", "0.0.1"),
        ("${{ LATEST }}", "hash_match", "passed over"),
        ("${{ LATEST }}", "results", "has changed"),
        ("${{ LATEST }}", "absent", "no version of it"),
        ("2.0.0", "absent", "2.0.0"),
    ],
)
def test_register_version_decided_by_the_file(
    mocker, tmp_path, version, present, outcome
):
    # A token keeps the highest version while the file is its file. Otherwise
    # ${{PATCH}} makes the next version and ${{LATEST}} is refused; the
    # version decided is the one registered
    _data_file = os.path.join(tmp_path, "data.csv")
    with open(_data_file, "w") as out_f:
        out_f.write("a,b\n1,2\n")
    mocker.patch(
        "fair.registry.sync.download_from_registry", return_value=_data_file
    )
    mocker.patch("fair.registry.requests.local_token", return_value="")
    mocker.patch("fair.register.convert_key_value_to_id", return_value=3)
    _results = [{"version": "0.0.1"}, {"version": "0.0.2"}]
    mocker.patch(
        "fair.registry.storage.check_if_object_exists",
        return_value=_results if present == "results" else present,
    )
    _store = mocker.patch(
        "fair.registry.storage.store_data_file", return_value="registered"
    )
    _entry = {
        "data_product": "a/b",
        "use": {"data_product": "a/b", "namespace": "PSU", "version": version},
        "root": "https://example.com/",
        "path": "a/b.csv",
        "file_type": "csv",
        "public": True,
    }
    _store_dir = os.path.join(tmp_path, "store")
    if outcome in ("has changed", "no version of it"):
        with pytest.raises(fdp_exc.UserConfigError, match=outcome):
            fdp_reg.fetch_registrations(
                "http://127.0.0.1:8000/api/", "", _store_dir, [_entry]
            )
        _store.assert_not_called()
        return
    fdp_reg.fetch_registrations(
        "http://127.0.0.1:8000/api/", "", _store_dir, [_entry]
    )
    if outcome == "passed over":
        _store.assert_not_called()
    else:
        assert _store.call_args.kwargs["data"]["use"]["version"] == outcome
        assert _store.call_args.kwargs["local_file"].endswith(f"{outcome}.csv")


@pytest.mark.faircli_register
@pytest.mark.dependency(name="register")
def test_register(
    global_config,
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    tmp_path,
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

        _cfg_path = os.path.join(
            pyDataPipeline, "simpleModel", "ext", "SEIRSconfig.yaml"
        )
        _res = _cli_runner.invoke(
            cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0

        # The external object says where its file was fetched from: the root
        # and path of its 'register' entry, and the file that was found there
        _register = next(
            entry
            for entry in yaml.safe_load(open(_cfg_path))["register"]
            if "external_object" in entry
        )
        _token = local_registry._token

        def _get(record, field):
            return fdp_req.url_get(record[field], _token)

        _external = fdp_req.get(
            local_registry._url, "external_object", _token
        )[0]
        _original = _get(_external, "original_store")
        _stored = _get(
            _get(_get(_external, "data_product"), "object"), "storage_location"
        )
        assert _get(_original, "storage_root")["root"] == _register["root"]
        assert _original["path"] == _register["path"]
        assert _original["hash"] == _stored["hash"]
        assert _original["url"] != _stored["url"]

        # The same file registered again, under a second name and in a second
        # namespace, is a data product each time, and a further pull adds none
        _again = f"{_register['external_object']}/again"
        _again_cfg = yaml.safe_load(open(_cfg_path))
        _again_cfg.pop("write", None)
        _again_cfg["register"] = [
            entry
            for entry in _again_cfg["register"]
            if "external_object" not in entry
        ] + [
            {"namespace": "SecondNamespace", "full_name": "A second one"},
            dict(_register, external_object=_again),
            dict(
                _register,
                namespace_name="SecondNamespace",
                release_version="2.1.0",
            ),
        ]
        _again_path = os.path.join(tmp_path, "again.yaml")
        with open(_again_path, "w") as f:
            yaml.dump(_again_cfg, f, sort_keys=False)

        def _data_products():
            return sorted(
                (_get(_product, "namespace")["name"], _product["name"])
                for _product in fdp_req.get(
                    local_registry._url, "data_product", _token
                )
            )

        _expected = sorted(
            _data_products()
            + [
                (_register["namespace_name"], _again),
                ("SecondNamespace", _register["external_object"]),
            ]
        )
        for _ in range(2):
            _res = _cli_runner.invoke(
                cli, ["pull", _again_path, "--debug"], catch_exceptions=True
            )
            assert _res.exit_code == 0
            assert _data_products() == _expected

        # The registry records the file once, at the first name's path, and
        # the data store holds it once
        _store = _get(_stored, "storage_root")["root"].replace("file://", "")
        assert os.path.exists(os.path.join(_store, _stored["path"]))
        assert [
            _file
            for _dir, _, _files in os.walk(_store)
            for _file in _files
            if fdp_store.calculate_file_hash(os.path.join(_dir, _file))
            == _stored["hash"]
        ] == [os.path.basename(_stored["path"])]

        # An entry's 'release_version' is the version of its external object
        assert fdp_req.get(
            local_registry._url,
            "external_object",
            _token,
            params={"version": "2.1.0"},
        )

        # A source is one file: another file under the same identifier, title
        # and release version is refused, and registered once it has a title
        # of its own
        _other_cfg = yaml.safe_load(open(_cfg_path))
        _other_cfg.pop("write", None)
        _other = dict(
            _register,
            external_object="another/file",
            root=TEST_DATA_DIR,
            path="test1.csv",
        )
        _other_cfg["register"] = [
            entry
            for entry in _other_cfg["register"]
            if "external_object" not in entry
        ] + [_other]
        _other_path = os.path.join(tmp_path, "other.yaml")
        with open(_other_path, "w") as f:
            yaml.dump(_other_cfg, f, sort_keys=False)
        _res = _cli_runner.invoke(
            cli, ["pull", _other_path, "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 1
        assert "A source is one file" in _res.output
        assert _data_products() == _expected

        _other["title"] = "A title of its own"
        with open(_other_path, "w") as f:
            yaml.dump(_other_cfg, f, sort_keys=False)
        _res = _cli_runner.invoke(
            cli, ["pull", _other_path, "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 0
        _expected.append((_register["namespace_name"], "another/file"))
        assert _data_products() == sorted(_expected)

        _working_yaml_path = os.path.join(tmp_path, "working_yaml.yaml")
        _cfg_str = {}

        with open(TEST_REGISTER_CFG) as cfg_file:
            _cfg_str = cfg_file.read()

        print(f"Test Data Directory {TEST_DATA_DIR}")
        _cfg_str = _cfg_str.replace("<TEST_DATA_DIR>", TEST_DATA_DIR)

        _cfg = yaml.safe_load(_cfg_str)

        with open(_working_yaml_path, "w") as f:
            yaml.dump(_cfg, f, sort_keys=False)

        _res = _cli_runner.invoke(
            cli, ["pull", _working_yaml_path, "--debug"], catch_exceptions=True
        )
        with capsys.disabled():
            print(f"exit code: {_res.exit_code}")
            print(f"exc info: {_res.exc_info}")
            print(f"exception: {_res.exception}")
        assert _res.exit_code == 0


@pytest.mark.faircli_register
@pytest.mark.dependency(depends=["register"])
def test_register_no_file(
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    mocker,
    tmp_path,
):
    # A source with no file of its own, such as a deposit a run queries, is
    # registered by 'fair pull' as a data product with no file, once, and a
    # run may then read it. The DOI is one the registration test resolves
    monkeypatch_module.chdir(pyDataPipeline)
    _download = mocker.spy(fdp_sync, "download_from_registry")
    _entry = dict(
        _NO_FILE_ENTRY, identifier="https://doi.org/10.1038/s41592-020-0856-2"
    )
    _cfg = {
        "run_metadata": {
            "description": "Registering a deposit",
            "script": "echo done",
        },
        "register": [
            {"namespace": "ECMWF", "full_name": "A namespace for a deposit"},
            _entry,
        ],
    }
    _cfg_path = os.path.join(tmp_path, "deposit.yaml")

    def _write(entry):
        _cfg["register"][1] = entry
        with open(_cfg_path, "w") as f:
            yaml.dump(_cfg, f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()
    with local_registry, remote_registry:
        _token = local_registry._token

        def _deposits():
            return fdp_req.get(
                local_registry._url,
                "data_product",
                _token,
                params={"name": "deposit/whole"},
            )

        _write(_entry)
        for _ in range(2):
            _res = _cli_runner.invoke(
                cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
            )
            assert _res.exit_code == 0, _res.output
            assert len(_deposits()) == 1
        _deposit = _deposits()[0]
        assert _deposit["version"] == "1.0.0"
        assert not fdp_req.url_get(_deposit["object"], _token)[
            "storage_location"
        ]
        _source = fdp_req.url_get(_deposit["external_object"], _token)
        assert (_source["identifier"], _source["title"]) == (
            _entry["identifier"],
            _entry["title"],
        )
        _download.assert_not_called()

        # Another source at that version is refused
        _write(dict(_entry, title="Another deposit"))
        _res = _cli_runner.invoke(
            cli, ["pull", _cfg_path, "--debug"], catch_exceptions=True
        )
        assert _res.exit_code == 1
        assert "registered already" in _res.output
        assert len(_deposits()) == 1

        # A run is given it to read
        _write(_entry)
        _res = _cli_runner.invoke(
            cli,
            ["run", _cfg_path, "--local", "--dirty", "--debug"],
            catch_exceptions=True,
        )
        assert _res.exit_code == 0, _res.output


@pytest.mark.faircli_register
@pytest.mark.dependency(depends=["register"])
def test_register_without_a_version(
    local_registry,
    remote_registry,
    pyDataPipeline: str,
    monkeypatch_module,
    tmp_path,
):
    # An entry with no version keeps the highest version while its file is the
    # same, and has the next when it changes; a run reads the highest. With
    # ${{LATEST}} a changed file is refused, and a run refused anything that
    # 'fair pull' has not registered
    monkeypatch_module.chdir(pyDataPipeline)
    _file = os.path.join(tmp_path, "unversioned.csv")
    _entry = {
        "data_product": "unversioned/data",
        "namespace_name": "TestNameSpace",
        "root": f"file://{tmp_path}{os.path.sep}",
        "path": "unversioned.csv",
        "file_type": "csv",
    }
    _cfg = {
        "run_metadata": {"description": "No version", "script": "echo done"},
        "register": [
            {"namespace": "TestNameSpace", "full_name": "A namespace"},
            _entry,
        ],
    }
    _cfg_path = os.path.join(tmp_path, "unversioned.yaml")

    def _write(content, entry=_entry):
        with open(_file, "w") as out_f:
            out_f.write(content)
        _cfg["register"][1] = entry
        with open(_cfg_path, "w") as cfg_f:
            yaml.dump(_cfg, cfg_f, sort_keys=False)

    _cli_runner = click.testing.CliRunner()

    def _fair(*args):
        return _cli_runner.invoke(
            cli, [*args, _cfg_path, "--debug"], catch_exceptions=True
        )

    with local_registry, remote_registry:
        _token = local_registry._token

        def _versions(name="unversioned/data"):
            return sorted(
                product["version"]
                for product in fdp_req.get(
                    local_registry._url,
                    "data_product",
                    _token,
                    params={"name": name},
                )
            )

        _write("a,b\n1,2\n")
        for _ in range(2):
            assert _fair("pull").exit_code == 0
            assert _versions() == ["0.0.1"]
        _write("a,b\n3,4\n")
        assert _fair("pull").exit_code == 0
        assert _versions() == ["0.0.1", "0.0.2"]

        _res = _fair("run", "--local", "--dirty")
        assert _res.exit_code == 0, _res.output
        _job = max(glob.glob(os.path.join(fdp_com.default_jobs_dir(), "*")))
        with open(os.path.join(_job, fdp_com.USER_CONFIG_FILE)) as job_f:
            _read = yaml.safe_load(job_f)["read"]
        assert [(r["data_product"], r["use"]["version"]) for r in _read] == [
            ("unversioned/data", "0.0.2")
        ]

        _latest = dict(_entry, version="${{LATEST}}")
        _write("a,b\n3,4\n", _latest)
        assert _fair("pull").exit_code == 0
        _write("a,b\n5,6\n", _latest)
        _res = _fair("pull")
        assert _res.exit_code == 1
        assert "has changed" in _res.output
        assert _versions() == ["0.0.1", "0.0.2"]

        _write("a,b\n1,2\n", dict(_entry, data_product="never/pulled"))
        _res = _fair("run", "--local", "--dirty")
        assert _res.exit_code == 1
        assert "was not found" in _res.output
