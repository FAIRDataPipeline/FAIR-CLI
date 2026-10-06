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


@pytest.mark.faircli_register
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
