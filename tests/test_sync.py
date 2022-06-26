import tempfile

import pytest
import pytest_mock

import fair.registry.sync as fdp_sync


@pytest.mark.faircli_sync
def test_pull_download():

    _root = "https://github.com/"
    _path = "FAIRDataPipeline/FAIR-CLI/blob/main/README.md"

    _file = fdp_sync.download_from_registry(
        "http://127.0.0.1:8000", _root, _path
    )

    assert open(_file).read()


@pytest.mark.faircli_sync
def test_fetch_data_product(mocker: pytest_mock.MockerFixture):

    with tempfile.TemporaryDirectory() as tempd:
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
                return [
                    {"name": _dummy_data_product_namespace, "url": "namespace"}
                ]
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
                    "path": "FAIRDataPipeline/FAIR-CLI/archive/refs/heads/main.zip",
                    "storage_root": "storage_root",
                }
            elif "storage_root" in url:
                return {"root": "https://github.com/"}
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
            "data_product": _dummy_data_product_name,
            "object": "object",
        }

        fdp_sync.fetch_data_product("", tempd, _example_data_product)
