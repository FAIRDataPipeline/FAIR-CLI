import pytest
import requests
import subprocess
import os

import fair.server as fdp_serv
import fair.exceptions as fdp_exc

LOCALHOST = "http://localhost:8000/api/"


def server_up(mocker):
    class mock_return:
        def __init__(self, url):
            self.status_code = 200 if url == LOCALHOST else 400

    mocker.patch.object(requests, "get", lambda url: mock_return(url))


def server_down(mocker):
    class mock_return:
        def __init__(self, url):
            self.status_code = 400 if url == LOCALHOST else 200
            if url == LOCALHOST:
                raise requests.exceptions.ConnectionError

    mocker.patch.object(requests, "get", lambda url: mock_return(url))


@pytest.fixture
def server_emulator(mocker):
    class _dummy_subprocess:
        def wait(self):
            return True

        def __init__(self, cmd_list):
            class stdout:
                def __init__(self):
                    pass

                def read(self, i):
                    return b""

            self.stdout = stdout()
            assert os.path.exists(cmd_list[0])
            if "run" in cmd_list[0]:
                server_up(mocker)
            else:
                server_down(mocker)

    mocker.patch.object(
        subprocess,
        "Popen",
        lambda cmd_list, **kwargs: _dummy_subprocess(cmd_list),
    )


@pytest.mark.server
def test_server_running(server_emulator):
    assert not fdp_serv.check_server_running(LOCALHOST)


@pytest.mark.server
def test_start_server_success(server_emulator):
    fdp_serv.launch_server(LOCALHOST, True)


@pytest.mark.server
def test_start_server_fail(server_emulator):
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.launch_server("http://badhost", True)


@pytest.mark.server
def test_stop_server_success(server_emulator):
    fdp_serv.stop_server(LOCALHOST)


@pytest.mark.server
def test_stop_server_fail(server_emulator):
    with pytest.raises(fdp_exc.RegistryError):
        fdp_serv.stop_server("http://badhost")
