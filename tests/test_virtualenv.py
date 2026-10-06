import os
import sys

import pytest
import pytest_mock

import fair.exceptions as fdp_exc
import fair.virtualenv as fdp_env


@pytest.mark.faircli_virtualenv
def test_find_python_not_frozen(mocker: pytest_mock.MockerFixture):
    mocker.patch.object(sys, "frozen", False, create=True)
    assert fdp_env.find_python() == [sys.executable]


@pytest.mark.faircli_virtualenv
def test_find_python_frozen_uses_path(mocker: pytest_mock.MockerFixture):
    # In a PyInstaller binary sys.executable is the binary, never a Python
    mocker.patch.object(sys, "frozen", True, create=True)
    mocker.patch.dict(os.environ, {}, clear=True)
    mocker.patch(
        "shutil.which", side_effect=lambda n: {"python": "/usr/bin/python"}.get(n)
    )
    mocker.patch.object(fdp_env, "_is_suitable", return_value=True)
    assert fdp_env.find_python() == ["/usr/bin/python"]


@pytest.mark.faircli_virtualenv
def test_find_python_frozen_skips_old(mocker: pytest_mock.MockerFixture):
    mocker.patch.object(sys, "frozen", True, create=True)
    mocker.patch.dict(os.environ, {}, clear=True)
    mocker.patch("shutil.which", side_effect=lambda n: f"/usr/bin/{n}")
    mocker.patch.object(
        fdp_env, "_is_suitable", side_effect=lambda p: p[0] == "/usr/bin/python"
    )
    assert fdp_env.find_python() == ["/usr/bin/python"]


@pytest.mark.faircli_virtualenv
def test_find_python_frozen_env_var(mocker: pytest_mock.MockerFixture):
    mocker.patch.object(sys, "frozen", True, create=True)
    mocker.patch.dict(os.environ, {"FAIR_PYTHON": sys.executable})
    assert fdp_env.find_python() == [sys.executable]

    mocker.patch.dict(os.environ, {"FAIR_PYTHON": "/no/such/python"})
    with pytest.raises(fdp_exc.RegistryError):
        fdp_env.find_python()


@pytest.mark.faircli_virtualenv
def test_is_suitable():
    assert fdp_env._is_suitable([sys.executable])
    assert not fdp_env._is_suitable(["/no/such/python"])


@pytest.mark.faircli_virtualenv
def test_create_venv(tmp_path):
    _venv = tmp_path / "venv"
    fdp_env.create_venv(str(_venv), prompt="Test")
    _bin = "Scripts" if sys.platform == "win32" else "bin"
    assert any((_venv / _bin).glob("python*"))
    assert "prompt = 'Test'" in (_venv / "pyvenv.cfg").read_text()


@pytest.mark.faircli_virtualenv
def test_create_venv_uv_fallback(mocker: pytest_mock.MockerFixture, tmp_path):
    mocker.patch.object(fdp_env, "find_python", return_value=None)
    mocker.patch("shutil.which", return_value="/usr/bin/uv")
    _call = mocker.patch("subprocess.check_call")
    fdp_env.create_venv(str(tmp_path / "venv"))
    _cmd = _call.call_args[0][0]
    assert _cmd[:3] == ["/usr/bin/uv", "venv", "--seed"]
    assert ">=3.10" in _cmd


@pytest.mark.faircli_virtualenv
def test_create_venv_no_python(mocker: pytest_mock.MockerFixture, tmp_path):
    mocker.patch.object(fdp_env, "find_python", return_value=None)
    mocker.patch("shutil.which", return_value=None)
    with pytest.raises(fdp_exc.RegistryError):
        fdp_env.create_venv(str(tmp_path / "venv"))


@pytest.mark.faircli_virtualenv
def test_install_registry_checks_python_first(
    mocker: pytest_mock.MockerFixture, tmp_path
):
    # A missing Python must not leave a half-installed registry behind
    import fair.registry.server as fdp_svr

    mocker.patch.object(fdp_env, "find_python", return_value=None)
    mocker.patch("shutil.which", return_value=None)
    _clone = mocker.patch("git.Repo.clone_from")
    _install_dir = tmp_path / "registry"
    with pytest.raises(fdp_exc.RegistryError):
        fdp_svr.install_registry(install_dir=str(_install_dir))
    _clone.assert_not_called()
    assert not _install_dir.exists()
