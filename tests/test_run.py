import datetime
import pytest
import os
import glob
import tempfile
import platform
import time
import yaml

import fair.run as fdp_run
import fair.common as fdp_com
import fair.history as fdp_hist
import fair.configuration as fdp_conf


@pytest.mark.run
def test_create_work_cfg(no_init_session):
    no_init_session.make_starter_config()
    _now = datetime.datetime.now()
    _ts = _now.strftime("%Y-%m-%d_%H_%M_%S")
    _out = os.path.join(fdp_com.default_coderun_dir(), _ts, "config.yaml")
    os.makedirs(os.path.dirname(_out))
    fdp_run.create_working_config(
        no_init_session._session_loc,
        no_init_session._session_config,
        _out,
        _now,
    )
    assert os.path.exists(_out)
    _cfg = yaml.safe_load(_out)
    assert _cfg


@pytest.fixture
def setup_with_opts(no_init_session):
    def _do_setup(meta_data_additions):
        with open(no_init_session._session_config) as f:
            _cfg = yaml.safe_load(f)
            for addition, value in meta_data_additions.items():
                _cfg["run_metadata"][addition] = value
        for a in ["shell", "script", "script_path"]:
            if a not in meta_data_additions and a in _cfg["run_metadata"]:
                del _cfg["run_metadata"][a]
        with open(no_init_session._session_config, "w") as f:
            yaml.dump(_cfg, f)

        _out = fdp_run.setup_run_script(
            no_init_session._session_config, no_init_session._session_loc
        )

        return _out, _cfg

    return _do_setup


@pytest.mark.run
def test_run_setup_custom(setup_with_opts):
    _out, _cfg = setup_with_opts(
        {"shell": "python", "script": "print('Test Run')"}
    )
    assert _out["shell"] == "python"
    assert open(_out["script"]).read() == "print('Test Run')"
    assert _out["env"]["FDP_LOCAL_REPO"] == _cfg["run_metadata"]["local_repo"]


@pytest.mark.run
def test_run_setup_default_unix(mocker, setup_with_opts):
    mocker.patch.object(platform, "system", lambda *args: "Linux")
    _cmd = 'echo "Test Run"'
    _out, _cfg = setup_with_opts({"script": _cmd})
    assert _out["shell"] == "bash"
    assert open(_out["script"]).read() == _cmd
    assert _out["env"]["FDP_LOCAL_REPO"] == _cfg["run_metadata"]["local_repo"]


@pytest.mark.run
def test_run_setup_default_windows(mocker, setup_with_opts):
    mocker.patch.object(platform, "system", lambda *args: "Windows")
    _cmd = 'Write-Host "Test Run"'
    _out, _cfg = setup_with_opts({"script": _cmd})
    assert _out["shell"] == "pwsh"
    assert open(_out["script"]).read() == _cmd
    assert _out["env"]["FDP_LOCAL_REPO"] == _cfg["run_metadata"]["local_repo"]


@pytest.mark.run
def test_run_setup_with_script(setup_with_opts):
    _script = "print('Test Run')"
    _temp = tempfile.mktemp()
    with open(_temp, "w") as f:
        f.write(_script)
    _out, _cfg = setup_with_opts({"shell": "python", "script_path": _temp})
    assert _out["shell"] == "python"
    assert open(_out["script"]).read() == "print('Test Run')"
    assert _out["env"]["FDP_LOCAL_REPO"] == _cfg["run_metadata"]["local_repo"]


@pytest.mark.run
def test_run_config_cmd(mocker, no_init_session):
    os.makedirs(fdp_hist.history_directory(), exist_ok=True)
    # FIXME: Have to sleep as concurrent runs break (as same date and time)
    time.sleep(1)
    no_init_session.make_starter_config()
    mocker.patch.object(
        fdp_conf,
        "read_local_fdpconfig",
        lambda *args: no_init_session._local_config,
    )
    _before = glob.glob(os.path.join(fdp_com.default_coderun_dir(), "*"))

    with open(no_init_session._session_config) as f:
        _cfg = yaml.safe_load(f)
    _cfg["run_metadata"]["script"] = 'echo "Hello World!"'
    with open(no_init_session._session_config, "w") as f:
        yaml.dump(_cfg, f)
    fdp_run.run_command(
        no_init_session._session_loc,
        no_init_session._session_config,
    )
    _after = glob.glob(os.path.join(fdp_com.default_coderun_dir(), "*"))
    assert len(_before) + 1 == len(_after)
    _run_dir = [i for i in _after if i not in _before][0]
    assert os.path.exists(os.path.join(_run_dir, "config.yaml"))
    assert os.path.exists(os.path.join(_run_dir, "run_script"))


def test_run_bash_cmd(mocker, no_init_session):
    os.makedirs(fdp_hist.history_directory(), exist_ok=True)
    # FIXME: Have to sleep as concurrent runs break (as same date and time)
    time.sleep(1)
    no_init_session.make_starter_config()
    mocker.patch.object(
        fdp_conf,
        "read_local_fdpconfig",
        lambda *args: no_init_session._local_config,
    )
    _before = glob.glob(os.path.join(fdp_com.default_coderun_dir(), "*"))

    with open(no_init_session._session_config) as f:
        _cfg = yaml.safe_load(f)
    if "script_path" in _cfg["run_metadata"]:
        del _cfg["run_metadata"]["script_path"]
    fdp_run.run_command(
        no_init_session._session_loc,
        no_init_session._session_config,
        'echo "Hello World!"',
    )
    _after = glob.glob(os.path.join(fdp_com.default_coderun_dir(), "*"))
    assert len(_before) + 1 == len(_after)
    _run_dir = [i for i in _after if i not in _before][0]
    assert os.path.exists(os.path.join(_run_dir, "config.yaml"))
    assert os.path.exists(os.path.join(_run_dir, "run_script"))
