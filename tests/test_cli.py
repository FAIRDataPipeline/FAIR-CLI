import pytest
import os
import subprocess
import datetime
import yaml


import fair.common as fdp_com
import fair.configuration as fdp_conf


@pytest.mark.cli
def test_run_bash(no_init_session):
    no_init_session.make_starter_config()
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    no_init_session.close_session()
    _fair_run_bash_cmd = "fair run bash \"echo 'Hello World!'\""
    _cwd = os.getcwd()
    os.chdir(no_init_session._session_loc)
    subprocess.check_call(_fair_run_bash_cmd, shell=True)
    os.chdir(_cwd)
    _log_file = sorted(
        os.listdir(
            os.path.join(
                no_init_session._session_loc, fdp_com.FAIR_FOLDER, "logs"
            )
        )
    )[0]
    assert datetime.datetime.now().strftime("%Y-%m-%d_%H_%M_%S") in _log_file


@pytest.mark.cli
def test_run_norm(no_init_session):
    no_init_session.make_starter_config()
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )
    no_init_session.close_session()
    _cfg = yaml.safe_load(
        open(fdp_com.local_user_config(no_init_session._session_loc))
    )
    _cfg["run_metadata"]["shell"] = "python"
    _cfg["run_metadata"]["script"] = "print('Hello World!')"
    yaml.dump(
        _cfg,
        open(fdp_com.local_user_config(no_init_session._session_loc), "w"),
    )
    _fair_run_bash_cmd = "fair run"

    _cwd = os.getcwd()
    os.chdir(no_init_session._session_loc)
    subprocess.check_call(_fair_run_bash_cmd, shell=True)
    os.chdir(_cwd)
    _log_file = sorted(
        os.listdir(
            os.path.join(
                no_init_session._session_loc, fdp_com.FAIR_FOLDER, "logs"
            )
        )
    )[0]
    assert datetime.datetime.now().strftime("%Y-%m-%d_%H_%M") in _log_file
