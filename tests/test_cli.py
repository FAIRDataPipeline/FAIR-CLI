import os
import datetime
import glob
import pytest
import yaml

from click.testing import CliRunner

import fair.common as fdp_com
import fair.history as fdp_hist
import fair.cli as fdp_cli


@pytest.mark.cli
def test_run_bash(no_init_session):
    no_init_session.make_starter_config()
    os.makedirs(
        os.path.join(no_init_session._session_loc, fdp_com.FAIR_FOLDER),
        exist_ok=True,
    )

    no_init_session.close_session()
    
    _runner = CliRunner()
    _result = _runner.invoke(fdp_cli.cli, ['run', '--script', "echo 'Hello World!'"])

    try:
        assert _result.exit_code == 0
        assert _result.output == "Hello World!\n"
    except AssertionError as e:
        print(_result.output)
        raise e

    _hist_dir = fdp_hist.history_directory(no_init_session._session_loc)

    _time_sorted_logs = sorted(
        glob.glob(os.path.join(_hist_dir, "*")),
        key=os.path.getmtime,
        reverse=True,
    )

    assert datetime.datetime.now().strftime("%Y-%m-%d_%H_%M") in open(_time_sorted_logs[-1]).read()


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
    _cfg["run_metadata"]['script'] = "print('Hello World!')"
    yaml.dump(
        _cfg,
        open(fdp_com.local_user_config(no_init_session._session_loc), "w"),
    )

    _cwd = os.getcwd()
    os.chdir(no_init_session._session_loc)
    _runner = CliRunner()
    _result = _runner.invoke(fdp_cli.cli, ['run'])
    assert _result.exit_code == 0
    assert _result.output == "Hello World!\n"
    os.chdir(_cwd)
    _log_file = sorted(
        os.listdir(
            os.path.join(
                no_init_session._session_loc, fdp_com.FAIR_FOLDER, "logs"
            )
        )
    )[0]

    # If test is run over a minute boundary then it may fail
    try:
        _now = datetime.datetime.now()
        assert _now.strftime("%Y-%m-%d_%H_%M") in _log_file
    except AssertionError:
        _now += datetime.timedelta(minutes=-1)
        assert _now.strftime("%Y-%m-%d_%H_%M") in _log_file
