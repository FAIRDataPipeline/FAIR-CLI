import datetime
import pytest
import os
import yaml

import fair.run as fdp_run
import fair.common as fdp_com


@pytest.mark.run
def test_create_user_cfg(no_init_session):
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
