import pytest
import os
import git
import datetime
import pathlib

import fair.parsing as fdp_parse
import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc

@pytest.mark.varparse
def test_parse_vars(mocker, git_mock):
    _dummy_file = os.path.join(os.path.dirname(__file__), 'data', 'dummy_conf.yaml')
    _now = datetime.datetime.now()
    mocker.patch.object(
        fdp_com,
        'find_fair_root',
        lambda x : pathlib.Path(os.path.dirname(_dummy_file)).parent.parent
    )
    _fake_name = 'Joe Bloggs'
    mocker.patch.object(fdp_conf, 'get_current_user_name', lambda x : _fake_name)
    _fake_id = '02842618'
    mocker.patch.object(fdp_conf, 'get_current_user_orcid', lambda x : _fake_id)
    _branch = git.Repo(
        pathlib.Path(os.path.dirname(_dummy_file)).parent.parent
    ).active_branch.name
    _remote = git.Repo(
        pathlib.Path(os.path.dirname(_dummy_file)).parent.parent
    ).remotes['origin'].url

    _other = git.Repo(
        pathlib.Path(os.path.dirname(_dummy_file)).parent.parent
    ).remotes['other'].url

    _u_config = fdp_parse.subst_cli_vars(
        os.getcwd(),
        _now,
        _dummy_file
    )

    print(f'\nOutput Config:\n{_u_config}')
    _junk = 'Datetime - '
    assert _now.strftime("%Y-%m-%s %H:%M:%S") == _u_config['read'][0].replace(_junk, '')
    _junk = 'Repo/'
    assert _dummy_file == _u_config['read'][1].replace(_junk, '')
    _junk = 'RandomText@?'
    assert _branch == _u_config['write'][0].replace(_junk, '')
    _junk = 'origin21231'
    assert _remote == _u_config['write'][1].replace(_junk, '')
    assert _u_config['run_metadata']['author_id'] == _fake_id
    assert _u_config['run_metadata']['conf_dir'] == os.getcwd()
    assert _u_config['run_metadata']['git_url'] == _other
    assert _u_config['read'][2]['user'] == _fake_name

