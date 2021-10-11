import pytest
import os
import hashlib

import fair.history as fdp_hist

from fair.common import FAIR_FOLDER


@pytest.mark.history
def test_history_directory(job_directory: str):
    os.makedirs(os.path.join(os.path.dirname(job_directory), FAIR_FOLDER))
    _expected = os.path.join(os.path.dirname(job_directory), FAIR_FOLDER, 'logs')
    assert fdp_hist.history_directory(job_directory) == _expected


@pytest.mark.history
def test_show_history(capsys: pytest.CaptureFixture, job_directory: str, job_log: str):
    fdp_hist.show_history(os.getcwd())
    _captured = capsys.readouterr()
    _job = _captured.out.split('\n')[0].split()[-1]
    assert _job == hashlib.sha1(job_directory.encode('utf-8')).hexdigest()
    assert _captured.out.split('\n')[1].split(': ')[1].strip() == 'Interface Test <test@noreply>'
    assert _captured.out.split('\n')[2].split(': ')[1].strip() == 'Fri Oct 08 14:45:43 2021'


@pytest.mark.history
def test_job_log_show(capsys: pytest.CaptureFixture, job_directory: str, job_log: str):
    fdp_hist.show_job_log(os.getcwd(), hashlib.sha1(job_directory.encode('utf-8')).hexdigest())
    _captured = capsys.readouterr()
    _command = _captured.out.split('\n')[3].split('=')[-1].strip()
    assert _command == 'fair pull'
