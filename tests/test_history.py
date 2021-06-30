import pytest
import tempfile
import hashlib
import os

import fair.history as fdp_hist


TEST_LOG = """
--------------------------------
 Commenced = Mon Jun 28 10:14:25 2021 
 Author    = Joe Bloggs <jbloggs@nowhere>
 Namespace = jbloggs
 Command   = bash -eo pipefail /home/jbloggs/.fair/data/coderun/2021-06-28_10_14_25/run_script
--------------------------------
Hello World!
------- time taken 0:00:00.018649 -------
"""


@pytest.fixture
def dummy_log(mocker):
    _hist_dir = tempfile.mkdtemp()
    mocker.patch.object(fdp_hist, "history_directory", lambda *args: _hist_dir)
    with open(
        os.path.join(_hist_dir, "run_2021-06-28_10_14_25.log"), "w"
    ) as f:
        f.write(TEST_LOG)
    return _hist_dir


@pytest.mark.history
def test_show_hist_log(capfd, dummy_log):
    fdp_hist.show_run_log(
        dummy_log, hashlib.sha1(TEST_LOG.encode("utf-8")).hexdigest()
    )
    out, _ = capfd.readouterr()
    assert out.strip() == TEST_LOG.strip()


@pytest.mark.history
def test_show_history(capfd, dummy_log):
    _expt = """
run 876e9247944b6487dbf6b5a2777cbf1c5249e22b
Author: Joe Bloggs <jbloggs@nowhere>
Date:   Mon Jun 28 10:14:25 2021
    """
    fdp_hist.show_history(dummy_log, 1)
    out, _ = capfd.readouterr()
    assert out.strip() == _expt.strip()
