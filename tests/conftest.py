import os
import pytest
import pytest_mock
import tempfile
import subprocess


from fair.common import default_jobs_dir
from fair.history import history_directory

TEST_JOB_FILE_TIMESTAMP = '2021-10-11_10_0_0_100000'

@pytest.fixture
def job_directory(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set default to point to temporary
        mocker.patch('fair.common.default_jobs_dir', lambda: tempd)

        # Create a mock job directory
        os.makedirs(os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP))
        yield os.path.join(tempd, TEST_JOB_FILE_TIMESTAMP)


@pytest.fixture
def job_log(mocker: pytest_mock.MockerFixture) -> str:
    with tempfile.TemporaryDirectory() as tempd:
        # Set the log directory
        mocker.patch('fair.history.history_directory', lambda *args: tempd)

        # Create mock job log
        with open(os.path.join(tempd, f'job_{TEST_JOB_FILE_TIMESTAMP}.log'), 'w') as out_f:
            out_f.write('''--------------------------------
 Commenced = Fri Oct 08 14:45:43 2021 
 Author    = Interface Test <test@noreply>
 Command   = fair pull
--------------------------------
------- time taken 0:00:00.791088 -------''')

        yield tempd


@pytest.fixture(scope="module")
def local_registry():
    pass
