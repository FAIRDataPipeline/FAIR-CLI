import hashlib
import os
import pathlib

import click.testing
import pytest

import fair.exceptions as fdp_exc
import fair.history as fdp_hist
from fair.cli import cli
from fair.common import FAIR_FOLDER

_JOB_LOG = """--------------------------------
 Commenced = Fri Oct 08 14:45:43 2021
 Author    = Interface Test <test@noreply>
 Command   = fair run
--------------------------------
------- time taken 0:00:00.791088 -------"""


def _job_hash(jobs_dir: str, timestamp: str) -> str:
    """Return the identifier 'fair log' gives a job"""
    _job = os.path.abspath(os.path.join(jobs_dir, timestamp))
    return hashlib.sha1(_job.encode("utf-8")).hexdigest()


def _listed_jobs(output: str) -> list:
    """Return the identifiers of the jobs 'fair log' printed"""
    return [
        line.split()[-1] for line in output.splitlines() if line.startswith("job #")
    ]


@pytest.fixture
def shared_jobs(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> dict:
    """Make a jobs directory two repositories share, and point the history at it

    This repository has eleven jobs, each with its log. Among them are two jobs
    of another repository, a job whose working config names this repository but
    whose log is gone, and a job with no working config.
    """
    _repo = tmp_path.joinpath("repo")
    _repo.joinpath(FAIR_FOLDER).mkdir(parents=True)
    _other = tmp_path.joinpath("other")
    _jobs = tmp_path.joinpath("jobs")
    _logs = tmp_path.joinpath("logs")
    _logs.mkdir()
    _own = [f"2021-10-{day:02d}_10_00_00_000000" for day in range(1, 12)]
    _others = {
        "2021-10-12_10_00_00_000000": str(_other),
        "2021-10-05_12_00_00_000000": str(_other),
        "2021-10-06_12_00_00_000000": str(_repo),
        "2021-10-07_12_00_00_000000": None,
    }
    for _time in _own:
        _jobs.joinpath(_time).mkdir(parents=True)
        _logs.joinpath(f"job_{_time}.log").write_text(_JOB_LOG, encoding="utf-8")
    for _time, _repo_dir in _others.items():
        _jobs.joinpath(_time).mkdir(parents=True)
        if _repo_dir:
            _jobs.joinpath(_time, "config.yaml").write_text(
                f"run_metadata:\n  local_repo: '{_repo_dir}'\n", encoding="utf-8"
            )
    monkeypatch.setattr("fair.common.default_jobs_dir", lambda *args: str(_jobs))
    monkeypatch.setattr("fair.history.history_directory", lambda *args: str(_logs))
    return {
        "repo": str(_repo),
        "other": str(_other),
        "jobs": str(_jobs),
        "own": sorted(_own, reverse=True),
        "others": sorted(_others, reverse=True),
    }


@pytest.mark.faircli_history
def test_history_directory(job_directory: str):
    os.makedirs(os.path.join(os.path.dirname(job_directory), FAIR_FOLDER))
    _expected = os.path.join(os.path.dirname(job_directory), FAIR_FOLDER, "logs")
    assert fdp_hist.history_directory(job_directory) == _expected


@pytest.mark.faircli_history
def test_show_history(capsys: pytest.CaptureFixture, job_directory: str, job_log: str):
    fdp_hist.show_history(os.getcwd())
    _captured = capsys.readouterr()
    _job = _captured.out.split("\n")[0].split()[-1]
    assert _job == hashlib.sha1(job_directory.encode("utf-8")).hexdigest()
    assert (
        _captured.out.split("\n")[1].split(": ")[1].strip()
        == "Interface Test <test@noreply>"
    )
    assert (
        _captured.out.split("\n")[2].split(": ")[1].strip()
        == "Fri Oct 08 14:45:43 2021"
    )


@pytest.mark.faircli_history
def test_job_log_show(capsys: pytest.CaptureFixture, job_directory: str, job_log: str):
    fdp_hist.show_job_log(
        os.getcwd(), hashlib.sha1(job_directory.encode("utf-8")).hexdigest()
    )
    _captured = capsys.readouterr()
    _command = _captured.out.split("\n")[3].split("=")[-1].strip()
    assert _command == "fair pull"


@pytest.mark.faircli_history
def test_show_history_shared_jobs(capsys: pytest.CaptureFixture, shared_jobs: dict):
    fdp_hist.show_history(shared_jobs["repo"])
    _out = capsys.readouterr().out
    assert _listed_jobs(_out) == [
        _job_hash(shared_jobs["jobs"], t) for t in shared_jobs["own"][:10]
    ]
    assert (
        f"Not listed: 4 jobs in '{shared_jobs['jobs']}' with no log in this repository"
        in _out
    )
    assert (
        f"2 jobs run from '{shared_jobs['other']}', where 'fair log' lists them" in _out
    )
    assert "1 job run from this repository, its log gone" in _out
    assert "1 job whose working config names no repository" in _out


@pytest.mark.faircli_history
def test_show_history_length(capsys: pytest.CaptureFixture, shared_jobs: dict):
    fdp_hist.show_history(shared_jobs["repo"], length=3)
    assert _listed_jobs(capsys.readouterr().out) == [
        _job_hash(shared_jobs["jobs"], t) for t in shared_jobs["own"][:3]
    ]


@pytest.mark.faircli_history
def test_show_history_no_jobs(
    capsys: pytest.CaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    shared_jobs: dict,
):
    _no_logs = str(tmp_path.joinpath("no_logs"))
    monkeypatch.setattr("fair.history.history_directory", lambda *args: _no_logs)
    fdp_hist.show_history(shared_jobs["repo"])
    _out = capsys.readouterr().out
    assert not _listed_jobs(_out)
    assert "No jobs have been run from this repository" in _out
    assert "Not listed: 15 jobs" in _out


@pytest.mark.faircli_history
def test_job_log_show_other_repository(shared_jobs: dict):
    _job = _job_hash(shared_jobs["jobs"], shared_jobs["others"][0])
    with pytest.raises(fdp_exc.FileNotFoundError) as e:
        fdp_hist.show_job_log(shared_jobs["repo"], _job)
    assert "Could not find job matching id" in e.value.msg
    assert "'fair log'" in e.value.hint


@pytest.mark.faircli_history
@pytest.mark.parametrize(
    "arguments,shown",
    [([], 10), (["-n", "3"], 3), (["--count", "12"], 11), (["--all"], 11)],
)
def test_fair_log_shows_as_many_jobs_as_asked(
    shared_jobs: dict, arguments: list, shown: int
):
    _result = click.testing.CliRunner().invoke(cli, ["log", *arguments])
    assert _result.exit_code == 0, _result.output
    assert _listed_jobs(_result.stdout) == [
        _job_hash(shared_jobs["jobs"], t) for t in shared_jobs["own"][:shown]
    ]


@pytest.mark.faircli_history
@pytest.mark.parametrize(
    "arguments,refusal",
    [
        (["--all", "-n", "3"], "Give '-n' or '--all', not both"),
        (["-n", "0"], "0 is not in the range x>=1"),
    ],
)
def test_fair_log_refuses(shared_jobs: dict, arguments: list, refusal: str):
    _result = click.testing.CliRunner().invoke(cli, ["log", *arguments])
    assert _result.exit_code != 0
    assert refusal in _result.output
    assert not _listed_jobs(_result.output)
