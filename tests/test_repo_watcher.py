import subprocess
from pathlib import Path

import pytest
from job_helper.repo_watcher import RepoState, RepoWatcher


def test_RepoState():
    rs = RepoState.from_folder(Path(__file__).parent)
    assert len(rs.commit) > 0


def test_RepoWatcher(tmp_path):
    test_repo = tmp_path / "test_repo"
    test_repo.mkdir()
    subprocess.run("git init", cwd=test_repo, shell=True)
    with (test_repo / "test.txt").open("w") as f:
        print("test", file=f)
    subprocess.run("git add test.txt; git commit -m test", cwd=test_repo, shell=True)
    with (test_repo / "test.txt").open("w") as f:
        print("test2", file=f)
    with (test_repo / "test2.txt").open("w") as f:
        print("test2", file=f)

    w = RepoWatcher(watched_repos=[test_repo])
    rs = w.repo_states()
    assert len(rs) == 1
    assert ("test.txt", "M ") in rs[0].status
    assert ("test2.txt", "??") in rs[0].status
    assert rs[0].direcotry == test_repo
    assert len(rs[0].diff) > 0

    w = RepoWatcher(force_commit_repos=[test_repo])
    with pytest.raises(Exception):
        rs = w.repo_states()
