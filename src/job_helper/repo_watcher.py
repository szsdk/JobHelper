from __future__ import annotations

import base64
import subprocess
import zlib
from pathlib import Path
from typing import Union

from pydantic import BaseModel

from .config import RepoWatcherConfig, jhcfg


def git_status(repo_dir) -> list[tuple[str, str]]:
    """
    Check the Git status of the current folder or the whole repository.
    `scope` can be either "folder" or "repository".
    """

    # Run the 'git status . -s' command
    result = subprocess.run(
        "git status -s .", capture_output=True, shell=True, cwd=repo_dir, text=True
    )
    ans = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if len(line) > 0:
            ans.append((line[2:].strip(), line[:2]))
    return ans


class RepoState(BaseModel):
    directory: Path
    commit: str
    diff: str
    status: list[tuple[str, str]]

    @staticmethod
    def from_folder(folder: Union[str, Path]) -> RepoState:
        dir = Path(folder).resolve()
        git_diff = subprocess.check_output("git diff HEAD", shell=True, cwd=dir)
        compressed_diff = zlib.compress(git_diff)

        return RepoState(
            directory=dir,
            commit=(
                subprocess.check_output("git rev-parse HEAD", shell=True, cwd=dir)
                .decode()
                .strip()
            ),
            diff=base64.b64encode(compressed_diff).decode(),
            status=git_status(dir),
        )


class RepoWatcher(RepoWatcherConfig):
    @classmethod
    def from_jhcfg(cls) -> RepoWatcher:
        return cls.model_validate(jhcfg.repo_watcher.model_dump())

    def repo_states(self) -> list[RepoState]:
        ans = []
        for repo in self.force_commit_repos:
            rs = RepoState.from_folder(repo)
            # Check if there are actual uncommitted changes
            # Empty diffs are compressed to 'eJwDAAAAAAE=' after base64 encoding
            if len(rs.status) > 0 or rs.diff != base64.b64encode(zlib.compress(b'')).decode():
                raise Exception(f"Uncommitted changes in {rs.directory}")
            ans.append(RepoState.from_folder(repo))
        for repo in self.watched_repos:
            ans.append(RepoState.from_folder(repo))
        return ans
