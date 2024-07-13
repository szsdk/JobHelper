import copy
import os
import shlex
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import toml
from job_helper import jhcfg, scheduler
from job_helper.cli import JobHelperConfig, console_main
from pydantic import BaseModel

from tests.fake_slurm import ServerState, SlurmServer


class MockJhcfg:
    def __init__(self, /, **kwargs):
        self._config = kwargs

    def __enter__(self):
        self._old = copy.copy(jhcfg)
        for k, v in self._config.items():
            cls = type(getattr(jhcfg, k))
            if issubclass(cls, BaseModel):
                setattr(jhcfg, k, cls.model_validate(v))
            else:
                setattr(jhcfg, k, v)

        JobHelperConfig.model_validate(jhcfg.model_dump())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for k in self._config.keys():
            setattr(jhcfg, k, getattr(self._old, k))
        return False


@pytest.fixture
def testing_jhcfg(tmp_path):
    with MockJhcfg(
        project=dict(log_dir=tmp_path / "log" / "project"),
        repo_watcher=dict(watched_repos=["."]),
        scheduler={
            "name": "tests.fake_slurm.FakeSlurmScheduler",
            "config": dict(log_dir=tmp_path / "log" / "job"),
        },
        commands=dict(
            generate_data="tests.example_cmds.GenerateDataArg",
            sum_data="tests.example_cmds.SumDataArg",
        ),
    ):
        yield


@pytest.fixture
def slurm_server(monkeypatch, request):
    monkeypatch.setenv(
        "PATH", str(Path(__file__).parent / "fake_slurm_cmds"), prepend=os.pathsep
    )
    monkeypatch.setattr("job_helper.slurm_helper._env0", os.environ.copy())
    with SlurmServer(getattr(request, "param", ServerState())) as s:
        yield s


def run_jh(cmd: str):
    if Path("pyproject.toml").exists():
        cfg_src = toml.load("pyproject.toml")["tool"]["job_helper"]
    else:
        cfg_src = {}
    with (
        patch.object(sys, "argv", shlex.split(cmd)),
        MockJhcfg(**cfg_src),
    ):
        console_main()
