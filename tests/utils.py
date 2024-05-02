import copy
import os
import shlex
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import toml
from job_helper import jhcfg
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
                setattr(jhcfg, k, cls(**v))
            else:
                setattr(jhcfg, k, v)

        JobHelperConfig.model_validate(jhcfg.model_dump())
        # Since cmd_logger and rich_console are cached_property, we need to refresh them. But before that, we need to make sure they exist then we can delete them.
        jhcfg.cmd_logger
        jhcfg.rich_console
        del jhcfg.cmd_logger
        del jhcfg.rich_console
        jhcfg.cmd_logger
        jhcfg.rich_console

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for k in self._config.keys():
            setattr(jhcfg, k, getattr(self._old, k))
        del jhcfg.cmd_logger
        del jhcfg.rich_console
        jhcfg.cmd_logger
        jhcfg.rich_console
        return False


@pytest.fixture
def testing_jhcfg(tmp_path):
    with MockJhcfg(
        project=dict(log_dir=tmp_path / "log" / "project"),
        repo_watcher=dict(watched_repos=["."]),
        slurm=dict(log_dir=tmp_path / "log" / "job"),
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


def run_jh(cmd: str, cfg_src=Path("jh_config.toml")):
    if isinstance(cfg_src, Path):
        cfg_src = toml.load(cfg_src) if cfg_src.exists() else {}
    with (
        patch.object(sys, "argv", shlex.split(cmd)),
        MockJhcfg(**cfg_src),
    ):
        console_main()
