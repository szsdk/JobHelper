import copy

import pytest
from job_helper import jhcfg
from pydantic import BaseModel


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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for k in self._config.keys():
            setattr(jhcfg, k, getattr(self._old, k))
        return False


@pytest.fixture
def testing_jhcfg(tmp_path):
    with MockJhcfg(
        project=dict(log_dir=tmp_path / "log" / "project"),
        repo_watcher=dict(repos=["."]),
        slurm=dict(
            log_dir=tmp_path / "log" / "job",
            sbatch_cmd="python tests/fake_slurm.py sbatch",
            sacct_cmd="python tests/fake_slurm.py sacct",
        ),
        commands=dict(
            generate_data="tests.example_cmds.GenerateDataArg",
            sum_data="tests.example_cmds.SumDataArg",
            project="job_helper.Project",
            tools="job_helper.tools",
        ),
    ):
        yield
