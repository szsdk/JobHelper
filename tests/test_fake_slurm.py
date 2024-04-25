import subprocess
from datetime import datetime

import pytest
from job_helper import jhcfg
from job_helper.slurm_helper import parse_sacct_output

import tests.fake_slurm as fs
from tests.fake_slurm import JobInfo


@pytest.fixture
def init_state():
    return fs.ServerState(
        jobs={
            1: JobInfo(
                JobID=1,
                State="COMPLETED",
                Start=datetime(2020, 1, 1, 3, 1),
                End=datetime(2020, 1, 1, 4, 2),
            ),
            2: JobInfo(JobID=2, State="RUNNING", Start=datetime(2020, 1, 1, 5, 2)),
            3: JobInfo(JobID=3, State="PENDING"),
        },
        job_id=3,
    )


def test_sacct(init_state):
    print(jhcfg.slurm.sacct_cmd)
    with fs.SlurmServer(init_state):
        result = subprocess.run(
            " ".join([jhcfg.slurm.sacct_cmd, "--jobs", "1,2"]),
            shell=True,
            stdout=subprocess.PIPE,
        )
        assert parse_sacct_output(result.stdout.decode()) == [
            init_state.jobs[i] for i in [1, 2]
        ]
