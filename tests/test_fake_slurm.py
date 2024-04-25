from datetime import datetime

import tests.fake_slurm as fs
from tests.fake_slurm import JobInfo


def test_sacct():
    init_state = fs.ServerState(
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
    with fs.SlurmServer(init_state):
        assert fs.sacct(jobs=[1, 2, 3]) == init_state
