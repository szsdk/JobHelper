import copy
from unittest.mock import patch

import numpy as np
import pytest
import yaml
from job_helper import Project

from tests.fake_slurm import SlurmServer
from tests.test_arg import GenerateDataArg, SumDataArg


@pytest.fixture(scope="session")
def project_1(tmpdir_factory):
    dir = tmpdir_factory.mktemp("project_1")
    print(dir)
    data_fn = dir / "c.txt"
    cfg_str = f"""
jobs:
  generate_data:
    command: generate_data
    config:
      count: 100
      output_fn: {data_fn}
  job_1:
    command: job_combo
    config:
      jobs: 
        - sh: sleep 1
        - generate_data
    slurm_config: 
      dependency:
      - START
  job_sleep:
    command: shell
    config:
      sh: sleep 0.4
    slurm_config: 
      dependency:
      - START
  sum_data:
    command: sum_data
    config:
      input_fn: {data_fn}
      output_fn: {dir / 'sum.txt'}
    slurm_config:
      dependency:
        - job_1
        - job_sleep
"""
    return Project(
        {"generate_data": GenerateDataArg, "sum_data": SumDataArg},
        yaml.safe_load(cfg_str),
    )


@patch("job_helper.config.jhcfg.slurm.sbatch_cmd", "python tests/fake_slurm.py client")
def test_project(project_1):
    project_1 = copy.deepcopy(project_1)
    data = np.arange(project_1.config.jobs["generate_data"].config["count"])
    with SlurmServer():
        project_1.run(dry=False)

    np.testing.assert_array_equal(
        np.loadtxt(
            project_1.config.jobs["sum_data"].config["input_fn"],
            dtype=int,
        ),
        data,
    )
    assert (
        np.loadtxt(project_1.config.jobs["sum_data"].config["output_fn"], dtype=int)
        == data.sum()
    )

    project_1.config.jobs["generate_data"].config["count"] = 20
    data = np.arange(project_1.config.jobs["generate_data"].config["count"])

    with SlurmServer():
        project_1.run(dry=False, reruns="job_1", run_following=False)

    np.testing.assert_array_equal(
        np.loadtxt(
            project_1.config.jobs["generate_data"].config["output_fn"], dtype=int
        ),
        data,
    )

    with SlurmServer():
        project_1.run(dry=False, reruns="job_sleep")
    assert (
        np.loadtxt(project_1.config.jobs["sum_data"].config["output_fn"], dtype=int)
        == data.sum()
    )
