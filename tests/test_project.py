import json
import urllib.error

import pytest
import yaml

from job_helper import Project, ProjectConfig
from job_helper.project_helper import ProjectRunningResult, flowchart, render_chart
from tests.utils import slurm_server, testing_jhcfg


@pytest.fixture(scope="session")
def project_cfg(tmpdir_factory):
    dir = tmpdir_factory.mktemp("project_1")
    print(dir)
    data_fn = dir / "c.txt"
    return f"""
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
    job_preamble:
      dependency:
        after:
          - START
  job_sleep:
    command: shell
    config:
      sh: sleep 0.4
    job_preamble:
      dependency:
        afternotok:
          - START
  sum_data:
    command: sum_data
    config:
      input_fn: {data_fn}
      output_fn: {dir / "sum.txt"}
    job_preamble:
      dependency:
        - job_1
        - job_sleep
"""


def test_project_load(project_cfg, tmp_path):
    p0 = Project(**yaml.safe_load(project_cfg))
    with open(tmp_path / "project_1.yaml", "w") as f:
        print(project_cfg, file=f)
    assert p0 == Project.from_config(tmp_path / "project_1.yaml")

    with open(tmp_path / "project_1.json", "w") as f:
        print(json.dumps(p0.model_dump()), file=f)
    assert p0 == Project.from_config(tmp_path / "project_1.json")


@pytest.mark.parametrize("output_fn", ["-", "job_flow.png", "job_flow.svg"])
def test_jobflow(output_fn, project_cfg, tmp_path):
    print(str(tmp_path / output_fn))
    try:
        ProjectConfig.model_validate(yaml.safe_load(project_cfg)).jobflow(
            output_fn="-" if output_fn == "-" else str(tmp_path / output_fn),
            timeout=2.0,
        )
    except TimeoutError:
        pytest.skip("Kroki server timeout")
    except urllib.error.HTTPError as e:
        pytest.skip(e.read().decode())


def test_flowchart(tmp_path):
    try:
        render_chart(
            flowchart(
                nodes={
                    "job_1": "norun",
                    "job_2": "failed",
                    "job_3": "completed",
                },
                links={("job_1", "job_3"): "afterok", ("job_2", "job_3"): "afterany"},
            ),
            tmp_path / "t.png",
            timeout=2.0,
        )
    except TimeoutError:
        pytest.skip("Kroki server timeout")
    except urllib.error.HTTPError as e:
        pytest.skip(e.read().decode())


def test_project(project_cfg, slurm_server, testing_jhcfg):
    project_1 = Project(**yaml.safe_load(project_cfg))
    data = list(range(project_1.jobs["generate_data"].config["count"]))
    project_1.run(dry=False)
    slurm_server.complete_all()

    with open(project_1.jobs["sum_data"].config["input_fn"], "r") as f:
        input_data = list(map(int, f.read().split()))
    assert input_data == data, (
        f"Data in {project_1.jobs['sum_data'].config['input_fn']} does not match expected range."
    )

    with open(project_1.jobs["sum_data"].config["output_fn"], "r") as f:
        output_data = int(f.read().strip())
    assert output_data == sum(data), (
        f"Sum in {project_1.jobs['sum_data'].config['output_fn']} does not match expected sum."
    )

    project_1.jobs["generate_data"].config["count"] = 20
    data = list(range(project_1.jobs["generate_data"].config["count"]))

    project_output_fn = project_1.run(dry=False, reruns="job_1", run_following=False)
    assert project_output_fn is not None
    ProjectRunningResult.from_config(project_output_fn).job_states()

    slurm_server.complete_all()

    with open(project_1.jobs["generate_data"].config["output_fn"], "r") as f:
        output_data = list(map(int, f.read().split()))
    assert output_data == data, (
        f"Data in {project_1.jobs['generate_data'].config['output_fn']} does not match expected range."
    )

    project_1.run(dry=False, reruns="job_sleep")
    slurm_server.complete_all()

    with open(project_1.jobs["sum_data"].config["output_fn"], "r") as f:
        output_data = int(f.read().strip())
    assert output_data == sum(data), (
        f"Sum in {project_1.jobs['sum_data'].config['output_fn']} does not match expected sum."
    )
