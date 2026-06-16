"""Tests for JobArrayArg class."""

import json
import os
import re
import subprocess

from job_helper.project_helper import JobArrayArg, JobConfig, Project
from job_helper.scheduler import JobPreamble


def _config_from_script(script: str) -> dict:
    match = re.search(r"cat > \"\$payload\" <<'EOF'\n(.+)\nEOF", script)
    assert match is not None
    return json.loads(match.group(1))


def test_job_array_script_contains_payload():
    project = Project(
        jobs={
            "array_job": JobConfig(
                command="job_array",
                config={
                    "jobs": [
                        {"command": "shell", "config": {"sh": "echo job-0"}},
                        {"command": "shell", "config": {"sh": "echo job-1"}},
                    ]
                },
                job_preamble=JobPreamble(array="0-1"),
            )
        }
    )

    job_arg = project.commands["job_array"].model_validate(
        project.jobs["array_job"].config
    )
    script = job_arg.script(project)
    config = _config_from_script(script)

    assert "#SBATCH" not in script
    assert "python -m fire job_helper.project_helper JobArrayArg" in script
    assert "fetch-job" in script
    assert "script" in script
    assert config["jobs"] == [
        {
            "command": "shell",
            "config": {"sh": "echo job-0"},
            "job_preamble": {"dependency": []},
        },
        {
            "command": "shell",
            "config": {"sh": "echo job-1"},
            "job_preamble": {"dependency": []},
        },
    ]


def test_job_array_supports_named_project_jobs():
    project = Project(
        jobs={
            "job_0": JobConfig(command="shell", config={"sh": "echo job-0"}),
            "job_1": JobConfig(command="shell", config={"sh": "echo job-1"}),
            "array_job": JobConfig(
                command="job_array",
                config={"jobs": ["job_0", "job_1"], "index_base": 1},
                job_preamble=JobPreamble(array="1-2"),
            ),
        }
    )

    job_arg = project.commands["job_array"].model_validate(
        project.jobs["array_job"].config
    )
    config = _config_from_script(job_arg.script(project))

    assert config["jobs"] == [
        {
            "command": "shell",
            "config": {"sh": "echo job-0"},
            "job_preamble": {"dependency": []},
        },
        {
            "command": "shell",
            "config": {"sh": "echo job-1"},
            "job_preamble": {"dependency": []},
        },
    ]
    assert config["index_base"] == 1


def test_job_array_fetch_job_returns_selected_script():
    job_array = JobArrayArg(
        jobs=[
            JobConfig(command="shell", config={"sh": "echo job-0"}),
            JobConfig(command="shell", config={"sh": "echo job-1"}),
        ]
    )

    job_arg = job_array.fetch_job(1)
    assert hasattr(job_arg, "script")
    assert job_arg.script() == "echo job-1"


def test_job_array_fetch_job_fire_command(tmp_path):
    payload = tmp_path / "job-array.json"
    payload.write_text(
        json.dumps(
            {
                "jobs": [
                    {"command": "shell", "config": {"sh": "echo job-0"}},
                    {"command": "shell", "config": {"sh": "echo job-1"}},
                ]
            }
        )
    )

    result = subprocess.run(
        [
            "python",
            "-m",
            "fire",
            "job_helper.project_helper",
            "JobArrayArg",
            "from-config",
            str(payload),
            "-",
            "fetch-job",
            "-",
            "script",
        ],
        env={**os.environ, "PYTHONPATH": "src", "SLURM_ARRAY_TASK_ID": "1"},
        stdout=subprocess.PIPE,
        check=True,
    )

    assert result.stdout.decode().strip() == "echo job-1"
