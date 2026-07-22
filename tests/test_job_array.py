"""Tests for JobArrayArg class."""

import json
import os
import re
import subprocess

import pytest

from job_helper.project_helper import JobArrayArg, JobConfig, Project
from job_helper.scheduler import JobPreamble
from job_helper.slurm_helper import SlurmScheduler


def _config_from_script(script: str) -> dict:
    match = re.search(
        r"cat > \"\$payload\" <<'JOB_HELPER_JOB_ARRAY_PAYLOAD'\n"
        r"(.+)\nJOB_HELPER_JOB_ARRAY_PAYLOAD",
        script,
    )
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


def test_job_array_fetch_job_reuses_single_entry_for_any_array_id():
    job_array = JobArrayArg(
        jobs=[
            JobConfig(command="shell", config={"sh": "echo task-$SLURM_ARRAY_TASK_ID"})
        ]
    )

    assert job_array.fetch_job(0).script() == "echo task-$SLURM_ARRAY_TASK_ID"
    assert job_array.fetch_job(42).script() == "echo task-$SLURM_ARRAY_TASK_ID"


def test_job_array_fetch_job_rejects_out_of_range_multi_job_array_id():
    job_array = JobArrayArg(
        jobs=[
            JobConfig(command="shell", config={"sh": "echo job-0"}),
            JobConfig(command="shell", config={"sh": "echo job-1"}),
        ],
        index_base=1,
    )

    with pytest.raises(IndexError, match="Array task id 3 resolves to job index 2"):
        job_array.fetch_job(3)


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


def test_large_job_array_script_is_submitted_through_stdin(monkeypatch):
    large_command = "echo " + "x" * (1024 * 1024)
    project = Project(
        jobs={
            "array_job": JobConfig(
                command="job_array",
                config={
                    "jobs": [{"command": "shell", "config": {"sh": large_command}}]
                },
                job_preamble=JobPreamble(array="0-0"),
            )
        }
    )
    job_arg = project.commands["job_array"].model_validate(
        project.jobs["array_job"].config
    )
    submitted = {}

    def fake_run(command, **kwargs):
        submitted["command"] = command
        submitted.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout="123\n", stderr="")

    monkeypatch.setattr("job_helper.slurm_helper.subprocess.run", fake_run)
    scheduler = SlurmScheduler(print_script=False, save_script=False)

    job = scheduler.submit(
        project.jobs["array_job"].job_preamble,
        job_arg.script(project),
        {},
        "array_job",
        dry=False,
    )

    assert submitted["command"] == ["sbatch", "--parsable"]
    assert "shell" not in submitted
    assert len(submitted["input"].encode()) > 1024 * 1024
    assert "cat > \"$payload\" <<'JOB_HELPER_JOB_ARRAY_PAYLOAD'" in submitted["input"]
    assert submitted["text"] is True
    assert submitted["stdout"] is subprocess.PIPE
    assert submitted["stderr"] is subprocess.PIPE
    assert submitted["check"] is True
    assert job.job_id == 123
