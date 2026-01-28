"""Tests for JobParallelArg class."""

import pytest

from job_helper.project_helper import JobConfig, JobParallelArg, Project
from job_helper.scheduler import JobPreamble


def test_job_parallel_simple():
    """Test JobParallelArg with simple JobConfig."""
    project = Project(jobs={})
    parallel_job = JobParallelArg(
        jobs=[
            JobConfig(
                command="shell",
                config={"sh": "echo 'Job 1'"},
                job_preamble=JobPreamble(),
            ),
            JobConfig(
                command="shell",
                config={"sh": "echo 'Job 2'"},
                job_preamble=JobPreamble(),
            ),
            JobConfig(
                command="shell",
                config={"sh": "echo 'Job 3'"},
                job_preamble=JobPreamble(),
            ),
        ],
        ntasks_per_job=1,
    )

    script = parallel_job.script(project)

    # Check that the script contains the expected elements
    assert "srun --exact -n 1" in script
    assert "python -m fire" in script
    assert script.count("&") == 3  # Three background jobs
    assert "wait" in script
    assert "echo 'Job 1'" in script
    assert "echo 'Job 2'" in script
    assert "echo 'Job 3'" in script


def test_job_parallel_custom_ntasks():
    """Test JobParallelArg with custom ntasks_per_job."""
    project = Project(jobs={})
    parallel_job = JobParallelArg(
        jobs=[
            JobConfig(
                command="shell",
                config={"sh": "echo 'Multi-task job'"},
                job_preamble=JobPreamble(),
            ),
        ],
        ntasks_per_job=4,
    )

    script = parallel_job.script(project)
    assert "srun --exact -n 4" in script
    assert "wait" in script


def test_job_parallel_multiple_jobs():
    """Test JobParallelArg with multiple different jobs."""
    project = Project(jobs={})
    jobs = [
        JobConfig(
            command="shell",
            config={"sh": "python script1.py"},
            job_preamble=JobPreamble(),
        ),
        JobConfig(
            command="shell",
            config={"sh": "python script2.py"},
            job_preamble=JobPreamble(),
        ),
        JobConfig(
            command="shell",
            config={"sh": "python script3.py"},
            job_preamble=JobPreamble(),
        ),
    ]

    parallel_job = JobParallelArg(
        jobs=jobs,
        ntasks_per_job=1,
    )

    script = parallel_job.script(project)

    # Should have three background jobs
    assert script.count("srun --exact -n 1") == 3
    assert script.count("&") == 3
    assert "wait" in script
    assert "python script1.py" in script
    assert "python script2.py" in script
    assert "python script3.py" in script


def test_job_parallel_invalid_command():
    """Test JobParallelArg raises error for non-JobArgBase command."""
    project = Project(jobs={})
    # Try to use a command that's not a JobArgBase
    with pytest.raises(KeyError):
        parallel_job = JobParallelArg(
            jobs=[
                JobConfig(
                    command="nonexistent.InvalidCommand",
                    config={},
                    job_preamble=JobPreamble(),
                ),
            ],
            ntasks_per_job=1,
        )
        parallel_job.script(project)
