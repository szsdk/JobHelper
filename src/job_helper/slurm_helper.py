from __future__ import annotations

import copy
import os
import subprocess
import sys
from datetime import datetime
from typing import Annotated, Iterable, Literal, Optional, Union

from loguru import logger
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .config import DirExists, jhcfg
from .scheduler import JobPreamble, Scheduler

_env0 = (  # noqa: E402
    os.environ.copy()
)  # It should be before importing other modules, especially `mpi4py`.


class JobInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")
    JobID: int
    State: str
    Start: Union[datetime, Literal["Unknown"], Literal["None"]] = "Unknown"
    End: Union[datetime, Literal["Unknown"]] = "Unknown"


def parse_sacct_output(s) -> list[JobInfo]:
    lines = s.splitlines()
    if len(lines) < 2:
        return []
    header = lines[0].split("|")
    ans = []
    for line in lines[1:]:
        ans.append(JobInfo(**dict(zip(header, line.split("|")))))
    return ans


class SlurmDependency(BaseModel):
    after: list[str] = Field(default_factory=list)
    afterany: list[str] = Field(default_factory=list)
    afternotok: list[str] = Field(default_factory=list)
    afterok: list[str] = Field(default_factory=list)
    singleton: bool = False  # Placeholder; TODO: Implement this

    def __iter__(self):
        for k in ["after", "afterany", "afternotok", "afterok"]:
            yield from getattr(self, k)

    def slurm_str(self) -> str:
        ans = []
        for k in ["after", "afterany", "afternotok", "afterok"]:
            js = getattr(self, k)
            if len(js) > 0:
                ans.append(f"{k}:{':'.join(js)}")
        return ",".join(ans)

    def replace_with_job_id(self, jobs, dry: bool):
        ans = copy.deepcopy(self)
        for k in ["after", "afterany", "afternotok", "afterok"]:
            js = getattr(self, k)
            ansk = getattr(ans, k)
            ansk.clear()
            for j in js:
                if j in jobs:
                    if dry:
                        ansk.append(j)
                    elif jobs[j].job_id is not None:
                        ansk.append(str(jobs[j].job_id))
                else:
                    if j != "START":
                        logger.warning("job {} not found", j)
        return ans


class SlurmConfig(JobPreamble):
    """
    It is a class for configuring the Slurm job scheduler. It should be noted that all keys are in long format (e.g., `job_name` instead of `-J`).
    """

    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    job_name: str = ""
    dependency: SlurmDependency = SlurmDependency()
    output: str = Field(
        default_factory=lambda: f"{SlurmScheduler.model_validate(jhcfg.scheduler.config).log_dir.get_path()}/%j.out"
    )

    @model_validator(mode="before")
    @classmethod
    def default_and_replace_underscore(cls, v):
        if isinstance(v, dict):
            v = {k.replace("-", "_"): v for k, v in v.items()}
        return v

    @field_validator("dependency", mode="before")
    @classmethod
    def from_list(cls, v):
        if isinstance(v, list):
            return SlurmDependency(afterok=v)
        return v

    def preamble(self):
        preamble = []
        for k, v in self:
            if v is None:
                continue
            if k == "dependency":
                v = v.slurm_str() if isinstance(v, SlurmDependency) else v
                if v == "":
                    continue
            else:
                v = str(v)
            preamble.append(f"#SBATCH --{k.replace('_', '-'):<19} {v}")
        return "\n".join(preamble)


class Slurm(BaseModel):
    """
    This class provides a simple interface for submitting jobs to a cluster (Slurm).
    """

    run_cmd: str
    job_id: Optional[int] = None
    config: SlurmConfig = Field(
        default_factory=lambda: SlurmConfig(), validate_default=True
    )

    def set_slurm(self, **kwargs) -> Slurm:
        for k, v in kwargs.items():
            k = k.replace("_", "-")
            setattr(self.config, k, v)
        return self

    def sbatch(self, dry: bool = True):
        SlurmScheduler.model_validate(jhcfg.scheduler.config).sbatch(self, dry=dry)

    def __str__(self) -> str:
        return f"{type(self).__name__}(job id: {self.job_id})"


class SlurmScheduler(Scheduler):
    shell: str = "/bin/sh"
    sbatch_cmd: Annotated[str, Field(description="sbatch command")] = "sbatch"
    sacct_cmd: Annotated[str, Field(description="sacct command")] = "sacct"
    save_script: bool = Field(
        default=True, description="Save the script to the log_dir"
    )
    print_script: bool = Field(default=True, description="Print the script")

    def submit(
        self, config, job_script, jobs: dict[str, Slurm], jobname: str, dry: bool
    ):
        c = SlurmConfig.model_validate(config.model_dump())
        c.dependency = c.dependency.replace_with_job_id(jobs, dry)
        c.job_name = jobname
        job = Slurm(run_cmd=job_script, config=c)
        self.sbatch(job, dry=dry)
        return job

    def dependency(self, config) -> Iterable[int]:
        c = SlurmConfig.model_validate(config.model_dump())
        return c.dependency

    def script(self, job) -> str:
        """
        Generate the script to be submitted to the cluster.
        """
        cmds = [f"#!{self.shell}"]
        cmds.append(job.config.preamble())
        cmds.append(job.run_cmd)
        return "\n".join(cmds)

    def sbatch(self, job: Slurm, dry: bool = True):
        """
        Submit the job to the cluster.
        If `dry` is True, it only prints the script. Otherwise (--nodry), it submits the job.
        """
        script = self.script(job)
        sbatch_cmd = self.sbatch_cmd
        slurm_script = "\n".join([f'{sbatch_cmd} --parsable << "EOF"', script, "EOF"])
        if self.print_script:
            print(script)
        if dry:
            logger.info("It is a dry run.")
            return self

        result = subprocess.run(
            slurm_script, shell=True, stdout=subprocess.PIPE, env=_env0
        )
        stdout = result.stdout.decode("utf-8").strip()
        if result.returncode != 0:
            logger.error(result.stderr)
            sys.exit(1)
        job.job_id = int(stdout)
        logger.info("Submitted job {} to {}", job.config.job_name, job.job_id)
        if self.save_script:
            with (self.get_log_dir() / f"{job.job_id}_slurm.sh").open("w") as fp:
                print(script, file=fp)
        return self
