from __future__ import annotations

import copy
import os
import subprocess
import sys
from datetime import datetime
from typing import Any, Iterable, Literal, Optional, Union

from loguru import logger
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from .config import JobHelperConfig, jhcfg
from .config import SlurmConfig as JHSlurmConfig

_env0 = (  # noqa: E402
    os.environ.copy()
)  # It should be before importing other modules, especially `mpi4py`.


class JobInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")
    JobID: int
    State: str
    Start: Union[datetime, Literal["Unknown"]] = "Unknown"
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


class SlrumDependency(BaseModel):
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
                    logger.warning(f"job {j} not found")
        return ans


class JobPreamble(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    dependency: Any = Field(default_factory=list)


class SlurmConfig(JobPreamble):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    job_name: str = ""
    dependency: SlrumDependency = SlrumDependency()
    output: str = Field(default_factory=lambda: f"{jhcfg.slurm.log_dir}/%j.out")

    @model_validator(mode="before")
    @classmethod
    def default_and_replace_underscore(cls, v):
        if isinstance(v, dict):
            # TODO: read defaults from jhcfg
            v = {k.replace("-", "_"): v for k, v in v.items()}
        return v

    @field_validator("dependency", mode="before")
    @classmethod
    def from_list(cls, v):
        if isinstance(v, list):
            return SlrumDependency(afterok=v)
        return v

    def preamble(self):
        preamble = []
        for k, v in self:
            if v is None:
                continue
            if k == "dependency":
                v = v.slurm_str() if isinstance(v, SlrumDependency) else v
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
    jh_config: JHSlurmConfig = Field(default_factory=lambda: copy.deepcopy(jhcfg.slurm))

    def set_slurm(self, **kwargs) -> Slurm:
        for k, v in kwargs.items():
            k = k.replace("_", "-")
            setattr(self.config, k, v)
        return self

    @computed_field
    @property
    def script(self) -> str:
        """
        Generate the script to be submitted to the cluster.
        """
        cmds = [f"#!{self.jh_config.shell}"]
        cmds.append(self.config.preamble())
        cmds.append(self.run_cmd)
        return "\n".join(cmds)

    def sbatch(self, dry: bool = True, save_script: bool = True) -> Slurm:
        """
        Submit the job to the cluster.
        If `dry` is True, it only prints the script. Otherwise (--nodry), it submits the job.
        """
        sbatch_cmd = self.jh_config.sbatch_cmd
        slurm_script = "\n".join(
            [f'{sbatch_cmd} --parsable << "EOF"', self.script, "EOF"]
        )
        print(self.script)
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
        self.job_id = int(stdout)
        logger.info("Submitted batch job {}", stdout)
        if save_script:
            with (self.jh_config.log_dir / f"{self.job_id}_slurm.sh").open("w") as fp:
                print(self.script, file=fp)
        return self

    def __str__(self) -> str:
        return f"{type(self).__name__}(job id: {self.job_id})"
