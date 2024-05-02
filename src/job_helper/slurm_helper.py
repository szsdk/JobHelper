from __future__ import annotations

import copy
import datetime
import logging
import os
import subprocess
import sys
from datetime import datetime
from typing import Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from .config import JobHelperConfig, jhcfg
from .config import SlurmConfig as JHSlurmConfig

_env0 = (  # noqa: E402
    os.environ.copy()
)  # It should be before importing other modules, especially `mpi4py`.

_cmd_logger = logging.getLogger("_jb_cmd")


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


class Slurm(BaseModel):
    """
    This class provides a simple interface for submitting jobs to a cluster (Slurm).
    """

    run_cmd: str
    job_id: Optional[int] = None
    config: dict[str, str] = Field(default_factory=dict, validate_default=True)
    jh_config: JHSlurmConfig = Field(default_factory=lambda: copy.deepcopy(jhcfg.slurm))

    @field_validator("config", mode="before")
    @classmethod
    def default_and_replace_underscore(cls, v):
        v = {k.replace("_", "-"): v for k, v in v.items()}
        if "output" not in v:
            v["output"] = f"{jhcfg.slurm.log_dir}/%j.out"
        return v

    def set_slurm(self, **kwargs: str) -> Slurm:
        self.config.update({k.replace("_", "-"): v for k, v in kwargs.items()})
        return self

    @computed_field
    @property
    def script(self) -> str:
        """
        Generate the script to be submitted to the cluster.
        """
        cmds = [f"#!{jhcfg.slurm.shell}"]
        for k, v in self.config.items():
            if v is None:
                continue
            cmds.append(f"#SBATCH --{k:<19} {v}")
        cmds.append(self.run_cmd)
        return "\n".join(cmds)

    def sbatch(self, dry: bool = True, save_script: bool = True) -> Slurm:
        """
        Submit the job to the cluster.
        If `dry` is True, it only prints the script. Otherwise (--nodry), it submits the job.
        """
        sbatch_cmd = jhcfg.slurm.sbatch_cmd
        slurm_script = "\n".join(
            [f'{sbatch_cmd} --parsable << "EOF"', self.script, "EOF"]
        )
        print(self.script)
        if dry:
            logging.info("It is a dry run.")
            return self

        result = subprocess.run(
            slurm_script, shell=True, stdout=subprocess.PIPE, env=_env0
        )
        stdout = result.stdout.decode("utf-8").strip()
        if result.returncode != 0:
            logging.error(result.stderr)
            sys.exit(1)
        self.job_id = int(stdout)
        _cmd_logger.info("Submitted batch job %s", stdout)
        if save_script:
            with (self.jh_config.log_dir / f"{self.job_id}_slurm.sh").open("w") as fp:
                print(self.script, file=fp)
        return self

    def __str__(self) -> str:
        return f"{type(self).__name__}(job id: {self.job_id})"
