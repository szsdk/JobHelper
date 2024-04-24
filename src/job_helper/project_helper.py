from __future__ import annotations

import base64
import copy
import logging
import urllib.request
import zlib
from pathlib import Path
from typing import Any, Mapping, Protocol, TypeVar, Union

from pydantic import BaseModel, ConfigDict

from .arg import PDArgBase
from .slurm_helper import Slurm


def _get_slurm_config(
    jobname: str, slurm_config: SlurmConfig, jobs: dict[str, Slurm], dry: bool
) -> dict[str, str]:
    ans = copy.deepcopy(slurm_config.model_dump())
    jl = []
    for j in slurm_config.dependency:
        if j in jobs:
            if dry:
                jl.append(str(j))
            elif jobs[j].job_id is not None:
                jl.append(str(jobs[j].job_id))
            # jl.append(str(j if dry else jobs[j].job_id))
        else:
            logging.warning(f"job {j} not found")
    if len(jl) > 0:
        ans["dependency"] = "afterok:" + ":".join(jl)
    else:
        del ans["dependency"]
    for k, v in ans.items():
        ans[k] = str(v)
    ans["job-name"] = jobname
    return ans


class ShellCommand(BaseModel):
    sh: str

    def slurm(self) -> Slurm:
        return Slurm(self.sh)


class ProjectArgBase(PDArgBase):
    def slurm(self, project: Project) -> Slurm:
        raise NotImplementedError


class SlurmConfig(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    dependency: list[str] = []


class JobConfig(BaseModel):
    command: str
    config: dict[str, Any]
    slurm_config: SlurmConfig = SlurmConfig()


class ProjectConfig(BaseModel):
    jobs: dict[str, JobConfig]


class JobComboArg(ProjectArgBase):
    jobs: list[Union[str, ShellCommand, JobConfig]]

    def slurm(self, project: Project) -> Slurm:
        cmds = []
        for job in self.jobs:
            if isinstance(job, ShellCommand):
                cmds.append(job.sh)
                continue
            if isinstance(job, str):
                j = project.config.jobs[job]
            elif isinstance(job, JobConfig):
                j = job
            else:
                raise NotImplementedError

            j = project[j.command].model_validate(j.config)
            cmds.append(
                (
                    j.slurm(project) if isinstance(j, ProjectArgBase) else j.slurm()
                ).run_cmd
            )
        return Slurm("\n".join(cmds))


class Project:
    def __init__(self, env: Mapping[str, type[PDArgBase]], config):
        self.config = ProjectConfig.model_validate(config)
        self._env = env
        for cmd in ["job_combo", "shell"]:
            if cmd in self._env:
                raise ValueError(f"{cmd} is reserved.")
        self._default_env = {"job_combo": JobComboArg, "shell": ShellCommand}

    def __getitem__(self, key):
        if key in self._default_env:
            return self._default_env[key]
        return self._env[key]

    def _get_job_torun(self, joblist, run_following) -> dict[str, JobConfig]:
        jobs = copy.deepcopy(self.config.jobs)
        jl = {}
        for j in joblist.split(";"):
            jl[j] = jobs.pop(j, None)
        if not run_following:
            if "START" in jl:
                del jl["START"]
            return jl
        while True:
            changed = False
            for jobname, job in jobs.items():
                if job.slurm_config is None:
                    continue
                for d in job.slurm_config.dependency:
                    if d in jl:
                        jl[jobname] = jobs.pop(jobname)
                        changed = True
                        break
                if changed:
                    break
            if not changed:
                break
        if "START" in jl:
            del jl["START"]
        return jl

    def _run_jobs(self, jobs, jobs_torun: dict[str, JobConfig], dry: bool):
        while len(jobs_torun) > 0:
            jobname, job = jobs_torun.popitem()
            for j in job.slurm_config.dependency:
                if j in jobs_torun:
                    self._run_jobs(jobs, jobs_torun, dry)
            job_arg = self[job.command].model_validate(job.config)
            if job.slurm_config is not None:
                jobs[jobname] = (
                    (
                        job_arg.slurm(self)
                        if isinstance(job_arg, ProjectArgBase)
                        else job_arg.slurm()
                    ).set_slurm(
                        **_get_slurm_config(jobname, job.slurm_config, jobs, dry)
                    )
                ).sbatch(dry=dry)
        return jobs

    def run(
        self, reruns: str = "START", run_following: bool = True, dry: bool = True
    ) -> None:
        jobs_torun = self._get_job_torun(reruns, run_following)
        jobs = self._run_jobs({}, jobs_torun, dry)
        logging.info(jobs)

    def jobflow(
        self,
        reruns: str = "START",
        run_following: bool = True,
        output_fn="-",
    ):
        jobs_torun = self._get_job_torun(reruns, run_following)
        flow = ["flowchart TD"]
        for job_name, job in self.config.jobs.items():
            for d in job.slurm_config.dependency:
                node_d = d if d in jobs_torun else f"{d}:::norun"
                node_j = job_name if job_name in jobs_torun else f"{job_name}:::norun"
                flow.append(f"    {node_d} --> {node_j}")
                # flow.append(f"    {d} --> {job_name}")
        flow.append(
            "classDef norun fill:#ddd,stroke:#aaa,stroke-width:3px,stroke-dasharray: 5 5"
        )
        flow_src = "\n".join(flow)
        if output_fn == "-":
            print(flow_src)
        else:
            output_fn = Path(output_fn)
            url = base64.urlsafe_b64encode(zlib.compress(flow_src.encode(), 9)).decode(
                "ascii"
            )
            if output_fn.suffix == ".png":
                url = "https://kroki.io/mermaid/png/" + url
            elif output_fn.suffix == ".svg":
                url = "https://kroki.io/mermaid/svg/" + url
            else:
                raise ValueError(f"Unsupported output format: {output_fn.suffix}")
            print(url)
            hdr = {"User-Agent": "Mozilla/5.0"}
            req = urllib.request.Request(url, headers=hdr)
            with urllib.request.urlopen(req) as response, output_fn.open("wb") as fp:
                fp.write(response.read())
