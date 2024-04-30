from __future__ import annotations

import base64
import copy
import json
import logging
import pydoc
import urllib.request
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any, ClassVar, Union

import toml
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .arg import PDArgBase
from .config import ProjectConfig as JHProjectConfig
from .config import jhcfg
from .repo_watcher import RepoState, RepoWatcher
from .slurm_helper import Slurm


def _get_slurm_config(
    jobname: str, slurm_config: SlurmConfig, jobs: dict[str, Slurm], dry: bool
) -> dict[str, str]:
    ans = copy.deepcopy(slurm_config.model_dump())
    ans["dependency"] = slurm_config.dependency.slurm_str(jobs, dry)
    print(ans["dependency"])
    if ans["dependency"] == "":
        del ans["dependency"]
    for k, v in ans.items():
        ans[k] = str(v)
    ans["job-name"] = jobname
    return ans


class ShellCommand(PDArgBase):
    sh: str

    def slurm(self) -> Slurm:
        return Slurm(run_cmd=self.sh)


class ProjectArgBase(PDArgBase):
    def slurm(self, project: Project) -> Slurm:
        raise NotImplementedError


class SlrumDependency(BaseModel):
    after: list[str] = Field(default_factory=list)
    afterany: list[str] = Field(default_factory=list)
    afternotok: list[str] = Field(default_factory=list)
    afterok: list[str] = Field(default_factory=list)
    singleton: bool = False  # Placeholder; TODO: Implement this

    def __iter__(self):
        for k in ["after", "afterany", "afternotok", "afterok"]:
            yield from getattr(self, k)

    def slurm_str(self, jobs, dry: bool) -> str:
        ans = []
        for k in ["after", "afterany", "afternotok", "afterok"]:
            js = getattr(self, k)
            ansk = []
            for j in js:
                if j in jobs:
                    if dry:
                        ansk.append(j)
                    elif jobs[j].job_id is not None:
                        ansk.append(str(jobs[j].job_id))
                else:
                    logging.warning(f"job {j} not found")
            ans.append(f"{k}:{':'.join(ansk)}")
        return ",".join(ans)


class SlurmConfig(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)
    dependency: SlrumDependency = SlrumDependency()

    @field_validator("dependency", mode="before")
    @classmethod
    def from_list(cls, v):
        if isinstance(v, list):
            print(SlrumDependency(afterok=v))
            return SlrumDependency(afterok=v)
        return v


class JobConfig(PDArgBase):
    command: str
    config: dict[str, Any]
    slurm_config: SlurmConfig = SlurmConfig()


def flowchart(nodes: dict[str, str], links: dict[tuple[str, str], str]):
    node_styles = {
        "norun": "classDef norun fill:#ddd,stroke:#aaa,stroke-width:3px,stroke-dasharray: 5 5",
        "failed": "classDef failed fill:#eaa,stroke:#e44",
        "completed": "classDef completed fill:#aea,stroke:#4a4",
    }
    link_styles = {
        "after": "--o",
        "afterany": "-.-o",
        "afternotok": "-.-x",
        "afterok": "-->",
    }

    flow = ["flowchart TD"]
    for (job_a, job_b), link in links.items():
        a = job_a if job_a not in nodes else f"{job_a}:::{nodes[job_a]}"
        b = job_b if job_b not in nodes else f"{job_b}:::{nodes[job_b]}"
        flow.append(f"    {a} {link_styles[link]} {b}")
    flow.extend(list(node_styles.values()))
    return "\n".join(flow)


def render_chart(chart: str, output_fn: str):
    if output_fn == "-":
        print(chart)
        return
    output = Path(output_fn)
    url = base64.urlsafe_b64encode(zlib.compress(chart.encode(), 9)).decode("ascii")
    if output.suffix == ".png":
        url = "https://kroki.io/mermaid/png/" + url
    elif output.suffix == ".svg":
        url = "https://kroki.io/mermaid/svg/" + url
    else:
        raise ValueError(f"Unsupported output format: {output.suffix}")
    print(url)
    hdr = {"User-Agent": "Mozilla/5.0"}
    req = urllib.request.Request(url, headers=hdr)
    with urllib.request.urlopen(req) as response, output.open("wb") as fp:
        fp.write(response.read())


class ProjectConfig(PDArgBase):
    jobs: dict[str, JobConfig]
    node_styles: ClassVar[dict[str, str]] = {
        "norun": "classDef norun fill:#ddd,stroke:#aaa,stroke-width:3px,stroke-dasharray: 5 5"
    }

    def _get_job_torun(self, joblist, run_following) -> dict[str, JobConfig]:
        jobs = copy.deepcopy(self.jobs)
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

    def jobflow(
        self,
        reruns: str = "START",
        run_following: bool = True,
        output_fn="-",
    ):
        jobs_torun = self._get_job_torun(reruns, run_following)
        nodes = {k: "norun" for k in self.jobs.keys() if k not in jobs_torun}
        links = {
            (job_a, job_b): link_type
            for job_b, job in self.jobs.items()
            for link_type in ["afterok", "after", "afternotok", "afterany"]
            for job_a in getattr(job.slurm_config.dependency, link_type)
        }
        render_chart(flowchart(nodes, links), output_fn)


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
        return Slurm(run_cmd="\n".join(cmds))


def _get_commands():
    ans = {}
    for cmd, arg_class in jhcfg.commands.items():
        arg = pydoc.locate(arg_class)
        if isinstance(arg, type) and issubclass(arg, PDArgBase):
            ans[cmd] = arg
    return ans


class ProjectRunningResult(BaseModel):
    config: ProjectConfig
    jobs: dict[str, int]
    time: datetime = Field(default_factory=datetime.now)
    repo_states: list[RepoState] = Field(default_factory=list)


class Project(BaseModel):
    commands: dict[str, type[PDArgBase]] = Field(
        default_factory=_get_commands, validate_default=True
    )
    config: ProjectConfig
    jh_config: JHProjectConfig = Field(
        default_factory=lambda: copy.deepcopy(jhcfg.project)
    )

    @field_validator("commands", mode="after")
    @classmethod
    def add_default_commands(cls, v):
        for cmd in ["job_combo", "shell"]:
            if cmd in v:
                raise ValueError(f"{cmd} is reserved.")
        v.update({"job_combo": JobComboArg, "shell": ShellCommand})
        return v

    @field_validator("config", mode="before")
    def config_from_file(cls, v):
        if not isinstance(v, (str, Path)):
            return v
        fn = Path(v)
        if fn.suffix == ".yaml":
            return yaml.safe_load(fn.read_text())
        elif fn.suffix == ".toml":
            return toml.load(fn)
        elif fn.suffix == ".json":
            return json.loads(fn.read_text())
        raise ValueError(f"Unsupported config file: {fn}")

    def __getitem__(self, key):
        logging.info(f"commands: {self.commands}")
        return self.commands[key]

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
        jobs_torun = self.config._get_job_torun(reruns, run_following)
        repo_states = [] if dry else RepoWatcher.from_jhcfg().repo_states()
        jobs = self._run_jobs({}, jobs_torun, dry)
        if len(jobs) == 0:
            jhcfg.cmd_logger.warning("No jobs to run")
            return
        if not dry:
            self._output_running_result(
                jobs,
                repo_states,
            )

    def _output_running_result(self, jobs, repo_states) -> None:
        result = ProjectRunningResult(
            jobs={k: v.job_id for k, v in jobs.items()},
            config=self.config,
            repo_states=repo_states,
        )

        result_fn = self.jh_config.log_dir / f"{jobs[list(jobs.keys())[0]].job_id}.json"
        with result_fn.open("w") as fp:
            print(result.model_dump_json(), file=fp)
        jhcfg.cmd_logger.info(f"Running project {result_fn}, written to {result_fn}")

    def jobflow(
        self, reruns: str = "START", run_following: bool = True, output_fn: str = "-"
    ):
        self.config.jobflow(reruns, run_following, output_fn)
