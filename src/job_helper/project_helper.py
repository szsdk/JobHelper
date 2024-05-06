from __future__ import annotations

import base64
import copy
import pydoc
import subprocess
import urllib.request
import zlib
from datetime import datetime
from pathlib import Path
from typing import Any, Optional, Union

from loguru import logger
from pydantic import Field, field_validator

from .arg import ArgBase, JobArgBase
from .config import ProjectConfig as JHProjectConfig
from .config import jhcfg
from .repo_watcher import RepoState, RepoWatcher
from .slurm_helper import Slurm, SlurmConfig, parse_sacct_output


class ShellCommand(JobArgBase):
    sh: str

    def script(self) -> str:
        return self.sh


class ProjectArgBase(ArgBase):
    def script(self, project: Project) -> str:
        raise NotImplementedError


class JobConfig(ArgBase):
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


_HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
  <body>
    <pre class="mermaid">
{mermaid_code}
    </pre>
    <script type="module">
      import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
    </script>
  </body>
</html>"""


def render_chart(chart: str, output_fn: str):
    if output_fn == "-":
        print(chart)
        return
    output = Path(output_fn)
    if output.suffix == ".html":
        with output.open("w") as fp:
            print(_HTML_TEMPLATE.format(mermaid_code=chart), file=fp)
        return
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


class ProjectConfig(ArgBase):
    jobs: dict[str, JobConfig]

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

    def script(self, project: Project) -> str:
        cmds = []
        for job in self.jobs:
            if isinstance(job, ShellCommand):
                cmds.append(job.sh)
                continue
            if isinstance(job, str):
                j = project.jobs[job]
            elif isinstance(job, JobConfig):
                j = job
            else:
                raise NotImplementedError

            j = project.commands[j.command].model_validate(j.config)
            cmds.append(
                j.script(project) if isinstance(j, ProjectArgBase) else j.script()
            )
        return "\n".join(cmds)


def _get_commands():
    ans = {}
    for cmd, arg_class in jhcfg.commands.items():
        arg = pydoc.locate(arg_class)
        if isinstance(arg, type) and issubclass(arg, ArgBase):
            ans[cmd] = arg
    return ans


def generate_mermaid_gantt_chart(jobs):
    """
    Generate Mermaid Gantt chart code from a dictionary of jobs.

    Parameters:
    - jobs: A dictionary where keys are job names and values are JobInfo instances.

    Returns:
    - A string containing the formatted Mermaid Gantt chart code.
    """
    # Start the Mermaid Gantt chart code
    mermaid_code = """gantt
    dateFormat  YYYY-MM-DDTHH:mm:ss.SSS
    axisFormat  %H:%M:%S
"""
    state_map = {
        "COMPLETED": "done",
        "FAILED": "crit",
        "RUNNING": "active",
        "PENDING": "milestone",
    }
    for job_name, info in jobs.items():
        if info.State == "PENDING":
            end = datetime.now()
            start = end
        elif info.State == "RUNNING":
            start = info.Start
            end = datetime.now()
        else:
            start = info.Start
            end = info.End

        mermaid_code += f"    {job_name} :{state_map[info.State]}, {start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}, {end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}\n"

    return mermaid_code


class ProjectRunningResult(ArgBase):
    config: ProjectConfig
    jobs: dict[str, int]
    time: datetime = Field(default_factory=datetime.now)
    repo_states: list[RepoState] = Field(default_factory=list)

    def to_project(self) -> Project:
        return Project.model_validate(self.config.model_dump())

    def job_states(self, output_fn: str = "-"):
        """
        This function gets the current state of the jobs and generates a Gantt chart from it.
        """
        id_to_name = {v: k for k, v in self.jobs.items()}
        result = subprocess.run(
            " ".join(
                [
                    jhcfg.slurm.sacct_cmd,
                    "--jobs",
                    ",".join(map(str, self.jobs.values())),
                ]
            ),
            shell=True,
            stdout=subprocess.PIPE,
        )
        job_states = {
            id_to_name[job.JobID]: job
            for job in parse_sacct_output(result.stdout.decode())
        }
        render_chart(generate_mermaid_gantt_chart(job_states), output_fn)


class Project(ProjectConfig):
    commands: dict[str, Union[type[JobArgBase], type[ProjectArgBase]]] = Field(
        default_factory=_get_commands, validate_default=True, exclude=True
    )
    jh_config: JHProjectConfig = Field(
        default_factory=lambda: copy.deepcopy(jhcfg.project), exclude=True
    )

    @field_validator("commands", mode="after")
    @classmethod
    def add_default_commands(cls, v):
        for cmd in ["job_combo", "shell"]:
            if cmd in v:
                raise ValueError(f"{cmd} is reserved.")
        v.update({"job_combo": JobComboArg, "shell": ShellCommand})
        return v

    def _run_jobs(self, jobs, jobs_torun: dict[str, JobConfig], dry: bool):
        while len(jobs_torun) > 0:
            jobname, job = jobs_torun.popitem()
            for j in job.slurm_config.dependency:
                if j in jobs_torun:
                    self._run_jobs(jobs, jobs_torun, dry)
            job_arg = self.commands[job.command].model_validate(job.config)
            if job.slurm_config is not None:
                c = copy.deepcopy(job.slurm_config)
                c.dependency = c.dependency.replace_with_job_id(jobs, dry)
                c.job_name = jobname
                jobs[jobname] = Slurm(
                    run_cmd=job_arg.script(self)
                    if isinstance(job_arg, ProjectArgBase)
                    else job_arg.script(),
                    config=c,
                )
                jobs[jobname].sbatch(dry=dry)
        return jobs

    def run(
        self, reruns: str = "START", run_following: bool = True, dry: bool = True
    ) -> Optional[Path]:
        jobs_torun = self._get_job_torun(reruns, run_following)
        repo_states = [] if dry else RepoWatcher.from_jhcfg().repo_states()
        jobs = self._run_jobs({}, jobs_torun, dry)
        if len(jobs) == 0:
            logger.warning("No jobs to run")
            return
        if not dry:
            return self._output_running_result(
                jobs,
                repo_states,
            )

    def _output_running_result(self, jobs, repo_states) -> Path:
        result = ProjectRunningResult(
            jobs={k: v.job_id for k, v in jobs.items()},
            config=self,
            repo_states=repo_states,
        )

        result_fn = self.jh_config.log_dir / f"{jobs[list(jobs.keys())[0]].job_id}.json"
        with result_fn.open("w") as fp:
            print(result.model_dump_json(), file=fp)
        logger.info(f"Running project {result_fn}, written to {result_fn}")
        return result_fn
