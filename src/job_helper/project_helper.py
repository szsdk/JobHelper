from __future__ import annotations

import copy
import pydoc
import subprocess
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Optional, Union, cast

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, validate_call

from ._mermaid_backend import flowchart, render_chart
from .arg import ArgBase, JobArgBase
from .config import ProjectConfig as JHProjectConfig
from .config import jhcfg
from .repo_watcher import RepoState, RepoWatcher
from .scheduler import JobPreamble
from .slurm_helper import JobInfo, parse_sacct_output


class ShellCommand(JobArgBase):
    sh: Annotated[str, Field(description="The shell command to be run")] = ""

    def script(self) -> str:
        return self.sh


class ProjectArgBase(ArgBase):
    def script(self, project: Project) -> str:
        raise NotImplementedError


class JobConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    command: str
    config: dict[str, Any]
    job_preamble: JobPreamble = JobPreamble()


class ProjectConfig(ArgBase):
    jobs: dict[str, JobConfig]

    @classmethod
    @validate_call
    def from_config(cls, *paths: Union[str, Path]):
        """Load multiple project configuration files and merge their job definitions.

        Notes:
            - Each configuration file is loaded and its jobs are collected.
            - If two or more configuration files define jobs with the same name, an error is raised.
            - The resulting instance contains all unique jobs from the provided configurations.

        Args:
            paths: One or more paths to project configuration files.
        """
        projects: dict[str, ProjectConfig] = dict()
        jobs = set()
        for cfg_fn in paths:
            prj = cast(ProjectConfig, super().from_config(cfg_fn))
            if not jobs.intersection(prj.jobs.keys()):
                jobs = jobs.union(prj.jobs.keys())
                projects[str(cfg_fn)] = prj
            else:
                for p_fn, p in projects.items():
                    overlap = jobs.intersection(prj.jobs.keys())
                    if len(overlap) > 0:
                        raise ValueError(
                            f"Job names {overlap} are defined in both {cfg_fn} and {p_fn}."
                        )
        all_jobs = dict()
        for p in projects.values():
            all_jobs.update(p.jobs)
        return cls(jobs=all_jobs)

    def _get_job_torun(
        self, scheduler, joblist, run_following
    ) -> OrderedDict[str, JobConfig]:
        jobs = copy.copy(self.jobs)
        jl = OrderedDict()
        for j in joblist.split(";"):
            if j not in jobs and j != "START":
                raise ValueError(
                    f"Job '{j}' not found in project jobs {tuple(jobs.keys())}."
                )
            jl[j] = jobs.pop(j, None)
        if not run_following:
            if "START" in jl:
                del jl["START"]
            return jl
        while True:
            changed = False
            for jobname, job in jobs.items():
                if job.job_preamble is None:
                    continue
                for d in scheduler.dependency(job.job_preamble):
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
        timeout: float = 5.0,
    ) -> Optional[str]:
        scheduler = jhcfg.get_scheduler()
        jobs_torun = self._get_job_torun(scheduler, reruns, run_following)
        nodes = {k: "norun" for k in self.jobs.keys() if k not in jobs_torun}
        links = {
            (job_a, job_b): link_type
            for job_b, job in self.jobs.items()
            for link_type in ["afterok", "after", "afternotok", "afterany"]
            for job_a in getattr(scheduler.dependency(job.job_preamble), link_type)
        }
        return render_chart(flowchart(nodes, links), output_fn, timeout=timeout)


class JobComboArg(ProjectArgBase):
    jobs: list[Union[str, ShellCommand, JobConfig]]

    def script(self, project: Project) -> str:
        cmds: list[str] = []
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


class CommandsManager:
    def __init__(self, cmd_map: dict[str, str]):
        self.commands = {"job_combo": JobComboArg, "shell": ShellCommand}
        for cmd in self.commands:
            if cmd in cmd_map:
                raise ValueError(f"{cmd} is reserved.")
        self._cmd_map = copy.deepcopy(cmd_map)

    def __getitem__(self, item) -> Union[type[JobArgBase], type[ProjectArgBase]]:
        if item in self.commands:
            return self.commands[item]
        if item in self._cmd_map:
            cmd = pydoc.locate(self._cmd_map[item])
        else:
            cmd = pydoc.locate(item)

        if isinstance(cmd, type) and (
            issubclass(cmd, JobArgBase) or issubclass(cmd, ProjectArgBase)
        ):
            self.commands[item] = cmd
            return cmd
        raise KeyError(f"{item} not found in commands")

    def __eq__(self, value, /) -> bool:
        if not isinstance(value, CommandsManager):
            return False
        return self._cmd_map == value._cmd_map


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
            start = datetime.now() if isinstance(info.Start, str) else info.Start
            end = datetime.now() if isinstance(info.End, str) else info.End

        if info.State in state_map:
            state = state_map[info.State]
        elif "CANCELLED" in info.State:
            state = "crit"
        else:
            state = "crit"
        mermaid_code += f"    {job_name} :{state}, {start.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}, {end.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]} \n    %% {job_name}: {info.JobID} {info.State}\n"

    return mermaid_code


class ProjectRunningResult(ArgBase):
    config: ProjectConfig
    jobs: dict[str, int]
    time: datetime = Field(default_factory=datetime.now)
    repo_states: list[RepoState] = Field(default_factory=list)

    def to_project(self) -> Project:
        return Project.model_validate(self.config.model_dump())

    def _job_states(self) -> dict[str, JobInfo]:
        """
        This function gets the current state of the jobs and generates a Gantt chart from it.
        """
        sacct_cmd = getattr(jhcfg.get_scheduler(), "sacct_cmd", None)
        if sacct_cmd is None:
            raise ValueError(
                "This function is only supported for Slurm (`sacct_cmd` should be given)."
            )
        id_to_name = {v: k for k, v in self.jobs.items()}
        result = subprocess.run(
            [
                sacct_cmd,
                "--jobs",
                ",".join(map(str, self.jobs.values())),
                "-ojobid,jobname,start,end,state",
                "-P",
                "-X",
            ],
            stdout=subprocess.PIPE,
        )
        return {
            id_to_name[job.JobID]: job
            for job in parse_sacct_output(result.stdout.decode())
        }

    def job_states(self, output_fn: str = "-", timeout: float = 5.0) -> Optional[str]:
        return render_chart(
            generate_mermaid_gantt_chart(self._job_states()), output_fn, timeout=timeout
        )

    def recover(self, yes=False, dry=True):
        job_states = self._job_states()
        # Check if the jobs are still running
        for job_name, job in job_states.items():
            if job.State in ["RUNNING"]:
                logger.warning(f"Job {job_name} is still running.")
        if not yes:
            print(
                "All jobs will be cancelled. And jobs not completed will be resubmitted. Continue? [y/N]"
            )
            yes = input().lower() == "y"
        if not yes:
            return
        not_completed = {
            job_name: job
            for job_name, job in job_states.items()
            if job.State != "COMPLETED"
        }
        if not dry:
            subprocess.run(["scancel"] + [str(i.JobID) for i in not_completed.values()])
        self.to_project().run(reruns=";".join(not_completed.keys()), dry=dry)


class Project(ProjectConfig):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    commands: CommandsManager = Field(
        default_factory=lambda: CommandsManager(jhcfg.commands),
        validate_default=True,
        exclude=True,
    )
    jh_config: JHProjectConfig = Field(
        default_factory=lambda: copy.deepcopy(jhcfg.project), exclude=True
    )

    def _run_jobs(
        self,
        scheduler,
        jobs,
        jobs_torun: OrderedDict[str, JobConfig],
        dry: bool,
        sleep_seconds: int,
    ):
        while len(jobs_torun) > 0:
            stack = []
            jobname, job = jobs_torun.popitem(last=False)
            stack.append((jobname, job))
            while len(stack) > 0:
                for j in scheduler.dependency(stack[-1][1].job_preamble):
                    if j in jobs_torun:
                        stack.append((j, jobs_torun.pop(j)))
                        break
                    if (j not in jobs) and (j != "START"):
                        logger.warning("Job {} not found in {}", j, jobs)
                else:
                    jobname, job = stack.pop()
                    job_arg = self.commands[job.command].model_validate(job.config)
                    assert job.job_preamble is not None
                    jobs[jobname] = scheduler.submit(
                        job.job_preamble,
                        job_arg.script(self)
                        if isinstance(job_arg, ProjectArgBase)
                        else job_arg.script(),
                        jobs,
                        jobname,
                        dry,
                    )
                    time.sleep(sleep_seconds)
        return jobs

    def run(
        self,
        reruns: str = "START",
        run_following: bool = True,
        dry: bool = True,
        sleep_seconds: int = 0,
    ) -> Optional[Path]:
        """
        sleep_seconds: controls the sleep time between two jobs
        """
        scheduler = jhcfg.get_scheduler()
        jobs_torun = self._get_job_torun(scheduler, reruns, run_following)
        repo_states = [] if dry else RepoWatcher.from_jhcfg().repo_states()
        jobs = self._run_jobs(scheduler, {}, jobs_torun, dry, sleep_seconds)
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

        result_fn = (
            self.jh_config.get_log_dir() / f"{jobs[list(jobs.keys())[0]].job_id}.json"
        )
        with result_fn.open("w") as fp:
            print(result.model_dump_json(), file=fp)
        logger.info(f"Running project {result_fn}, written to {result_fn}")
        return result_fn
