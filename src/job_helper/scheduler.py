from typing import Iterable

from pydantic import BaseModel, ConfigDict

from .config import jhcfg
from .slurm_helper import Slurm, SlurmConfig


class Scheduler(BaseModel):
    def submit(self, config, job_script, jobs, jobname: str, dry: bool): ...
    def dependency(self, config) -> Iterable[int]: ...


class SlurmScheduler(Scheduler):
    def submit(
        self, config, job_script, jobs: dict[str, Slurm], jobname: str, dry: bool
    ):
        c = SlurmConfig.model_validate(config.model_dump())
        c.dependency = c.dependency.replace_with_job_id(jobs, dry)
        c.job_name = jobname
        job = Slurm(run_cmd=job_script, config=c)
        job.sbatch(dry=dry)
        return job

    def dependency(self, config) -> Iterable[int]:
        c = SlurmConfig.model_validate(config.model_dump())
        return c.dependency


def get_scheduler() -> Scheduler:
    if jhcfg.scheduler.name == "slurm":
        return SlurmScheduler.model_validate(jhcfg.scheduler.config)
    raise ValueError(f"Unsupported scheduler: {jhcfg.scheduler.name}")
