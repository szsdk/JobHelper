from pydantic import BaseModel

from .config import jhcfg


class Scheduler(BaseModel):
    def submit(self, config, job_script): ...


class SlurmScheduler(Scheduler):
    def submit(self, config, job_script): ...


def get_scheduler() -> Scheduler:
    if jhcfg.scheduler.name == "slurm":
        return SlurmScheduler.model_validate(jhcfg.scheduler.config)
    raise ValueError(f"Unsupported scheduler: {jhcfg.scheduler.name}")
