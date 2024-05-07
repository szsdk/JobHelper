from typing import Iterable

from pydantic import BaseModel


class Scheduler(BaseModel):
    def submit(self, config, job_script, jobs, jobname: str, dry: bool): ...
    def dependency(self, config) -> Iterable[int]: ...
