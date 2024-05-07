from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, Field


class Scheduler(BaseModel):
    def submit(self, config, job_script, jobs, jobname: str, dry: bool): ...
    def dependency(self, config) -> Iterable[int]: ...


class JobPreamble(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    dependency: Any = Field(default_factory=list)
