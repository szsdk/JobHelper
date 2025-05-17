from pathlib import Path
from typing import Annotated, Any, Iterable

from pydantic import BaseModel, ConfigDict, Field

from .config import DirExists


class Scheduler(BaseModel):
    log_dir: Annotated[DirExists, Field(validate_default=True)] = Path()

    def submit(self, config, job_script, jobs, jobname: str, dry: bool) -> Any: ...
    def dependency(self, config) -> Iterable[int]: ...


class JobPreamble(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    dependency: Any = Field(default_factory=list)
