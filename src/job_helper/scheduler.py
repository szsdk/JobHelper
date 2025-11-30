from pathlib import Path
from typing import Annotated, Any, Iterable

from loguru import logger as logger
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .config import LogDir


class Scheduler(BaseModel):
    # log_dir: Annotated[DirExists, Field(validate_default=True)] = Path()
    log_dir: Annotated[LogDir, Field(validate_default=True)] = LogDir(path=Path(""))

    def get_log_dir(self) -> Path:
        return self.log_dir.path

    def submit(self, config, job_script, jobs, jobname: str, dry: bool) -> Any: ...
    def dependency(self, config) -> Iterable[int]: ...


class JobPreamble(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    dependency: Any = Field(default_factory=list)
