import pydoc
from pathlib import Path
from typing import Annotated, Any, Iterable

from loguru import logger as logger
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ._utils import LogDir


class Scheduler(BaseModel):
    log_dir: Annotated[LogDir, Field(validate_default=True)] = LogDir(path=Path(""))

    def get_log_dir(self) -> Path:
        return self.log_dir.get_path()

    def submit(
        self, __config__, __job_script__, __jobs__, __jobname__: str, __dry__: bool
    ) -> Any: ...
    def dependency(self, __config__) -> Iterable[int]: ...

    @staticmethod
    def resolve_subclass(name: str) -> type["Scheduler"]:
        from .slurm_helper import SlurmScheduler

        if name == "slurm":
            return SlurmScheduler
        try:
            s = pydoc.locate(name)
            assert (isinstance(s, type)) and (issubclass(s, Scheduler)), (
                f"Expected 's' to be a subclass of Scheduler, got {type(s)}"
            )
            return s
        except ImportError:
            pass
        raise ValueError(f"Unsupported scheduler: {name}")


class JobPreamble(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    dependency: Any = Field(default_factory=list)
