from __future__ import annotations

import logging
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Dict, Optional, Protocol, Self, TypeVar, Union

from pydantic import BaseModel, ConfigDict, computed_field
from rich.console import Console as _Console
from rich.logging import RichHandler as _RichHandler


class CmdLoggerFileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, "typename"):
            record.label = f"{record.levelname[0]}-{record.typename}"
            self._style._fmt = "%(asctime)s %(label)-8s>> %(message)s"
        else:
            self._style._fmt = "%(asctime)s %(levelname)-8s>> %(message)s"
        return super().format(record)


class SlurmConfig(BaseModel):
    shell: str = "/bin/sh"
    sbatch_cmd: str = "sbatch"


class JobHelperConfig(BaseModel):
    """
    This is a singleton class for storing global variables.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
    _instance: ClassVar[Optional[Self]] = None
    log_dir: Path = Path("log")
    job_log_dir: Path = Path("log/jobs")
    console_width: int = 120
    slurm: SlurmConfig = SlurmConfig()

    def __post_init__(self):
        if JobHelperConfig._instance is not None:
            raise RuntimeError("The instance is already initialized.")
        if not self.log_dir.exists():
            self.log_dir.mkdir()
        if not self.job_log_dir.exists():
            self.job_log_dir.mkdir()
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(message)s",
            handlers=[_RichHandler(console=self.rich_console)],
        )
        logging.getLogger("matplotlib").setLevel(logging.WARNING)
        logging.getLogger("h5py").setLevel(logging.WARNING)
        JobHelperConfig._instance = self

    @computed_field
    @cached_property
    def rich_console(self) -> _Console:
        return _Console(width=self.console_width)

    @computed_field
    @cached_property
    def cmd_logger(self) -> logging.Logger:
        cmd_logger = logging.getLogger(f"cmd_{__file__}")
        cmd_logger.setLevel(logging.DEBUG)
        cmd_logger_file_handler = logging.FileHandler(self.log_dir / "cmd.log")
        cmd_logger_file_handler.setFormatter(CmdLoggerFileFormatter())
        cmd_logger.addHandler(cmd_logger_file_handler)
        return cmd_logger

    @staticmethod
    def get_instance() -> JobHelperConfig:
        if JobHelperConfig._instance is None:
            return JobHelperConfig()
        return JobHelperConfig._instance


jhcfg = JobHelperConfig()
