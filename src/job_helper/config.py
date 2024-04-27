from __future__ import annotations

import logging
import os
from functools import cached_property
from pathlib import Path
from typing import Annotated, ClassVar, Optional, Self

import toml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    DirectoryPath,
    Field,
    computed_field,
    field_validator,
)
from rich.console import Console as _Console
from rich.logging import RichHandler as _RichHandler


def dir_exists(v: Path) -> Path:
    v.mkdir(parents=True, exist_ok=True)
    return v


DirExists = Annotated[DirectoryPath, BeforeValidator(dir_exists)]


class CmdLoggerFileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        if hasattr(record, "typename"):
            record.label = f"{record.levelname[0]}-{record.typename}"
            self._style._fmt = "%(asctime)s %(label)-8s>> %(message)s"
        else:
            self._style._fmt = "%(asctime)s %(levelname)-8s>> %(message)s"
        return super().format(record)


class CLIConfig(BaseModel):
    logging_cmd: bool = Field(False, description="log the running command")
    log_file: Path = Field(Path("log/cmd.log"), validate_default=True)

    @field_validator("log_file", mode="before")
    def _validate_log_dir(cls, v: Path) -> Path:
        v.parent.mkdir(parents=True, exist_ok=True)
        return v


class RepoWatcherConfig(BaseModel):
    repos: list[DirectoryPath] = Field(default_factory=list)


class SlurmConfig(BaseModel):
    shell: str = "/bin/sh"
    sbatch_cmd: str = "sbatch"
    sacct_cmd: str = "sacct"
    log_dir: DirExists = Field(Path("log/jobs"), validate_default=True)


class ProjectConfig(BaseModel):
    watch_repos: bool = True
    log_dir: DirExists = Field(Path("log/projects"), validate_default=True)


class JobHelperConfig(BaseModel):
    """
    This is a singleton class for storing global variables.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    _instance: ClassVar[Optional[Self]] = None
    console_width: int = 120
    commands: dict[str, str] = Field(default_factory=dict)
    slurm: SlurmConfig = Field(default_factory=SlurmConfig)
    repo_watcher: RepoWatcherConfig = Field(default_factory=RepoWatcherConfig)
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    cli: CLIConfig = Field(default_factory=CLIConfig)

    def model_post_init(self, _):
        if JobHelperConfig._instance is not None:
            raise RuntimeError("The instance is already initialized.")
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
        cmd_logger_file_handler = logging.FileHandler(self.cli.log_file)
        cmd_logger_file_handler.setFormatter(CmdLoggerFileFormatter())
        cmd_logger.addHandler(cmd_logger_file_handler)
        return cmd_logger

    @staticmethod
    def get_instance() -> JobHelperConfig:
        if JobHelperConfig._instance is None:
            return JobHelperConfig()
        return JobHelperConfig._instance


def _init_jhcfg():
    fn = None
    if "JHCFG" in os.environ:
        fn = os.environ["JHCFG"]
    elif Path("jh_config.toml").exists():
        fn = "jh_config.toml"

    if fn is None:
        return JobHelperConfig()
    return JobHelperConfig.model_validate(toml.load(fn))


jhcfg = _init_jhcfg()
