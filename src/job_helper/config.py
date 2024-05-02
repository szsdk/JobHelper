from __future__ import annotations

import logging
import os
from functools import cached_property
from pathlib import Path
from typing import Annotated, ClassVar, Union

import toml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    DirectoryPath,
    Field,
    field_validator,
    model_validator,
)
from rich.logging import RichHandler


class CmdLoggerFileFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        typename = getattr(record, "typename", None)
        if typename is None:
            self._style._fmt = "%(asctime)s %(levelname)-8s>> %(message)s"
        else:
            record.label = f"{record.levelname[0]}-{typename}"
            self._style._fmt = "%(asctime)s %(label)-8s>> %(message)s"
        return super().format(record)


def dir_exists(v: Union[str, Path]) -> Path:
    v = Path(v)
    v.mkdir(parents=True, exist_ok=True)
    return v


DirExists = Annotated[DirectoryPath, BeforeValidator(dir_exists)]


class CLIConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    logging_cmd: Annotated[bool, Field(description="log the running command")] = True
    log_file: Annotated[Path, Field(description="log file", validate_default=True)] = (
        Path("log/cmd.log")
    )

    @field_validator("log_file", mode="before")
    def _validate_log_dir(cls, v: Union[str, Path]) -> Path:
        v = Path(v)
        v.parent.mkdir(parents=True, exist_ok=True)
        return v


class RepoWatcherConfig(BaseModel):
    watched_repos: list[DirectoryPath] = Field(default_factory=list)
    force_commit_repos: list[DirectoryPath] = Field(default_factory=list)

    @field_validator("watched_repos", "force_commit_repos", mode="after")
    def _validate_repos(cls, v: list[DirectoryPath]) -> list[DirectoryPath]:
        ans = []
        for p in v:
            if not p.is_dir():
                cmd_logger.warning(
                    f"{p} is not a directory. It is removed from repo_watcher"
                )
                continue
            if not (p / ".git").exists():
                cmd_logger.warning(
                    f"{p} is not a git repo. It is removed from repo_watcher"
                )
                continue
            ans.append(p)
        return ans

    @model_validator(mode="after")
    def no_overlap_repos_and_force_commit(self):
        if set(self.watched_repos) & set(self.force_commit_repos):
            raise ValueError("watched_repos and force_commit_repos should not overlap.")
        return self


class SlurmConfig(BaseModel):
    shell: str = "/bin/sh"
    sbatch_cmd: Annotated[str, Field(description="sbatch command")] = "sbatch"
    sacct_cmd: str = "sacct"
    log_dir: Annotated[DirExists, Field(validate_default=True)] = Path("log/jobs")


class ProjectConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    log_dir: DirExists = Field(default=Path("log/projects"), validate_default=True)


class JobHelperConfig(BaseModel):
    """
    This is a singleton class for storing global variables.
    """

    model_config = ConfigDict(validate_assignment=True)
    _reserved_commands: ClassVar[list[str]] = ["init", "project"]
    console_width: Annotated[int, Field(description="console width")] = 120
    commands: dict[str, str] = Field(default_factory=dict)
    slurm: SlurmConfig = Field(
        default_factory=SlurmConfig, description="Slurm configuration"
    )
    repo_watcher: RepoWatcherConfig = Field(default_factory=RepoWatcherConfig)
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    cli: CLIConfig = Field(default_factory=CLIConfig)

    @field_validator("commands", mode="after")
    def not_contains_reserved_commands(cls, v: dict[str, str]) -> dict[str, str]:
        for k in cls._reserved_commands:
            if k in v:
                raise ValueError(f"{k} is a reserved command.")
        return v


class _InitJobHelperConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    cli: CLIConfig = Field(default_factory=CLIConfig)


def _init_jhcfg(cls):
    fn = None
    if "JHCFG" in os.environ:
        fn = os.environ["JHCFG"]
    elif Path("jh_config.toml").exists():
        fn = "jh_config.toml"

    if fn is None:
        return cls()
    return cls.model_validate(toml.load(fn))


def _init_logger():
    cmd_logger.setLevel(logging.DEBUG)
    cfg = _init_jhcfg(_InitJobHelperConfig)
    cmd_logger_file_handler = logging.FileHandler(cfg.cli.log_file)
    cmd_logger_file_handler.setFormatter(CmdLoggerFileFormatter())
    cmd_logger.addHandler(cmd_logger_file_handler)
    cmd_logger.addHandler(RichHandler())


cmd_logger = logging.getLogger("_jb_cmd")
_init_logger()

jhcfg = _init_jhcfg(JobHelperConfig)
