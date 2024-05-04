from __future__ import annotations

import os
from functools import cached_property
from pathlib import Path
from typing import Annotated, ClassVar, Union

import toml
from loguru import logger as logger
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    DirectoryPath,
    Field,
    field_validator,
    model_validator,
)


def dir_exists(v: Union[str, Path]) -> Path:
    v = Path(v)
    v.mkdir(parents=True, exist_ok=True)
    return v


DirExists = Annotated[DirectoryPath, BeforeValidator(dir_exists)]


class CLIConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    logging_cmd: Annotated[bool, Field(description="log the running command")] = True
    log_file: Annotated[Path, Field(description="log file", validate_default=True)] = (
        Path("cmd.log")
    )
    serialize_log: Annotated[
        bool,
        Field(description="serialize log, set to False to get a human-readable log"),
    ] = True

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
                logger.warning(
                    f"{p} is not a directory. It is removed from repo_watcher"
                )
                continue
            if not (p / ".git").exists():
                logger.warning(
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
    log_dir: Annotated[DirExists, Field(validate_default=True)] = Path()


class ProjectConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    log_dir: DirExists = Field(default=Path(""), validate_default=True)


class JobHelperConfig(BaseModel):
    """
    This is a singleton class for storing global variables.
    """

    model_config = ConfigDict(validate_assignment=True)
    _reserved_commands: ClassVar[list[str]] = ["init", "project"]
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


def init_jhcfg():
    fn = None
    if "JHCFG" in os.environ:
        fn = os.environ["JHCFG"]
    elif Path("jh_config.toml").exists():
        fn = "jh_config.toml"

    if fn is None:
        logger.warning("jh_config.toml or JHCFG is not found. Use default settings.")
        return JobHelperConfig()
    return JobHelperConfig.model_validate(toml.load(fn))


jhcfg = init_jhcfg()
