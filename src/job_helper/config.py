from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Annotated, Any, ClassVar, Literal, Union

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
from pydantic.networks import IPvAnyAddress


def dir_exists(v: Union[str, Path]) -> Path:
    v = Path(v)
    v.mkdir(parents=True, exist_ok=True)
    return v


DirExists = Annotated[DirectoryPath, BeforeValidator(dir_exists)]


def get_available_port(start_port, end_port) -> int:
    hostname = socket.gethostname()
    for port in range(start_port, end_port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex((hostname, port)) != 0:
                return port
    raise ValueError(f"No available port in range {start_port} to {end_port}")


class ServerConfig(BaseModel):
    ip: Union[IPvAnyAddress, Literal["localhost"]] = "localhost"
    port: int = Field(default_factory=lambda: get_available_port(8000, 9000))


class CLIConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    logging_cmd: Annotated[bool, Field(description="log the running command")] = True
    log_file: Annotated[Path, Field(description="log file", validate_default=True)] = (
        Path("cmd.log")
    )
    serialize_log: Annotated[
        bool,
        Field(description="serialize log, set to false to get a human-readable log"),
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


class ProjectConfig(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, validate_assignment=True)
    log_dir: DirExists = Field(
        default=Path(""),
        validate_default=True,
        description="Log directory. Defaults to the current directory. Providing an absolute path centralizes all log files into a single folder.",
    )


class SchedulerConfig(BaseModel):
    name: Annotated[
        str,
        Field(
            description="The type of scheduler. Or the python import path to a custom scheduler class"
        ),
    ] = "slurm"
    config: dict[str, Any] = Field(
        default_factory=lambda: dict(
            dict(
                shell="/bin/sh", sbatch_cmd="sbatch", sacct_cmd="sacct", log_dir=Path()
            )
        ),
        description="The configuration for the scheduler. It varies for different schedulers.",
    )


class JobHelperConfig(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    _reserved_commands: ClassVar[list[str]] = ["init", "project"]
    commands: dict[str, str] = Field(
        default_factory=dict,
        description="You can add custom commands here, for example: `add=cli.AddOne`. The key is the command name, and the value is the class import path. These commands can then be referenced in a job configuration file, providing a simple and convenient task runner. For more complex scenarios, this approach is not recommended. Instead, specify the class path directly in the job configuration file and consider using a more advanced task runner, such as `justfile` or `poethepoet`.",
    )
    scheduler: SchedulerConfig = Field(
        default_factory=lambda: SchedulerConfig(), description="scheduler config"
    )
    repo_watcher: RepoWatcherConfig = Field(
        default_factory=RepoWatcherConfig, description="repo watcher config"
    )
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    cli: CLIConfig = Field(default_factory=CLIConfig, description="cli config")
    server: ServerConfig = Field(
        default_factory=ServerConfig, description="config for web server"
    )

    @field_validator("commands", mode="after")
    def not_contains_reserved_commands(cls, v: dict[str, str]) -> dict[str, str]:
        for k in cls._reserved_commands:
            if k in v:
                raise ValueError(f"{k} is a reserved command.")
        return v


def init_jhcfg():
    if "JHCFG" in os.environ:
        return JobHelperConfig.model_validate(toml.load(os.environ["JHCFG"]))
    for c in [Path().resolve(), *Path().resolve().parents]:
        p = c / "pyproject.toml"
        if p.exists():
            content = toml.load(p).get("tool", {}).get("job_helper", None)
            if content is not None:
                return JobHelperConfig.model_validate(content)
        p = c / "jh_config.toml"
        if p.exists():
            return JobHelperConfig.model_validate(toml.load(p))
    logger.warning(
        "jh_config.toml or JHCFG environment variable or [job_helper] in a pyproject.toml is not found. Use default settings."
    )
    return JobHelperConfig()


jhcfg = init_jhcfg()
